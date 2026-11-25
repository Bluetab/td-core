defmodule TdCore.XLSX.UploadRecoveryTest do
  use ExUnit.Case

  import ExUnit.CaptureLog
  import Mox

  alias TdCluster.TestHelpers.TdAuditMock.UploadJobs
  alias TdCore.XLSX.UploadRecovery

  # The events below are built by hand with the shape documented in `Oban.Telemetry`
  # (Oban 2.19). td-core does not depend on Oban, so each service has a contract test
  # that runs a real job and checks that Oban still emits this shape.

  @job_event [:oban, :job, :exception]
  @plugin_event [:oban, :plugin, :stop]
  @queue :xlsx_test_upload_queue

  setup :verify_on_exit!

  setup context do
    test_pid = self()
    id = {:recovery_test, System.unique_integer([:positive])}

    on_exit(fn -> UploadRecovery.detach(id: id) end)

    writer = fn job, message ->
      send(test_pid, {:written, job.id, message})
      :ok
    end

    fetch_job = fn id ->
      send(test_pid, {:fetched, id})
      Map.get(context, :fetched_job)
    end

    opts =
      [id: id, queues: [@queue], fetch_job: fetch_job, writer: writer]
      |> Keyword.merge(Map.get(context, :attach_opts, []))

    unless context[:skip_attach], do: UploadRecovery.attach(opts)

    [opts: opts, id: id]
  end

  defp job(attrs \\ %{}) do
    Map.merge(
      %{
        id: System.unique_integer([:positive]),
        queue: Atom.to_string(@queue),
        attempt: 1,
        max_attempts: 1,
        args: %{"job_id" => 42}
      },
      Map.new(attrs)
    )
  end

  defp exception(job, extra \\ %{}) do
    meta = Map.merge(%{job: job, state: :discard, reason: %RuntimeError{message: "boom"}}, extra)
    :telemetry.execute(@job_event, %{duration: 1}, meta)
  end

  defp lifeline(discarded_jobs, extra \\ %{}) do
    meta = Map.merge(%{plugin: Oban.Plugins.Lifeline, discarded_jobs: discarded_jobs}, extra)
    :telemetry.execute(@plugin_event, %{duration: 1}, meta)
  end

  describe "final job failure" do
    test "writes one failure for a job that failed on its last attempt" do
      %{id: id} = job = job()

      exception(job)

      assert_receive {:written, ^id, "Upload job failed: error"}
      refute_receive {:written, _, _}
    end

    test "describes a timeout and a crash by the name of the error struct" do
      %{id: timeout_id} = timeout = job()
      %{id: crash_id} = crash = job()

      exception(timeout, %{reason: %{__struct__: Oban.TimeoutError}})
      exception(crash, %{reason: %{__struct__: Oban.CrashError}})

      assert_receive {:written, ^timeout_id, "Upload job failed: timeout"}
      assert_receive {:written, ^crash_id, "Upload job failed: crash"}
    end

    test "matches a job whose queue is an atom against the configured queues" do
      %{id: id} = job = job(%{queue: @queue})

      exception(job)

      assert_receive {:written, ^id, _}
    end

    @tag attach_opts: [queues: ["xlsx_test_upload_queue"]]
    test "accepts the queues as strings" do
      %{id: id} = job = job()

      exception(job)

      assert_receive {:written, ^id, _}
    end

    test "does nothing when the job still has attempts left" do
      exception(job(%{attempt: 1, max_attempts: 5}))

      refute_receive {:written, _, _}
    end

    test "does nothing for a job in a queue that is not watched" do
      exception(job(%{queue: "default"}))

      refute_receive {:written, _, _}
    end

    test "does nothing when the attempt counters are missing" do
      exception(job() |> Map.drop([:attempt, :max_attempts]))

      refute_receive {:written, _, _}
    end

    test "does nothing for an exception that is not a discard" do
      exception(job(), %{state: :failure})

      refute_receive {:written, _, _}
    end

    test "does not handle a job cancelled by hand (a :stop event)" do
      :telemetry.execute([:oban, :job, :stop], %{duration: 1}, %{job: job(), state: :discard})

      refute_receive {:written, _, _}
    end
  end

  describe "jobs discarded by Lifeline" do
    @describetag fetched_job: nil

    @tag fetched_job: %{id: 7, queue: "xlsx_test_upload_queue", args: %{"job_id" => 7}}
    test "reads the job and writes one failure" do
      lifeline([%{id: 7, queue: "xlsx_test_upload_queue", state: "discarded"}])

      assert_receive {:fetched, 7}
      assert_receive {:written, 7, "Upload job interrupted"}
      refute_receive {:written, _, _}
    end

    test "does not read or write jobs of other queues" do
      lifeline([%{id: 8, queue: "default", state: "discarded"}])

      refute_receive {:fetched, _}
      refute_receive {:written, _, _}
    end

    test "writes nothing when the job was pruned and cannot be read" do
      lifeline([%{id: 9, queue: "xlsx_test_upload_queue", state: "discarded"}])

      assert_receive {:fetched, 9}
      refute_receive {:written, _, _}
    end

    test "ignores events from other plugins" do
      :telemetry.execute(@plugin_event, %{duration: 1}, %{
        plugin: Oban.Plugins.Pruner,
        discarded_jobs: [%{id: 10, queue: "xlsx_test_upload_queue", state: "discarded"}]
      })

      refute_receive {:fetched, _}
    end

    test "handles an event without discarded jobs" do
      :telemetry.execute(@plugin_event, %{duration: 1}, %{plugin: Oban.Plugins.Lifeline})

      refute_receive {:fetched, _}
    end
  end

  describe "failures inside the handlers" do
    @tag attach_opts: [writer: &__MODULE__.raising_writer/2]
    test "a writer that raises is logged and the handler stays attached", %{id: id} do
      log = capture_log(fn -> exception(job()) end)

      assert log =~ "could not write failure"
      assert handler_attached?(id, @job_event)

      # :telemetry detaches a handler that raises, so this proves it did not
      log = capture_log(fn -> exception(job()) end)
      assert log =~ "could not write failure"
    end

    @tag attach_opts: [fetch_job: &__MODULE__.raising_fetch/1]
    test "a fetch_job that raises is logged and the handler stays attached", %{id: id} do
      log =
        capture_log(fn ->
          lifeline([%{id: 11, queue: "xlsx_test_upload_queue", state: "discarded"}])
        end)

      assert log =~ "could not fetch job 11"
      assert handler_attached?(id, @plugin_event)
      refute_receive {:written, _, _}
    end
  end

  describe "attach/1" do
    test "attaching twice with the same id writes only once", %{opts: opts} do
      assert :ok = UploadRecovery.attach(opts)

      %{id: id} = job = job()
      exception(job)

      assert_receive {:written, ^id, _}
      refute_receive {:written, ^id, _}
    end

    @tag skip_attach: true
    test "requires the queues and fetch_job options" do
      assert_raise KeyError, fn -> UploadRecovery.attach(fetch_job: & &1) end
      assert_raise KeyError, fn -> UploadRecovery.attach(queues: [:a]) end
    end

    @tag skip_attach: true
    test "detach/1 stops the handling", %{opts: opts, id: id} do
      UploadRecovery.attach(opts)
      UploadRecovery.detach(id: id)

      exception(job())

      refute_receive {:written, _, _}
    end
  end

  describe "default writer" do
    @describetag skip_attach: true

    setup %{opts: opts} do
      # attach without the stub writer, so the default one is used
      UploadRecovery.attach(Keyword.delete(opts, :writer))
      :ok
    end

    test "marks the upload job as failed in td-audit using job_id from the args" do
      UploadJobs.create_failed(&Mox.expect/4, 42, "Upload job failed: error")

      # the handler runs in the process that emits the event, so verify_on_exit!
      # fails the test if td-audit is not called with exactly these arguments
      exception(job())
    end

    test "logs a warning when the job has no job_id in its args" do
      log = capture_log(fn -> exception(job(%{args: %{}})) end)

      assert log =~ "has no job_id in its args"
    end
  end

  def raising_writer(_job, _message), do: raise("writer down")
  def raising_fetch(_id), do: raise("repo down")

  defp handler_attached?(id, event) do
    Enum.any?(
      :telemetry.list_handlers(event),
      &(&1.id == {UploadRecovery, id, handler_kind(event)})
    )
  end

  defp handler_kind(@job_event), do: :job
  defp handler_kind(@plugin_event), do: :plugin
end
