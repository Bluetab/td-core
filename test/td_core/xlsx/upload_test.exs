defmodule TdCore.XLSX.UploadTest do
  @moduledoc """
  Tests for the shared XLSX upload flow. td-audit and the enqueue function are
  injected, so no external service is needed.
  """
  use ExUnit.Case

  alias TdCore.Utils.FileHash
  alias TdCore.XLSX.Reader
  alias TdCore.XLSX.Upload

  defmodule FakeAudit do
    @moduledoc false
    # Records every call in the test process mailbox and answers with the
    # responses configured in the process dictionary.
    def create_job(attrs) do
      send(self(), {:audit, :create_job, attrs})
      Process.get(:create_job_response, {:ok, %{id: 42}})
    end

    def create_pending(job_id) do
      send(self(), {:audit, :create_pending, job_id})
      Process.get(:create_pending_response, {:ok, nil})
    end

    def create_failed(job_id, message) do
      send(self(), {:audit, :create_failed, job_id, message})
      {:ok, nil}
    end
  end

  setup do
    tmp = Path.join(System.tmp_dir!(), "td_core_upload_#{System.unique_integer([:positive])}")
    upload_dir = Path.join(tmp, "uploads")
    source_dir = Path.join(tmp, "tmp")
    File.mkdir_p!(source_dir)
    source = Path.join(source_dir, "synthetic.xlsx")
    File.write!(source, "synthetic content")

    on_exit(fn -> File.rm_rf(tmp) end)

    upload = %{
      path: source,
      filename: "synthetic.xlsx",
      user_id: 7,
      scope: "synthetic_scope",
      opts: %{"lang" => "en"}
    }

    %{upload: upload, source: source, upload_dir: upload_dir}
  end

  defp base_opts(ctx, extra) do
    Keyword.merge(
      [
        audit: FakeAudit,
        upload_dir: ctx.upload_dir,
        authorize: fn -> :ok end,
        enqueue: fn args ->
          send(self(), {:enqueue, args})
          {:ok, %{id: 1}}
        end,
        hash_fn: fn _path -> "HASH" end
      ],
      extra
    )
  end

  defp collect_events do
    receive do
      {:order, event} -> [event | collect_events()]
      {:audit, :create_job, _} -> [{:audit, :create_job} | collect_events()]
      {:audit, :create_pending, _} -> [{:audit, :create_pending} | collect_events()]
    after
      0 -> []
    end
  end

  describe "accept/2 success" do
    test "creates job, sets pending, moves the file and enqueues in order", ctx do
      assert {:ok, 42} = Upload.accept(ctx.upload, base_opts(ctx, []))

      assert_received {:audit, :create_job,
                       %{
                         user_id: 7,
                         hash: "HASH",
                         filename: "synthetic.xlsx",
                         scope: "synthetic_scope"
                       }}

      assert_received {:audit, :create_pending, 42}
      refute_received {:audit, :create_failed, _, _}

      target = Path.join(ctx.upload_dir, "synthetic.xlsx")
      assert File.exists?(target)
      refute File.exists?(ctx.source)

      assert_received {:enqueue,
                       %{
                         path: ^target,
                         job_id: 42,
                         scope: "synthetic_scope",
                         opts: %{"lang" => "en"}
                       }}
    end

    test "runs authorize, hash, create_job, create_pending, move and enqueue in that order",
         ctx do
      parent = self()
      log = fn event -> send(parent, {:order, event}) end

      opts =
        base_opts(ctx,
          authorize: fn ->
            log.(:authorize)
            :ok
          end,
          hash_fn: fn _ ->
            log.(:hash)
            "HASH"
          end,
          move_fn: fn path, dir ->
            log.(:move)
            Reader.move_file!(path, dir)
          end,
          enqueue: fn _ ->
            log.(:enqueue)
            {:ok, %{id: 1}}
          end
        )

      assert {:ok, 42} = Upload.accept(ctx.upload, opts)

      # create_job and create_pending are recorded by FakeAudit with another tag
      events = collect_events()

      assert events == [
               :authorize,
               :hash,
               {:audit, :create_job},
               {:audit, :create_pending},
               :move,
               :enqueue
             ]
    end

    test "accept_with_details/2 returns the job id, the hash and the enqueue result", ctx do
      assert {:ok, %{job_id: 42, hash: "HASH", enqueued: %{id: 1}}} =
               Upload.accept_with_details(ctx.upload, base_opts(ctx, []))
    end

    test "accept_with_details/2 does not hash the file when the authorization fails", ctx do
      opts =
        base_opts(ctx,
          authorize: fn -> false end,
          hash_fn: fn _ -> flunk("the file must not be read before authorizing") end
        )

      assert {:error, :forbidden} = Upload.accept_with_details(ctx.upload, opts)
    end

    test "uses md5 hash of the file by default", ctx do
      opts = ctx |> base_opts([]) |> Keyword.delete(:hash_fn)
      expected = FileHash.hash(ctx.source, :md5)

      assert {:ok, 42} = Upload.accept(ctx.upload, opts)
      assert_received {:audit, :create_job, %{hash: ^expected}}
    end
  end

  describe "accept/2 authorization" do
    test "returns the error without creating a job or moving the file", ctx do
      opts = base_opts(ctx, authorize: fn -> {:error, :unauthorized} end)

      assert {:error, :unauthorized} = Upload.accept(ctx.upload, opts)

      refute_received {:audit, :create_job, _}
      refute_received {:enqueue, _}
      assert File.exists?(ctx.source)
      refute File.exists?(Path.join(ctx.upload_dir, "synthetic.xlsx"))
    end

    test "false maps to :forbidden without side effects", ctx do
      opts = base_opts(ctx, authorize: fn -> false end)

      assert {:error, :forbidden} = Upload.accept(ctx.upload, opts)
      refute_received {:audit, :create_job, _}
      assert File.exists?(ctx.source)
    end
  end

  describe "accept/2 enqueue failure" do
    test "marks the job failed, removes the moved file and returns the error", ctx do
      opts =
        base_opts(ctx,
          enqueue: fn _args -> {:error, :queue_down} end
        )

      assert {:error, :queue_down} = Upload.accept(ctx.upload, opts)

      assert_received {:audit, :create_failed, 42, "job enqueue failed"}
      refute File.exists?(Path.join(ctx.upload_dir, "synthetic.xlsx"))
    end
  end

  describe "accept/2 audit failures" do
    test "create_job error is returned without moving the file", ctx do
      Process.put(:create_job_response, {:error, :audit_down})

      assert {:error, :audit_down} = Upload.accept(ctx.upload, base_opts(ctx, []))

      refute_received {:audit, :create_pending, _}
      refute_received {:enqueue, _}
      assert File.exists?(ctx.source)
    end

    test "create_pending error marks the job failed and does not enqueue", ctx do
      Process.put(:create_pending_response, {:error, "pending_failed"})

      assert {:error, "pending_failed"} = Upload.accept(ctx.upload, base_opts(ctx, []))

      assert_received {:audit, :create_failed, 42, "pending_failed"}
      refute_received {:enqueue, _}
      assert File.exists?(ctx.source)
    end

    test "create_pending non-binary reason is normalized to a safe message", ctx do
      reason = {:changeset_error, %{field: ["is invalid"]}}
      Process.put(:create_pending_response, {:error, reason})

      assert {:error, ^reason} = Upload.accept(ctx.upload, base_opts(ctx, []))

      expected = inspect(reason)
      assert_received {:audit, :create_failed, 42, ^expected}
      refute_received {:enqueue, _}
    end

    test "create_pending atom reason is passed as is", ctx do
      Process.put(:create_pending_response, {:error, :pending_down})

      assert {:error, :pending_down} = Upload.accept(ctx.upload, base_opts(ctx, []))
      assert_received {:audit, :create_failed, 42, :pending_down}
    end
  end

  describe "accept/2 move failure" do
    test "an exception marks the job failed, cleans the destination and does not enqueue",
         ctx do
      target = Path.join(ctx.upload_dir, "synthetic.xlsx")

      opts =
        base_opts(ctx,
          move_fn: fn _path, dir ->
            File.mkdir_p!(dir)
            File.write!(target, "partial")
            raise "disk full"
          end
        )

      assert {:error, %RuntimeError{message: "disk full"}} = Upload.accept(ctx.upload, opts)

      assert_received {:audit, :create_failed, 42, "file move failed"}
      refute_received {:enqueue, _}
      refute File.exists?(target)
    end

    test "an exit is caught and reported the same way", ctx do
      opts = base_opts(ctx, move_fn: fn _path, _dir -> exit(:shutdown) end)

      assert {:error, {:exit, :shutdown}} = Upload.accept(ctx.upload, opts)

      assert_received {:audit, :create_failed, 42, "file move failed"}
      refute_received {:enqueue, _}
    end
  end
end
