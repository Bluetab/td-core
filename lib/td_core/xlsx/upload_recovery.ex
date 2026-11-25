defmodule TdCore.XLSX.UploadRecovery do
  @moduledoc """
  Marks as failed the XLSX uploads that an Oban job left orphaned.

  An upload job can end without running its own error handling: the node restarts
  while it executes (and `Oban.Plugins.Lifeline` later discards it), or Oban kills it
  on a timeout. Nothing then records the failure, so the upload stays `STARTED` forever.

  This module attaches to the Oban telemetry events and calls a writer for each upload
  job that ended that way. It does not depend on Oban: it only uses `:telemetry` and
  reads the event metadata as plain maps. The contract it relies on is documented in
  `Oban.Telemetry`:

    * `[:oban, :job, :exception]` with `state: :discard`, emitted when a job fails on
      its last attempt (`attempt >= max_attempts`). `meta.job` has the job.
    * `[:oban, :plugin, :stop]` from `Oban.Plugins.Lifeline`, whose `discarded_jobs` list
      has only `id`, `queue` and `state`. The job is read with the `:fetch_job` option.

  Only jobs in the configured queues are handled, and only their final failure: a job
  that still has attempts left is retried by Oban and is not reported.

  ## Usage

  Call `attach/1` once, from `Application.start/2` of the service:

      TdCore.XLSX.UploadRecovery.attach(
        queues: [:xlsx_notes_upload_queue, :xlsx_implementations_upload_queue],
        fetch_job: &MyApp.Repo.get(Oban.Job, &1)
      )

  ## Options

    * `:queues` (required) - queues whose jobs are handled. Atoms or strings.
    * `:fetch_job` (required) - `(id -> job | nil)`. Reads a job discarded by Lifeline, which
      does not include its args. It runs in the service's own Oban and Repo. If it returns
      `nil` (the job was pruned), nothing is written.
    * `:writer` - `(job, message -> term)`. Records the failure. The default marks the upload
      job as failed in td-audit, using `job.args["job_id"]`. A service that tracks uploads
      elsewhere passes its own writer.
    * `:id` - identifies the handlers, so a service can attach more than once.
      Default: `:default`.
  """

  require Logger

  alias TdCluster.Cluster.TdAudit.UploadJobs

  @job_event [:oban, :job, :exception]
  @plugin_event [:oban, :plugin, :stop]
  @lifeline Oban.Plugins.Lifeline
  @timeout_error Oban.TimeoutError
  @crash_error Oban.CrashError

  @doc """
  Attaches the telemetry handlers. Calling it again with the same `:id` does nothing.
  """
  @spec attach(keyword()) :: :ok
  def attach(opts) do
    config = build_config(opts)
    id = Keyword.get(opts, :id, :default)

    :ok = attach_handler({__MODULE__, id, :job}, @job_event, &__MODULE__.handle_job/4, config)

    :ok =
      attach_handler(
        {__MODULE__, id, :plugin},
        @plugin_event,
        &__MODULE__.handle_plugin/4,
        config
      )
  end

  @doc """
  Detaches the handlers attached with the given `:id` (default `:default`).
  """
  @spec detach(keyword()) :: :ok
  def detach(opts \\ []) do
    id = Keyword.get(opts, :id, :default)

    :telemetry.detach({__MODULE__, id, :job})
    :telemetry.detach({__MODULE__, id, :plugin})

    :ok
  end

  @doc false
  def handle_job(@job_event, _measurements, %{state: :discard, job: job} = meta, config) do
    if watched?(job, config) and final_attempt?(job) do
      write(job, "Upload job failed: #{describe(Map.get(meta, :reason))}", config)
    end

    :ok
  end

  def handle_job(_event, _measurements, _meta, _config), do: :ok

  @doc false
  def handle_plugin(@plugin_event, _measurements, %{plugin: @lifeline} = meta, config) do
    meta
    |> Map.get(:discarded_jobs, [])
    |> Enum.filter(&watched?(&1, config))
    |> Enum.each(&recover_discarded(&1, config))

    :ok
  end

  def handle_plugin(_event, _measurements, _meta, _config), do: :ok

  defp recover_discarded(%{id: id}, %{fetch_job: fetch_job} = config) do
    case safely(fn -> fetch_job.(id) end, "fetch job #{id}") do
      nil -> :ok
      {:error, _} -> :ok
      job -> write(job, "Upload job interrupted", config)
    end
  end

  defp write(job, message, %{writer: writer}) do
    safely(fn -> writer.(job, message) end, "write failure of job #{Map.get(job, :id)}")
  end

  # A handler that raises is detached by :telemetry, which would disable recovery
  # silently. Log and keep the handler attached.
  defp safely(fun, what) do
    fun.()
  rescue
    e ->
      Logger.error("Upload recovery could not #{what}: #{Exception.message(e)}")
      {:error, e}
  catch
    kind, reason ->
      Logger.error("Upload recovery could not #{what}: #{inspect({kind, reason})}")
      {:error, reason}
  end

  defp watched?(%{queue: queue}, %{queues: queues}), do: to_string(queue) in queues
  defp watched?(_job, _config), do: false

  defp final_attempt?(%{attempt: attempt, max_attempts: max})
       when is_integer(attempt) and is_integer(max),
       do: attempt >= max

  defp final_attempt?(_job), do: false

  defp describe(%{__struct__: @timeout_error}), do: "timeout"
  defp describe(%{__struct__: @crash_error}), do: "crash"
  defp describe(_reason), do: "error"

  defp attach_handler(handler_id, event, fun, config) do
    case :telemetry.attach(handler_id, event, fun, config) do
      :ok -> :ok
      {:error, :already_exists} -> :ok
    end
  end

  defp build_config(opts) do
    %{
      queues: opts |> Keyword.fetch!(:queues) |> Enum.map(&to_string/1),
      fetch_job: Keyword.fetch!(opts, :fetch_job),
      writer: Keyword.get(opts, :writer, &default_writer/2)
    }
  end

  defp default_writer(%{args: %{"job_id" => job_id}}, message) do
    UploadJobs.create_failed(job_id, message)
  end

  defp default_writer(job, _message) do
    Logger.warning("Upload recovery: job #{Map.get(job, :id)} has no job_id in its args")
  end
end
