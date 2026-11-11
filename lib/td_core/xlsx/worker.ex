defmodule TdCore.XLSX.Worker do
  @moduledoc """
  Common Oban worker policy for XLSX bulk uploads.

  `use TdCore.XLSX.Worker, queue: :my_queue` expands to `use Oban.Worker` and
  defines:

    * `max_attempts: 1` — a retry would duplicate the per-row events.
    * `queue` — the queue given in the options (required).
    * `timeout/1` — 10 minutes.
    * `perform/1` — runs `TdCore.XLSX.UploadWorker.run/1` with the job args and
      always removes the uploaded file afterwards.

  td-core does not depend on Oban: the expansion happens in the consumer
  application, which must have `:oban` as a dependency.

  ## Example

      defmodule MyApp.XLSX.UploadWorker do
        use TdCore.XLSX.Worker, queue: :xlsx_my_entity_upload_queue
      end

  Any other option is passed through to `use Oban.Worker`, except `:max_attempts`,
  which is fixed by this policy.
  """

  alias TdCore.XLSX.UploadWorker

  @timeout :timer.minutes(10)

  @doc "Timeout, in milliseconds, applied to every XLSX upload job."
  @spec timeout_ms() :: pos_integer()
  def timeout_ms, do: @timeout

  @doc """
  Runs an upload with the job args and removes the uploaded file afterwards, also
  when the upload raises.

  The args must contain the `"path"` of the uploaded file and the keys expected by
  `TdCore.XLSX.UploadWorker.run/1`. `runner` defaults to
  `&TdCore.XLSX.UploadWorker.run/1` and can be replaced in tests.
  """
  @spec perform(map(), (map() -> term())) :: term()
  def perform(%{"path" => path} = args, runner \\ &UploadWorker.run/1) do
    watcher = cleanup_on_death(path)

    try do
      runner.(args)
    after
      File.rm(path)
      Process.exit(watcher, :kill)
    end
  end

  # Oban enforces the timeout with `:timer.exit_after/2`, which sends an exit
  # signal that kills the job process without running its `after` clauses. We do
  # not trap exits (that would let the job keep running past its timeout), so a
  # separate process monitors the job and removes the file when it dies.
  defp cleanup_on_death(path) do
    job = self()

    spawn(fn ->
      ref = Process.monitor(job)

      receive do
        {:DOWN, ^ref, :process, _, _} -> File.rm(path)
      end
    end)
  end

  defmacro __using__(opts) do
    queue = Keyword.fetch!(opts, :queue)
    oban_opts = opts |> Keyword.delete(:queue) |> Keyword.delete(:max_attempts)

    quote do
      use Oban.Worker,
          unquote([queue: queue, max_attempts: 1] ++ oban_opts)

      @impl Oban.Worker
      def perform(%Oban.Job{args: args}), do: TdCore.XLSX.Worker.perform(args)

      @impl Oban.Worker
      def timeout(_job), do: TdCore.XLSX.Worker.timeout_ms()
    end
  end
end
