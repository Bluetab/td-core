defmodule TdCore.Telemetry.EctoMetrics do
  @moduledoc """
  Database metrics for a Postgres-backed service, from its repo's Ecto query
  event:

    * `db.client.operation.duration`: `query_time`, in milliseconds. It
      excludes queue and decode time.
    * `db.client.connection.wait_time`: `queue_time`, in milliseconds.
    * `truedat.db.client.operations`: queries by `truedat.outcome`, `success`
      or `failure`.

  `wiring/1` gives `TdCore.Telemetry.Exporter` the metrics and the step that
  attaches the handler, so the handler exists only while telemetry is on:

      {TdCore.Telemetry.Exporter, TdCore.Telemetry.EctoMetrics.wiring(MyApp.Repo)}

  ## Why a handler sits between Ecto and the metrics

  Ecto's measurements are optional: a query inside a transaction has no
  `queue_time`. `Telemetry.Metrics` returns `nil` for a missing measurement
  and leaves skipping it to the reporter, but `otel_metric_exporter` 0.4.4
  crashes on `nil`, after which `:telemetry` detaches its handler for good. The
  exporter also calls `:keep` with the metadata only, so `:keep` cannot test
  for the measurement either. The handler therefore re-emits one event per
  measurement that is present. Once the exporter skips `nil`, the metrics can
  listen to the repo event directly and the handler can go.

  Ecto also reports transaction control (`begin`, `commit`, `rollback`) as
  queries. The handler drops those, so the metrics count statements only.

  The handler compares the statement with those three words and copies
  nothing from the event but the measurements and the outcome. No SQL text,
  parameter or table name can reach a metric.
  """

  import Telemetry.Metrics

  @buckets [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000]
  @transaction_control ["begin", "commit", "rollback"]

  @duration_event [:td_core, :telemetry, :db, :operation_duration]
  @wait_event [:td_core, :telemetry, :db, :connection_wait]
  @operation_event [:td_core, :telemetry, :db, :operation]

  @doc "The metric definitions."
  def metrics do
    [
      distribution("db.client.operation.duration",
        event_name: @duration_event,
        measurement: :duration,
        unit: {:native, :millisecond},
        description:
          "Time the database spent on a query, in milliseconds, excluding decode time.",
        reporter_options: [buckets: @buckets]
      ),
      distribution("db.client.connection.wait_time",
        event_name: @wait_event,
        measurement: :wait_time,
        unit: {:native, :millisecond},
        description: "Time a query waited for a pool connection, in milliseconds.",
        reporter_options: [buckets: @buckets]
      ),
      counter("truedat.db.client.operations",
        event_name: @operation_event,
        tags: [:"truedat.outcome"],
        description: "Queries run, by outcome: success or failure."
      )
    ]
  end

  @doc """
  The options for `TdCore.Telemetry.Exporter`: the metrics, and the step that
  attaches the handler to `repo`'s query event. The event follows Ecto's
  `:telemetry_prefix`, so it stays right if the repo changes its prefix.
  """
  @spec wiring(module()) :: [metrics: [Telemetry.Metrics.t()], attach: [mfa()]]
  def wiring(repo) when is_atom(repo) do
    [metrics: metrics(), attach: [{__MODULE__, :attach, [query_event(repo)]}]]
  end

  @doc "The repo's query event, `telemetry_prefix ++ [:query]`."
  @spec query_event(module()) :: [atom()]
  def query_event(repo), do: repo.config()[:telemetry_prefix] ++ [:query]

  @doc "Attaches the handler to a query event. Attaching twice is harmless."
  @spec attach([atom()]) :: :ok
  def attach(event) do
    _ = :telemetry.attach({__MODULE__, event}, event, &__MODULE__.handle_query/4, nil)
    :ok
  end

  @doc "Detaches the handler from a query event."
  @spec detach([atom()]) :: :ok
  def detach(event) do
    _ = :telemetry.detach({__MODULE__, event})
    :ok
  end

  @doc false
  def handle_query(_event, _measurements, %{query: query}, _config)
      when query in @transaction_control,
      do: :ok

  def handle_query(_event, measurements, metadata, _config) do
    emit(@duration_event, :duration, measurements[:query_time])
    emit(@wait_event, :wait_time, measurements[:queue_time])
    :telemetry.execute(@operation_event, %{}, %{"truedat.outcome": outcome(metadata)})
  end

  defp emit(event, key, value) when is_number(value),
    do: :telemetry.execute(event, %{key => value}, %{})

  defp emit(_event, _key, nil), do: :ok

  defp outcome(%{result: {:ok, _}}), do: "success"
  defp outcome(_metadata), do: "failure"
end
