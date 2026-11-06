defmodule TdCore.Telemetry.EctoMetrics do
  @moduledoc """
  Database metrics shared by every Postgres-backed service, taken from the Ecto
  query event.

  A service gives `wiring/1` its own repo event and passes the result to
  `TdCore.Telemetry.Exporter`, so the handler exists only when telemetry is on
  and is detached again if the exporter is switched off after it was attached:

      {TdCore.Telemetry.Exporter, TdCore.Telemetry.EctoMetrics.wiring([:td_ai, :repo, :query])}

  A service that has more metrics concatenates the `:metrics`, `:setup` and
  `:teardown` lists of each `wiring/1`, and keeps all of them in one place
  (EXP-7).

  ## Why a handler sits between Ecto and the metrics

  Ecto leaves a measurement out of its event when it has no value: a query run
  inside a transaction has no `queue_time`. A metric definition cannot skip such
  an event, and the exporter crashes on the missing value, after which
  `:telemetry` detaches its handler and every later query goes uncounted. So
  `attach/1` re-emits one derived event per measurement that is present, and the
  definitions listen to those.

  The handler copies only measurements and the outcome. It never reads `params`,
  `cast_params`, `query` or `source` from the Ecto event (PRIV-5), so they cannot
  reach a metric even by mistake.
  """

  import Telemetry.Metrics

  @buckets [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000]

  @duration_event [:td_core, :telemetry, :db, :operation_duration]
  @wait_event [:td_core, :telemetry, :db, :connection_wait]
  @operation_event [:td_core, :telemetry, :db, :operation]

  @doc """
  The metric definitions. They depend on `Telemetry.Metrics` only, not on the
  package that delivers them (EXP-6).
  """
  def metrics do
    [
      distribution("db.client.operation.duration",
        event_name: @duration_event,
        measurement: :duration,
        unit: {:native, :millisecond},
        description:
          "Time the database spent on a query, in milliseconds. " <>
            "The standard defines this name in seconds. " <>
            "Ours is milliseconds and excludes decode time.",
        reporter_options: [buckets: @buckets]
      ),
      distribution("db.client.connection.wait_time",
        event_name: @wait_event,
        measurement: :wait_time,
        unit: {:native, :millisecond},
        description:
          "Time a query waited for a pool connection, in milliseconds. " <>
            "The standard defines this name in seconds. Ours is milliseconds.",
        reporter_options: [buckets: @buckets]
      ),
      counter("truedat.db.client.operations",
        event_name: @operation_event,
        measurement: :count,
        tags: [:"truedat.outcome"],
        description: "Queries run, by outcome: success or failure."
      )
    ]
  end

  @doc """
  The options to give `TdCore.Telemetry.Exporter`: the metrics, the step that
  attaches the handler to `event_prefix`, and the step that detaches it.
  """
  @spec wiring([atom()]) :: [
          metrics: [Telemetry.Metrics.t()],
          setup: [mfa()],
          teardown: [mfa()]
        ]
  def wiring(event_prefix) when is_list(event_prefix) do
    [
      metrics: metrics(),
      setup: [{__MODULE__, :attach, [event_prefix]}],
      teardown: [{__MODULE__, :detach, [event_prefix]}]
    ]
  end

  @doc """
  Attaches the handler to a repo's query event, such as `[:td_ai, :repo, :query]`.
  Attaching twice for the same event is harmless.
  """
  @spec attach([atom()]) :: :ok
  def attach(event_prefix) when is_list(event_prefix) do
    _ =
      :telemetry.attach(handler_id(event_prefix), event_prefix, &__MODULE__.handle_query/4, nil)

    :ok
  end

  @doc "Detaches the handler for a repo's query event."
  @spec detach([atom()]) :: :ok
  def detach(event_prefix) when is_list(event_prefix) do
    _ = :telemetry.detach(handler_id(event_prefix))
    :ok
  end

  @doc false
  def handle_query(_event, measurements, metadata, _config) do
    emit(@duration_event, :duration, Map.get(measurements, :query_time))
    emit(@wait_event, :wait_time, Map.get(measurements, :queue_time))

    :telemetry.execute(@operation_event, %{count: 1}, %{"truedat.outcome": outcome(metadata)})
  end

  defp emit(event, key, value) when is_number(value),
    do: :telemetry.execute(event, %{key => value}, %{})

  defp emit(_event, _key, _absent), do: :ok

  defp outcome(%{result: {:ok, _}}), do: "success"
  defp outcome(_failed_or_unknown), do: "failure"

  defp handler_id(event_prefix), do: {__MODULE__, event_prefix}
end
