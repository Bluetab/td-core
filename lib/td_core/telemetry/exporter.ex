defmodule TdCore.Telemetry.Exporter do
  @moduledoc """
  The child a service adds to its supervision tree to export metrics as OTLP
  protobuf with gzip, through `otel_metric_exporter`.

      {TdCore.Telemetry.Exporter, TdCore.Telemetry.EctoMetrics.wiring(MyApp.Repo)}

  It starts only when an endpoint is set. The endpoint comes from the
  `:endpoint` option, else `config :td_core, TdCore.Telemetry.Exporter,
  endpoint: ...`, else `OTEL_EXPORTER_OTLP_ENDPOINT`. When it is unset or empty
  the child returns `:ignore` and nothing runs. Services set `endpoint: nil` in
  `config/test.exs`, so a variable in the shell cannot switch it on in tests.

  `otel_metric_exporter` reads the other `OTEL_*` variables itself, among them
  `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`. This module adds
  `service.instance.id` from `POD_NAME`.

  Telemetry never takes a service down. If a protocol variable holds anything
  but `http/protobuf`, the options are invalid, or the exporter or an `:attach`
  step fails, the child logs one warning and returns `:ignore`. The protocol is
  checked here because on an invalid value the exporter silently drops every
  `OTEL_*` variable, the resource attributes included. The child is
  `:temporary`, so a crash after start is not restarted into a crash loop.

  ## Options

    * `:metrics` - `Telemetry.Metrics` definitions. `summary` is not supported
      and switches telemetry off. Required.
    * `:attach` - `{module, function, args}` steps run once the exporter is up,
      for handlers that must exist only while telemetry is on.
    * `:export_period` - milliseconds between exports. One minute by default.
    * `:resource` - resource attributes to use instead of `POD_NAME`.
    * `:endpoint` - see above.
  """

  require Logger

  @protocol_vars ["OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"]

  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :temporary
    }
  end

  @doc false
  def start_link(opts) do
    case endpoint(opts) do
      blank when blank in [nil, ""] -> :ignore
      endpoint -> start(endpoint, opts)
    end
  end

  defp start(endpoint, opts) do
    with :ok <- check_protocol(),
         {:ok, pid} <- OtelMetricExporter.start_link(exporter_opts(endpoint, opts)) do
      attach(pid, Keyword.get(opts, :attach, []))
    else
      {:error, reason} -> off(reason)
    end
  rescue
    exception -> off(Exception.message(exception))
  catch
    :exit, reason -> off(reason)
  end

  defp endpoint(opts) do
    Keyword.get_lazy(opts, :endpoint, fn ->
      :td_core
      |> Application.get_env(__MODULE__, [])
      |> Keyword.get_lazy(:endpoint, fn -> System.get_env("OTEL_EXPORTER_OTLP_ENDPOINT") end)
    end)
  end

  defp check_protocol do
    case Enum.find(@protocol_vars, &(System.get_env(&1) not in [nil, "http/protobuf"])) do
      nil -> :ok
      var -> {:error, "#{var} must be http/protobuf"}
    end
  end

  defp exporter_opts(endpoint, opts) do
    [
      metrics: Keyword.get(opts, :metrics),
      export_period: Keyword.get(opts, :export_period, :timer.minutes(1)),
      # The exporter appends /v1/metrics, so a trailing slash would give //v1/metrics.
      otlp_endpoint: String.trim_trailing(endpoint, "/"),
      otlp_protocol: :http_protobuf,
      otlp_compression: :gzip,
      resource: Keyword.get_lazy(opts, :resource, &pod_resource/0)
    ]
  end

  defp pod_resource do
    case System.get_env("POD_NAME") do
      pod when pod in [nil, ""] -> %{}
      pod -> %{"service.instance.id" => pod}
    end
  end

  # A step that raises stops the exporter. Handlers attached by earlier steps stay attached,
  # but they only feed metrics that nobody records any more.
  defp attach(pid, steps) do
    Enum.each(steps, fn {m, f, a} -> apply(m, f, a) end)
    {:ok, pid}
  rescue
    exception ->
      Supervisor.stop(pid)
      off(Exception.message(exception))
  end

  # The reason never holds the endpoint: it is a valid string, so validation never quotes it.
  defp off(reason) do
    reason = if is_binary(reason), do: reason, else: inspect(reason, limit: 5)
    Logger.warning("telemetry is off: #{String.slice(reason, 0, 300)}")
    :ignore
  end
end
