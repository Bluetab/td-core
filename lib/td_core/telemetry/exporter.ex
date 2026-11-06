defmodule TdCore.Telemetry.Exporter do
  @moduledoc """
  The child a service adds to its supervision tree to export metrics.

      children = [
        ...,
        {TdCore.Telemetry.Exporter, metrics: my_metrics()}
      ]

  It starts `otel_metric_exporter` only when an endpoint is set, and exports
  every minute as OTLP protobuf with gzip.

  ## Settings

  Each setting is read from the first of these that has a value: an option
  given to the child, the `:td_core` application config under this module's
  name, then the environment variable the chart sets in production.

  | Setting            | Option / config key | Environment variable                  |
  |--------------------|---------------------|---------------------------------------|
  | endpoint           | `:endpoint`         | `OTEL_EXPORTER_OTLP_ENDPOINT`         |
  | protocol           | `:protocol`         | `OTEL_EXPORTER_OTLP_PROTOCOL`         |
  | metrics protocol   | `:metrics_protocol` | `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` |
  | resource           | `:resource`         | `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `POD_NAME` |

  The resource is the five attributes of RES-1. From the environment, the
  exporter reads `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` itself and
  this module adds `service.instance.id` from `POD_NAME`. A `:resource` option
  or config value replaces the environment.

  A trailing slash on the endpoint is removed, because the exporter appends
  `/v1/metrics` and would otherwise post to `//v1/metrics`.

  ## Telemetry never takes a service down

  With the endpoint unset or empty, the child returns `:ignore`: no process,
  no telemetry handler, no connection and no log line.

  With the endpoint set, any problem that would stop the exporter turns
  telemetry off instead, and logs one warning that does not include the
  endpoint. That covers an unsupported protocol, an unusable `:metrics`
  option, and an exporter that fails to start, raises, or exits while starting.
  The child is `:temporary`, so a later exporter crash is not restarted into a
  crash loop.

  The protocol is checked here because the exporter itself reacts to an
  unsupported value by discarding every `OTEL_*` setting, the endpoint included.

  ## Metric types

  The exporter accepts `counter`, `sum`, `last_value` and `distribution`, and
  refuses to start when its list holds anything else. A `summary` in the list
  is therefore dropped before the list reaches the exporter, so an old
  `summary` list cannot switch telemetry off for the metrics that do export
  (EXP-4). One warning names every dropped metric, so a misconfiguration is not
  silent. When no supported metric remains, telemetry is off and a second
  warning says so.

  ## Options

    * `:metrics` - the list of `Telemetry.Metrics` definitions. Required.
    * `:setup` - a list of `{module, function, args}` run once the exporter has
      started, for anything that must exist only while telemetry is on, such as
      a telemetry handler (EXP-1).
    * `:teardown` - a list of `{module, function, args}` that undo `:setup`.
      They run only when a `:setup` step fails and telemetry is switched off,
      so that neither the exporter nor anything an earlier step attached is
      left behind.
    * `:export_period` - milliseconds between exports. Defaults to one minute.
    * `:id` - the child id. Defaults to this module.
  """

  require Logger

  @supported_protocol "http/protobuf"
  @max_reason_length 300

  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :id, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :temporary
    }
  end

  @doc false
  def start_link(opts) do
    case setting(opts, :endpoint, "OTEL_EXPORTER_OTLP_ENDPOINT") do
      endpoint when endpoint in [nil, ""] -> :ignore
      endpoint -> start_guarded(endpoint, opts)
    end
  end

  defp start_guarded(endpoint, opts) do
    with :ok <- check_protocol(opts),
         {:ok, metrics} <- exportable(Keyword.get(opts, :metrics)),
         {:ok, pid} <- start_exporter(endpoint, Keyword.put(opts, :metrics, metrics)) do
      {:ok, pid}
    else
      {:error, reason} -> disabled(reason)
    end
  rescue
    exception -> disabled(Exception.message(exception))
  catch
    :exit, reason -> disabled(inspect(reason, limit: 10))
  end

  defp setting(opts, key, env_var) do
    with :error <- Keyword.fetch(opts, key),
         :error <- Keyword.fetch(config(), key) do
      System.get_env(env_var)
    else
      {:ok, value} -> value
    end
  end

  defp config, do: Application.get_env(:td_core, __MODULE__, [])

  defp check_protocol(opts) do
    [
      {"OTEL_EXPORTER_OTLP_PROTOCOL", setting(opts, :protocol, "OTEL_EXPORTER_OTLP_PROTOCOL")},
      {"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
       setting(opts, :metrics_protocol, "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")}
    ]
    |> Enum.find_value(:ok, fn
      {_name, value} when value in [nil, @supported_protocol] ->
        nil

      {name, value} ->
        {:error,
         "#{name} is #{inspect(value)}, only #{inspect(@supported_protocol)} is supported"}
    end)
  end

  defp exportable(metrics) when is_list(metrics) do
    {kept, dropped} = Enum.split_with(metrics, &supported?/1)

    if dropped != [] do
      names = Enum.map_join(dropped, ", ", &metric_name/1)
      Logger.warning("telemetry: unsupported metric types are not exported: #{names}")
    end

    case kept do
      [] -> {:error, "no metric of a supported type was given"}
      kept -> {:ok, kept}
    end
  end

  defp exportable(other),
    do: {:error, "the :metrics option must be a list, got #{inspect(other, limit: 3)}"}

  defp supported?(%Telemetry.Metrics.Counter{}), do: true
  defp supported?(%Telemetry.Metrics.Sum{}), do: true
  defp supported?(%Telemetry.Metrics.LastValue{}), do: true
  defp supported?(%Telemetry.Metrics.Distribution{}), do: true
  defp supported?(_other), do: false

  defp metric_name(%{name: name}) when is_list(name), do: Enum.join(name, ".")
  defp metric_name(other), do: inspect(other, limit: 3)

  defp start_exporter(endpoint, opts) do
    started =
      OtelMetricExporter.start_link(
        metrics: Keyword.fetch!(opts, :metrics),
        export_period: Keyword.get(opts, :export_period, :timer.minutes(1)),
        otlp_endpoint: String.trim_trailing(endpoint, "/"),
        otlp_protocol: :http_protobuf,
        otlp_compression: :gzip,
        resource: resource(opts)
      )

    with {:ok, pid} <- started do
      setup_or_stop(pid, opts)
    end
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, inspect(reason, limit: 10)}
  end

  defp setup_or_stop(pid, opts) do
    Enum.each(Keyword.get(opts, :setup, []), fn {m, f, a} -> apply(m, f, a) end)
    {:ok, pid}
  rescue
    exception ->
      undo(pid, opts)
      {:error, Exception.message(exception)}
  catch
    :exit, reason ->
      undo(pid, opts)
      {:error, inspect(reason, limit: 10)}
  end

  defp undo(pid, opts) do
    stop(pid)

    for {m, f, a} <- Keyword.get(opts, :teardown, []) do
      try do
        apply(m, f, a)
      catch
        _kind, _reason -> :ok
      end
    end

    :ok
  end

  defp stop(pid) do
    Supervisor.stop(pid)
  catch
    :exit, _already_gone -> :ok
  end

  defp disabled(reason) do
    Logger.warning("telemetry is off: the metrics exporter did not start (#{message(reason)})")
    :ignore
  end

  defp message(reason) when is_binary(reason), do: truncate(reason)
  defp message(%{__exception__: true} = exception), do: truncate(Exception.message(exception))
  defp message(reason), do: truncate(inspect(reason, limit: 10, printable_limit: 200))

  defp truncate(text) do
    text = text |> String.replace(~r/\s+/, " ") |> String.trim()

    if String.length(text) > @max_reason_length,
      do: String.slice(text, 0, @max_reason_length) <> "...",
      else: text
  end

  defp resource(opts) do
    case setting(opts, :resource, "") do
      resource when is_map(resource) -> resource
      _from_environment -> pod_name_attribute()
    end
  end

  defp pod_name_attribute do
    case System.get_env("POD_NAME") do
      pod when pod in [nil, ""] -> %{}
      pod -> %{"service.instance.id" => pod}
    end
  end
end
