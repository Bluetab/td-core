if Application.compile_env(:td_core, :otlp_receiver, false) do
  defmodule TdCore.TestSupport.OtlpReceiver do
    @moduledoc """
    A local OTLP/HTTP receiver for tests. It is the seam of the observability
    work: a service's OTLP output.

    It listens on an ephemeral port inside the test process, accepts protobuf
    (optionally gzip-compressed) at `/v1/metrics`, decodes it with the protobuf
    modules shipped by `otel_metric_exporter`, and hands each export to the test
    as plain maps. Tests assert on names, types, units, bucket bounds, labels and
    the absence of forbidden attributes, never on protobuf structs.

    ## Usage

        setup do
          receiver = TdCore.TestSupport.OtlpReceiver.start()
          {:ok, receiver: receiver}
        end

        test "exports", %{receiver: receiver} do
          start_supervised!({TdCore.Telemetry.Exporter, endpoint: receiver.endpoint, metrics: metrics})

          metric = OtlpReceiver.await_metric(receiver, "my.metric")
          assert %{type: :sum} = metric
        end

    The receiver stops with the test that started it.

    `await_export/2` and `refute_export/2` read the test process's mailbox, one
    export at a time. `await_metric/3` reads the receiver's history of every
    export instead, so one export can answer several lookups and the mailbox is
    left undisturbed. A request that arrives while the test is tearing down is
    answered with `200` and not recorded, so a late export never crashes the
    receiver.

    ## Compiled only for tests

    The module exists only when `config :td_core, :otlp_receiver, true` is set,
    so it never ships in a release. Each service sets it in `config/test.exs`.
    `td_cache` and `td_cluster` cannot depend on `td_core`, so their tests copy
    this file instead.

    ## Response to the sender

    A request that is not protobuf, or whose body cannot be decoded, gets a
    `400`, so a misbehaving exporter fails a test instead of passing silently.
    `respond_with/2` makes the receiver answer with another status, for example
    to simulate an outage. Exports are recorded whatever the status.
    """

    alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
    alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceResponse
    alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.AnyValue
    alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.KeyValue

    @default_timeout 5_000

    @type receiver :: %{
            endpoint: String.t(),
            port: :inet.port_number(),
            agent: pid()
          }
    @type export :: %{
            path: String.t(),
            content_type: String.t() | nil,
            compression: :gzip | nil,
            metrics: [map()]
          }

    @doc """
    Starts a receiver owned by the calling test process and returns its handle.
    `endpoint` is the base URL to give to `OTEL_EXPORTER_OTLP_ENDPOINT`.
    """
    @spec start() :: receiver()
    def start do
      test_pid = self()
      {:ok, agent} = Agent.start_link(fn -> %{test: test_pid, status: 200, history: []} end)
      listener = {__MODULE__, make_ref()}

      {:ok, _server} =
        Plug.Cowboy.http(__MODULE__.Router, %{agent: agent},
          ref: listener,
          port: 0,
          ip: {127, 0, 0, 1}
        )

      port = :ranch.get_port(listener)
      ExUnit.Callbacks.on_exit(fn -> Plug.Cowboy.shutdown(listener) end)

      %{endpoint: "http://127.0.0.1:#{port}", port: port, agent: agent}
    end

    @doc """
    Waits for the next export and returns it, or fails the test with a message
    naming what was waited for and how long.
    """
    @spec await_export(receiver(), timeout()) :: export()
    def await_export(%{agent: agent, port: port}, timeout \\ @default_timeout) do
      receive do
        {__MODULE__, ^agent, export} -> export
      after
        timeout ->
          ExUnit.Assertions.flunk(
            "no OTLP export reached the receiver on port #{port} within #{timeout}ms"
          )
      end
    end

    @doc """
    Waits until an export contains a metric called `name` and returns that
    metric. Exports without it are skipped, which matters because an exporter
    sends on every period, including periods with nothing recorded.

    Fails the test, naming the metric and the names it did see, when none
    arrives within `timeout` milliseconds in total.
    """
    @spec await_metric(receiver(), String.t(), timeout()) :: map()
    def await_metric(receiver, name, timeout \\ @default_timeout) do
      deadline = System.monotonic_time(:millisecond) + timeout
      poll_metric(receiver, name, deadline, timeout)
    end

    defp poll_metric(%{agent: agent, port: port} = receiver, name, deadline, timeout) do
      history = Agent.get(agent, & &1.history)

      case history |> Enum.flat_map(& &1.metrics) |> Enum.find(&(&1.name == name)) do
        nil ->
          if System.monotonic_time(:millisecond) >= deadline do
            seen = history |> Enum.flat_map(& &1.metrics) |> Enum.map(& &1.name) |> Enum.uniq()

            ExUnit.Assertions.flunk(
              "no metric #{inspect(name)} reached the receiver within #{timeout}ms " <>
                "(port #{port}); saw #{inspect(seen)}"
            )
          else
            Process.sleep(10)
            poll_metric(receiver, name, deadline, timeout)
          end

        metric ->
          metric
      end
    end

    @doc """
    Asserts that no export arrives within `timeout` milliseconds.
    """
    @spec refute_export(receiver(), timeout()) :: :ok
    def refute_export(%{agent: agent, port: port}, timeout \\ 200) do
      receive do
        {__MODULE__, ^agent, export} ->
          ExUnit.Assertions.flunk(
            "expected no OTLP export on port #{port}, got #{length(export.metrics)} metric(s)"
          )
      after
        timeout -> :ok
      end
    end

    @doc "Makes the receiver answer every following request with `status`."
    @spec respond_with(receiver(), pos_integer()) :: :ok
    def respond_with(%{agent: agent}, status) when is_integer(status) do
      Agent.update(agent, &Map.put(&1, :status, status))
    end

    @doc false
    def status(agent) do
      Agent.get(agent, & &1.status)
    catch
      :exit, _ -> 200
    end

    @doc false
    def notify(agent, export) do
      test_pid =
        Agent.get_and_update(
          agent,
          &{&1.test, Map.update!(&1, :history, fn h -> [export | h] end)}
        )

      send(test_pid, {__MODULE__, agent, export})
      :ok
    catch
      :exit, _ -> :ok
    end

    @doc false
    def decode_metrics(%ExportMetricsServiceRequest{resource_metrics: resource_metrics}) do
      for rm <- resource_metrics,
          resource = attributes(rm.resource && rm.resource.attributes),
          sm <- rm.scope_metrics,
          metric <- sm.metrics do
        metric
        |> decode_metric()
        |> Map.put(:resource, resource)
      end
    end

    defp decode_metric(%{data: {:sum, sum}} = metric) do
      base(metric, :sum)
      |> Map.merge(%{
        monotonic: sum.is_monotonic,
        temporality: temporality(sum.aggregation_temporality),
        points: Enum.map(sum.data_points, &number_point/1)
      })
    end

    defp decode_metric(%{data: {:gauge, gauge}} = metric) do
      base(metric, :gauge)
      |> Map.put(:points, Enum.map(gauge.data_points, &number_point/1))
    end

    defp decode_metric(%{data: {:histogram, histogram}} = metric) do
      base(metric, :histogram)
      |> Map.merge(%{
        temporality: temporality(histogram.aggregation_temporality),
        points: Enum.map(histogram.data_points, &histogram_point/1)
      })
    end

    defp decode_metric(%{data: {other, _}} = metric),
      do: base(metric, other) |> Map.put(:points, [])

    defp decode_metric(metric), do: base(metric, nil) |> Map.put(:points, [])

    defp base(metric, type) do
      %{
        name: metric.name,
        description: metric.description,
        unit: metric.unit,
        type: type
      }
    end

    defp number_point(point) do
      {_kind, value} = point.value || {nil, nil}

      %{
        attributes: attributes(point.attributes),
        value: value
      }
    end

    defp histogram_point(point) do
      %{
        attributes: attributes(point.attributes),
        count: point.count,
        sum: point.sum,
        bounds: point.explicit_bounds,
        bucket_counts: point.bucket_counts
      }
    end

    defp temporality(:AGGREGATION_TEMPORALITY_CUMULATIVE), do: :cumulative
    defp temporality(:AGGREGATION_TEMPORALITY_DELTA), do: :delta
    defp temporality(_), do: :unspecified

    defp attributes(nil), do: %{}

    defp attributes(key_values) do
      Map.new(key_values, fn %KeyValue{key: key, value: value} -> {key, any_value(value)} end)
    end

    defp any_value(%AnyValue{value: {_kind, value}}), do: value
    defp any_value(%AnyValue{value: nil}), do: nil

    defmodule Router do
      @moduledoc false
      @behaviour Plug

      import Plug.Conn

      alias TdCore.TestSupport.OtlpReceiver

      @impl true
      def init(opts), do: opts

      @impl true
      def call(%Plug.Conn{method: "POST", request_path: "/v1/metrics"} = conn, %{agent: agent}) do
        {:ok, raw, conn} = read_body(conn, length: 64_000_000)
        content_type = conn |> get_req_header("content-type") |> List.first()

        with :ok <- check_content_type(content_type),
             {:ok, compression, body} <- decompress(conn, raw),
             {:ok, request} <- decode(body) do
          status = OtlpReceiver.status(agent)

          OtlpReceiver.notify(agent, %{
            path: conn.request_path,
            content_type: content_type,
            compression: compression,
            metrics: OtlpReceiver.decode_metrics(request)
          })

          reply(conn, status)
        else
          {:error, message} -> send_resp(conn, 400, message)
        end
      end

      def call(conn, _opts), do: send_resp(conn, 404, "not found")

      defp reply(conn, status) do
        conn
        |> put_resp_content_type("application/x-protobuf")
        |> send_resp(status, Protobuf.encode(%ExportMetricsServiceResponse{}))
      end

      defp check_content_type("application/x-protobuf" <> _), do: :ok

      defp check_content_type(other),
        do: {:error, "expected content-type application/x-protobuf, got #{inspect(other)}"}

      defp decompress(conn, raw) do
        case get_req_header(conn, "content-encoding") do
          ["gzip"] ->
            {:ok, :gzip, :zlib.gunzip(raw)}

          [] ->
            {:ok, nil, raw}

          other ->
            {:error, "unsupported content-encoding #{inspect(other)}"}
        end
      rescue
        error -> {:error, "cannot gunzip body: #{Exception.message(error)}"}
      end

      defp decode(body) do
        {:ok, ExportMetricsServiceRequest.decode(body)}
      rescue
        error -> {:error, "cannot decode OTLP protobuf: #{Exception.message(error)}"}
      end
    end
  end
end
