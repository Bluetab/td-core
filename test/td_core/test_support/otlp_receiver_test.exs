defmodule TdCore.TestSupport.OtlpReceiverTest do
  use ExUnit.Case, async: true

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.AnyValue
  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.KeyValue
  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.Histogram
  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.HistogramDataPoint
  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.Metric
  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.NumberDataPoint
  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.ResourceMetrics
  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.ScopeMetrics
  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.Sum
  alias OtelMetricExporter.Opentelemetry.Proto.Resource.V1.Resource
  alias TdCore.TestSupport.OtlpReceiver

  setup_all do
    {:ok, _} = Application.ensure_all_started(:inets)
    :ok
  end

  setup do
    {:ok, receiver: OtlpReceiver.start()}
  end

  describe "a gzip protobuf metrics payload posted to /v1/metrics" do
    test "is decoded to resource, name, unit, type, labels and value", %{receiver: receiver} do
      payload =
        request(
          [{"service.name", "td-ai"}, {"deployment.environment.name", "test"}],
          [
            %Metric{
              name: "truedat.db.client.operations",
              description: "Queries",
              unit: "1",
              data:
                {:sum,
                 %Sum{
                   is_monotonic: true,
                   aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
                   data_points: [
                     %NumberDataPoint{
                       attributes: [kv("truedat.outcome", "success")],
                       value: {:as_int, 7}
                     }
                   ]
                 }}
            }
          ]
        )

      assert 200 == post_metrics(receiver, payload)

      export = OtlpReceiver.await_export(receiver)

      assert export.path == "/v1/metrics"
      assert export.content_type == "application/x-protobuf"
      assert export.compression == :gzip

      assert [metric] = export.metrics

      assert %{
               resource: %{"service.name" => "td-ai", "deployment.environment.name" => "test"},
               name: "truedat.db.client.operations",
               description: "Queries",
               unit: "1",
               type: :sum,
               monotonic: true,
               temporality: :cumulative,
               points: [%{attributes: %{"truedat.outcome" => "success"}, value: 7}]
             } = metric
    end
  end

  describe "a histogram payload" do
    test "is decoded with bucket bounds, bucket counts, count, sum and unit", %{
      receiver: receiver
    } do
      payload =
        request(
          [{"service.name", "td-ai"}],
          [
            %Metric{
              name: "db.client.operation.duration",
              unit: "ms",
              data:
                {:histogram,
                 %Histogram{
                   aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
                   data_points: [
                     %HistogramDataPoint{
                       attributes: [kv("truedat.endpoint", "dd")],
                       count: 3,
                       sum: 61.0,
                       explicit_bounds: [5.0, 50.0],
                       bucket_counts: [1, 1, 1]
                     }
                   ]
                 }}
            }
          ]
        )

      assert 200 == post_metrics(receiver, payload)

      assert %{metrics: [metric]} = OtlpReceiver.await_export(receiver)

      assert %{
               name: "db.client.operation.duration",
               unit: "ms",
               type: :histogram,
               points: [
                 %{
                   attributes: %{"truedat.endpoint" => "dd"},
                   count: 3,
                   sum: 61.0,
                   bounds: [5.0, 50.0],
                   bucket_counts: [1, 1, 1]
                 }
               ]
             } = metric
    end
  end

  describe "await_export/2" do
    test "fails with a message naming the port and the wait when nothing arrives", %{
      receiver: %{port: port} = receiver
    } do
      error =
        assert_raise ExUnit.AssertionError, fn -> OtlpReceiver.await_export(receiver, 50) end

      assert error.message =~ "no OTLP export reached the receiver on port #{port} within 50ms"
    end
  end

  describe "await_metric/3" do
    test "skips exports without the metric and returns the first one that has it", %{
      receiver: receiver
    } do
      counter = %Metric{
        name: "truedat.db.client.operations",
        data: {:sum, %Sum{data_points: [%NumberDataPoint{value: {:as_int, 1}}]}}
      }

      assert 200 == post_metrics(receiver, request([], []))
      assert 200 == post_metrics(receiver, request([], [counter]))

      assert %{name: "truedat.db.client.operations", points: [%{value: 1}]} =
               OtlpReceiver.await_metric(receiver, "truedat.db.client.operations")
    end

    test "can await several metrics that arrived in the same export", %{receiver: receiver} do
      counter = fn name ->
        %Metric{
          name: name,
          data: {:sum, %Sum{data_points: [%NumberDataPoint{value: {:as_int, 1}}]}}
        }
      end

      assert 200 == post_metrics(receiver, request([], [counter.("first"), counter.("second")]))

      assert %{name: "first"} = OtlpReceiver.await_metric(receiver, "first")
      assert %{name: "second"} = OtlpReceiver.await_metric(receiver, "second")
    end

    test "fails naming the metric and the names it did see", %{receiver: receiver} do
      other = %Metric{
        name: "some.other.metric",
        data: {:sum, %Sum{data_points: [%NumberDataPoint{value: {:as_int, 1}}]}}
      }

      assert 200 == post_metrics(receiver, request([], [other]))

      error =
        assert_raise ExUnit.AssertionError, fn ->
          OtlpReceiver.await_metric(receiver, "wanted.metric", 100)
        end

      assert error.message =~ ~s(no metric "wanted.metric" reached the receiver within 100ms)
      assert error.message =~ "some.other.metric"
    end
  end

  describe "refute_export/2" do
    test "passes when nothing arrives", %{receiver: receiver} do
      assert :ok == OtlpReceiver.refute_export(receiver, 50)
    end

    test "fails when an export arrives", %{receiver: receiver} do
      assert 200 == post_metrics(receiver, request([], []))

      assert_raise ExUnit.AssertionError, ~r/expected no OTLP export/, fn ->
        OtlpReceiver.refute_export(receiver, 500)
      end
    end
  end

  describe "malformed requests" do
    test "a body that is not protobuf is refused with 400 and not recorded", %{
      receiver: %{endpoint: endpoint} = receiver
    } do
      assert 400 == post(endpoint <> "/v1/metrics", "{}", "application/json", [])
      OtlpReceiver.refute_export(receiver, 50)
    end

    test "a protobuf body that does not decode is refused with 400", %{
      receiver: %{endpoint: endpoint} = receiver
    } do
      assert 400 ==
               post(endpoint <> "/v1/metrics", <<255, 255, 255>>, "application/x-protobuf", [])

      OtlpReceiver.refute_export(receiver, 50)
    end
  end

  describe "respond_with/2" do
    test "answers with the given status and still records the export", %{receiver: receiver} do
      :ok = OtlpReceiver.respond_with(receiver, 503)

      assert 503 == post_metrics(receiver, request([{"service.name", "td-ai"}], []))
      assert %{metrics: []} = OtlpReceiver.await_export(receiver)
    end
  end

  defp request(resource_attributes, metrics) do
    %ExportMetricsServiceRequest{
      resource_metrics: [
        %ResourceMetrics{
          resource: %Resource{attributes: Enum.map(resource_attributes, &kv/1)},
          scope_metrics: [%ScopeMetrics{metrics: metrics}]
        }
      ]
    }
  end

  defp kv({key, value}), do: kv(key, value)
  defp kv(key, value), do: %KeyValue{key: key, value: %AnyValue{value: {:string_value, value}}}

  defp post_metrics(%{endpoint: endpoint}, request, http_options \\ []) do
    post_body(endpoint, encode_body(request), http_options)
  end

  defp encode_body(request),
    do: request |> Protobuf.encode_to_iodata() |> IO.iodata_to_binary() |> :zlib.gzip()

  defp post_body(endpoint, body, http_options) do
    post(
      endpoint <> "/v1/metrics",
      body,
      "application/x-protobuf",
      [{~c"content-encoding", ~c"gzip"}],
      http_options
    )
  end

  defp post(url, body, content_type, headers, http_options \\ []) do
    case :httpc.request(
           :post,
           {String.to_charlist(url), headers, String.to_charlist(content_type), body},
           http_options,
           []
         ) do
      {:ok, {{_, status, _}, _, _}} -> status
      {:error, reason} -> {:error, reason}
    end
  end
end
