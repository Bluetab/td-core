defmodule TdCore.Telemetry.EctoMetricsTest do
  # The exporter keeps a global ETS table and a persistent_term key, so these
  # tests cannot run concurrently.
  use ExUnit.Case, async: false

  alias TdCore.Telemetry.EctoMetrics
  alias TdCore.Telemetry.Exporter
  alias TdCore.TestSupport.OtlpReceiver

  @event [:td_test, :repo, :query]
  @request_like_bounds [
    5.0,
    10.0,
    25.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1000.0,
    2500.0,
    5000.0,
    10_000.0,
    30_000.0,
    60_000.0
  ]

  setup do
    on_exit(fn -> EctoMetrics.detach(@event) end)
    {:ok, receiver: OtlpReceiver.start()}
  end

  describe "a query through the real exporter" do
    test "produces both durations as millisecond distributions with the request-like buckets",
         %{receiver: receiver} do
      start_exporter(receiver)

      query(%{query_time: native_ms(12), queue_time: native_ms(3), decode_time: native_ms(1)})

      assert %{
               type: :histogram,
               unit: "ms",
               points: [%{count: 1, bounds: @request_like_bounds, sum: 12.0}]
             } = OtlpReceiver.await_metric(receiver, "db.client.operation.duration")

      assert %{
               type: :histogram,
               unit: "ms",
               points: [%{count: 1, bounds: @request_like_bounds, sum: 3.0}]
             } = OtlpReceiver.await_metric(receiver, "db.client.connection.wait_time")
    end

    test "states milliseconds in the descriptions, and that the duration excludes decode time",
         %{receiver: receiver} do
      start_exporter(receiver)
      query(%{query_time: native_ms(1), queue_time: native_ms(1)})

      assert %{description: duration} =
               OtlpReceiver.await_metric(receiver, "db.client.operation.duration")

      assert %{description: wait} =
               OtlpReceiver.await_metric(receiver, "db.client.connection.wait_time")

      assert duration =~ "milliseconds"
      assert duration =~ "excludes decode time"
      assert wait =~ "milliseconds"
    end
  end

  describe "the query counter" do
    test "counts queries by outcome, success or failure", %{receiver: receiver} do
      start_exporter(receiver)

      query(%{query_time: native_ms(1)}, %{result: {:ok, %{}}})
      query(%{query_time: native_ms(1)}, %{result: {:ok, %{}}})
      query(%{query_time: native_ms(1)}, %{result: {:error, %{message: "boom"}}})

      assert %{type: :sum, monotonic: true, points: points} =
               OtlpReceiver.await_metric(receiver, "truedat.db.client.operations")

      assert points
             |> Enum.map(&Map.take(&1, [:attributes, :value]))
             |> Enum.sort_by(& &1.attributes["truedat.outcome"]) == [
               %{attributes: %{"truedat.outcome" => "failure"}, value: 1},
               %{attributes: %{"truedat.outcome" => "success"}, value: 2}
             ]
    end
  end

  describe "a query inside a transaction (no queue_time)" do
    test "gives no wait-time sample, and later queries are still counted", %{receiver: receiver} do
      start_exporter(receiver)

      query(%{query_time: native_ms(4)})
      assert %{} = OtlpReceiver.await_metric(receiver, "db.client.operation.duration")

      query(%{query_time: native_ms(9), queue_time: native_ms(2)})

      assert %{points: [%{count: 1, sum: 2.0}]} =
               OtlpReceiver.await_metric(receiver, "db.client.connection.wait_time")

      assert 2 == counted(receiver, "success")
    end
  end

  describe "what leaves the service" do
    test "carries no SQL text, SQL parameters or table, and only the allowed labels", %{
      receiver: receiver
    } do
      start_exporter(receiver)

      query(
        %{query_time: native_ms(7), queue_time: native_ms(1)},
        %{
          query: "SELECT email FROM users WHERE name = $1",
          params: ["jane.doe@example.com"],
          cast_params: ["jane.doe@example.com"],
          source: "users",
          result: {:ok, %{}}
        }
      )

      OtlpReceiver.await_metric(receiver, "db.client.operation.duration")
      OtlpReceiver.await_metric(receiver, "db.client.connection.wait_time")
      OtlpReceiver.await_metric(receiver, "truedat.db.client.operations")

      exported = exported_metrics(receiver)
      assert length(exported) >= 3

      for metric <- exported, point <- metric.points do
        assert Map.keys(point.attributes) -- ["truedat.outcome"] == [],
               "#{metric.name} has an unexpected label: #{inspect(point.attributes)}"
      end

      dump = inspect(exported, limit: :infinity, printable_limit: :infinity)

      for secret <- ["SELECT", "WHERE", "$1", "jane.doe", "example.com", "users"] do
        refute dump =~ secret, "#{inspect(secret)} reached the export"
      end
    end
  end

  describe "with the endpoint unset" do
    test "the handler is never attached to the service's query event" do
      assert {:ok, :undefined} = start_supervised({Exporter, EctoMetrics.wiring(@event)})

      assert :telemetry.list_handlers(@event) == []
    end
  end

  describe "wiring/1" do
    test "detaches the handler when a later setup step fails, so queries cost nothing", %{
      receiver: receiver
    } do
      wiring =
        Keyword.update!(EctoMetrics.wiring(@event), :setup, &(&1 ++ [{__MODULE__, :fail, []}]))

      capture_log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, :undefined} =
                   start_supervised({Exporter, [endpoint: receiver.endpoint] ++ wiring})
        end)

      assert capture_log =~ "telemetry is off"
      assert :telemetry.list_handlers(@event) == []
    end

    test "attaches the handler and exports when the endpoint is set", %{receiver: receiver} do
      start_exporter(receiver)

      assert [_handler] = :telemetry.list_handlers(@event)
      query(%{query_time: native_ms(2)})
      assert %{} = OtlpReceiver.await_metric(receiver, "db.client.operation.duration")
    end
  end

  @doc false
  def fail, do: raise("a later setup step failed")

  defp exported_metrics(receiver) do
    receiver.agent
    |> Agent.get(& &1.history)
    |> Enum.flat_map(& &1.metrics)
    |> Enum.filter(&String.starts_with?(&1.name, ["db.client.", "truedat.db."]))
  end

  defp counted(receiver, outcome) do
    OtlpReceiver.await_metric(receiver, "truedat.db.client.operations")
    Process.sleep(250)

    receiver
    |> exported_metrics()
    |> Enum.filter(&(&1.name == "truedat.db.client.operations"))
    |> Enum.flat_map(& &1.points)
    |> Enum.filter(&(&1.attributes["truedat.outcome"] == outcome))
    |> Enum.map(& &1.value)
    |> Enum.sum()
  end

  defp start_exporter(receiver) do
    start_supervised!(
      {Exporter, [endpoint: receiver.endpoint, export_period: 100] ++ EctoMetrics.wiring(@event)}
    )
  end

  defp query(measurements, metadata \\ %{}) do
    :telemetry.execute(@event, measurements, Map.merge(%{result: {:ok, %{}}}, metadata))
  end

  defp native_ms(ms), do: System.convert_time_unit(ms, :millisecond, :native)
end
