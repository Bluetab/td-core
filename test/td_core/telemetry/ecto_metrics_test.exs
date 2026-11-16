defmodule TdCore.Telemetry.EctoMetricsTest do
  # The exporter keeps a global ETS table and a persistent_term key, so these tests cannot
  # run concurrently.
  use ExUnit.Case, async: false

  alias TdCore.Telemetry.EctoMetrics
  alias TdCore.Telemetry.Exporter
  alias TdCore.TestSupport.OtlpReceiver

  defmodule FakeRepo do
    def config, do: [telemetry_prefix: [:td_test, :custom_prefix]]
  end

  @event [:td_test, :custom_prefix, :query]
  @bounds [1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0] ++
            [10_000.0, 30_000.0, 60_000.0]

  setup do
    on_exit(fn -> EctoMetrics.detach(@event) end)
    {:ok, receiver: OtlpReceiver.start()}
  end

  test "wiring/1 follows the repo's telemetry_prefix" do
    assert [attach: [{EctoMetrics, :attach, [@event]}]] =
             Keyword.take(EctoMetrics.wiring(FakeRepo), [:attach])
  end

  test "with the endpoint unset, the handler is never attached" do
    assert {:ok, :undefined} = start_supervised({Exporter, EctoMetrics.wiring(FakeRepo)})
    assert :telemetry.list_handlers(@event) == []
  end

  test "exports both durations as millisecond distributions", %{receiver: receiver} do
    start_exporter(receiver)
    query(%{query_time: native_us(12_000), queue_time: native_us(800)})

    assert %{type: :histogram, unit: "ms", points: [%{count: 1, bounds: @bounds, sum: 12.0}]} =
             OtlpReceiver.await_metric(receiver, "db.client.operation.duration")

    # 0.8 ms is a float when it reaches the exporter, so it lands in the first bucket.
    assert %{points: [%{count: 1, bucket_counts: [1 | _]}]} =
             OtlpReceiver.await_metric(receiver, "db.client.connection.wait_time")
  end

  test "counts queries by outcome", %{receiver: receiver} do
    start_exporter(receiver)

    query(%{query_time: 1}, %{result: {:ok, %{}}})
    query(%{query_time: 1}, %{result: {:ok, %{}}})
    query(%{query_time: 1}, %{result: {:error, %{}}})

    assert %{type: :sum, points: points} =
             OtlpReceiver.await_metric(receiver, "truedat.db.client.operations")

    assert points |> Enum.map(&{&1.attributes["truedat.outcome"], &1.value}) |> Enum.sort() ==
             [{"failure", 1}, {"success", 2}]
  end

  test "a missing queue_time gives no wait sample and the handler stays attached", %{
    receiver: receiver
  } do
    start_exporter(receiver)

    query(%{query_time: native_us(4_000)})
    query(%{query_time: native_us(9_000), queue_time: native_us(2_000)})

    assert %{points: [%{count: 2}]} =
             OtlpReceiver.await_metric(receiver, "db.client.operation.duration")

    assert %{points: [%{count: 1, sum: 2.0}]} =
             OtlpReceiver.await_metric(receiver, "db.client.connection.wait_time")

    assert [_handler] = :telemetry.list_handlers(@event)
  end

  test "transaction control is left out of the duration and the counter, not the wait", %{
    receiver: receiver
  } do
    start_exporter(receiver)

    query(%{query_time: 1, queue_time: native_us(3_000)}, %{query: "begin"})
    query(%{query_time: native_us(4_000)}, %{query: "SELECT 1"})
    query(%{query_time: 1}, %{query: "commit"})
    query(%{query_time: 1}, %{query: "rollback"})

    assert %{points: [%{value: 1}]} =
             OtlpReceiver.await_metric(receiver, "truedat.db.client.operations")

    assert %{points: [%{count: 1, sum: 4.0}]} =
             OtlpReceiver.await_metric(receiver, "db.client.operation.duration")

    assert %{points: [%{count: 1, sum: 3.0}]} =
             OtlpReceiver.await_metric(receiver, "db.client.connection.wait_time")
  end

  test "no SQL text, parameter or table reaches the export, and no label but the outcome", %{
    receiver: receiver
  } do
    start_exporter(receiver)

    query(%{query_time: 7, queue_time: 1}, %{
      query: "SELECT email FROM users WHERE name = $1",
      params: ["jane.doe@example.com"],
      cast_params: ["jane.doe@example.com"],
      source: "users"
    })

    OtlpReceiver.await_metric(receiver, "truedat.db.client.operations")
    OtlpReceiver.await_metric(receiver, "db.client.connection.wait_time")

    exported =
      receiver.agent |> Agent.get(& &1.history) |> Enum.flat_map(& &1.metrics)

    for metric <- exported, point <- metric.points do
      assert Map.keys(point.attributes) -- ["truedat.outcome"] == []
    end

    dump = inspect(exported, limit: :infinity, printable_limit: :infinity)

    for secret <- ["SELECT", "$1", "jane.doe", "users"] do
      refute dump =~ secret
    end
  end

  defp start_exporter(receiver) do
    start_supervised!(
      {Exporter,
       [endpoint: receiver.endpoint, export_period: 100] ++ EctoMetrics.wiring(FakeRepo)}
    )
  end

  defp query(measurements, metadata \\ %{}) do
    :telemetry.execute(@event, measurements, Map.merge(%{result: {:ok, %{}}}, metadata))
  end

  defp native_us(us), do: System.convert_time_unit(us, :microsecond, :native)
end
