defmodule TdCore.Telemetry.ExporterTest do
  # The exporter keeps a global ETS table and a persistent_term key, so these
  # tests cannot run concurrently.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias TdCore.Telemetry.Exporter
  alias TdCore.TestSupport.OtlpReceiver

  setup do
    {:ok, receiver: OtlpReceiver.start()}
  end

  describe "with the endpoint unset" do
    test "starts nothing, opens no socket, logs nothing and fails nothing" do
      sockets_before = tcp_sockets()

      log =
        capture_log([level: :debug], fn ->
          assert {:ok, :undefined} = start_supervised({Exporter, metrics: [metric()]})
          :telemetry.execute([:td_core, :test, :event], %{count: 1}, %{})
          Process.sleep(150)
        end)

      assert log == ""
      assert Process.whereis(:otel_metric_exporter) == nil
      assert :telemetry.list_handlers([:td_core, :test, :event]) == []
      assert tcp_sockets() == sockets_before
    end

    test "an empty endpoint counts as unset" do
      assert {:ok, :undefined} = start_supervised({Exporter, metrics: [metric()], endpoint: ""})
      assert Process.whereis(:otel_metric_exporter) == nil
    end
  end

  describe "telemetry must never take a service down (D2)" do
    test "an unsupported protocol leaves the service running with telemetry off and a warning",
         %{receiver: receiver} do
      log =
        capture_log(fn ->
          assert {:ok, :undefined} =
                   start_supervised({Exporter, options(receiver, protocol: "grpc")})

          :telemetry.execute([:td_core, :test, :event], %{count: 1}, %{})
        end)

      assert log =~ "[warning]"
      assert log =~ "telemetry is off"
      assert log =~ "OTEL_EXPORTER_OTLP_PROTOCOL"
      assert log =~ "grpc"
      assert Process.whereis(:otel_metric_exporter) == nil
      OtlpReceiver.refute_export(receiver, 200)
    end

    test "an unsupported metrics-specific protocol is refused the same way", %{
      receiver: receiver
    } do
      log =
        capture_log(fn ->
          assert {:ok, :undefined} =
                   start_supervised({Exporter, options(receiver, metrics_protocol: "http/json")})
        end)

      assert log =~ "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"
      assert Process.whereis(:otel_metric_exporter) == nil
    end

    test "an exporter that fails to start leaves the service running with telemetry off and a warning",
         %{receiver: receiver} do
      log =
        capture_log(fn ->
          assert {:ok, :undefined} =
                   start_supervised({Exporter, options(receiver, export_period: -1)})
        end)

      assert log =~ "[warning]"
      assert log =~ "telemetry is off"
      assert Process.whereis(:otel_metric_exporter) == nil
      assert :telemetry.list_handlers([:td_core, :test, :event]) == []
    end

    test "a setup step that raises leaves no exporter running", %{receiver: receiver} do
      log =
        capture_log(fn ->
          assert {:ok, :undefined} =
                   start_supervised(
                     {Exporter, options(receiver, setup: [{__MODULE__, :boom, []}])}
                   )
        end)

      assert log =~ "[warning]"
      assert log =~ "telemetry is off"
      assert log =~ "setup exploded"
      assert Process.whereis(:otel_metric_exporter) == nil
      assert :telemetry.list_handlers([:td_core, :test, :event]) == []
    end

    test "a setup step that fails undoes the steps that already ran", %{receiver: receiver} do
      on_exit(fn -> __MODULE__.detach_probe() end)

      capture_log(fn ->
        assert {:ok, :undefined} =
                 start_supervised(
                   {Exporter,
                    options(receiver,
                      setup: [{__MODULE__, :attach_probe, []}, {__MODULE__, :boom, []}],
                      teardown: [{__MODULE__, :detach_probe, []}]
                    )}
                 )
      end)

      assert :telemetry.list_handlers([:td_core, :test, :probe]) == []
    end

    test "an exporter that crashes while starting, because one already runs, is contained", %{
      receiver: receiver
    } do
      start_supervised!({Exporter, options(receiver)})

      capture_log(fn ->
        assert {:ok, :undefined} =
                 start_supervised(
                   Supervisor.child_spec({Exporter, options(receiver)}, id: :second)
                 )
      end)

      :telemetry.execute([:td_core, :test, :event], %{count: 1}, %{})

      assert %{points: [%{value: 1}]} =
               OtlpReceiver.await_metric(receiver, "td_core.test.event.count")
    end
  end

  describe "bad options and endpoints" do
    test "a missing metrics option leaves the service running with telemetry off", %{
      receiver: receiver
    } do
      log =
        capture_log(fn ->
          assert {:ok, :undefined} = start_supervised({Exporter, endpoint: receiver.endpoint})
        end)

      assert log =~ "[warning]"
      assert log =~ "telemetry is off"
      assert Process.whereis(:otel_metric_exporter) == nil
    end

    test "a metrics option that is not a list leaves the service running with telemetry off", %{
      receiver: receiver
    } do
      log =
        capture_log(fn ->
          assert {:ok, :undefined} =
                   start_supervised({Exporter, options(receiver, metrics: :not_a_list)})
        end)

      assert log =~ "telemetry is off"
    end

    test "a trailing slash on the endpoint does not change where the export goes", %{
      receiver: receiver
    } do
      start_supervised!({Exporter, options(receiver, endpoint: receiver.endpoint <> "/")})
      :telemetry.execute([:td_core, :test, :event], %{count: 1}, %{})

      assert %{path: "/v1/metrics"} = OtlpReceiver.await_export(receiver)
    end

    test "nothing exportable is said once, naming what was dropped", %{receiver: receiver} do
      log =
        capture_log(fn ->
          assert {:ok, :undefined} =
                   start_supervised({Exporter, options(receiver, metrics: [summary()])})
        end)

      assert log =~ "[warning]"
      assert log =~ "td_core.test.event.duration"
      assert log =~ "telemetry is off"
    end

    test "a dropped summary is named in a warning while the rest still export", %{
      receiver: receiver
    } do
      log =
        capture_log(fn ->
          start_supervised!({Exporter, options(receiver, metrics: [summary(), metric()])})
          :telemetry.execute([:td_core, :test, :event], %{count: 1}, %{})
          assert %{} = OtlpReceiver.await_metric(receiver, "td_core.test.event.count")
        end)

      assert log =~ "[warning]"
      assert log =~ "td_core.test.event.duration"
      refute log =~ "telemetry is off"
    end
  end

  describe "summary metrics (EXP-4)" do
    test "a summary in the list is dropped and every supported metric still exports", %{
      receiver: receiver
    } do
      log =
        capture_log(fn ->
          start_supervised!({Exporter, options(receiver, metrics: [summary(), metric()])})

          :telemetry.execute([:td_core, :test, :event], %{count: 1, duration: 5}, %{})

          assert %{type: :sum} = OtlpReceiver.await_metric(receiver, "td_core.test.event.count")
        end)

      refute log =~ "telemetry is off"
    end

    test "a list of only summaries starts nothing", %{receiver: receiver} do
      capture_log(fn ->
        assert {:ok, :undefined} =
                 start_supervised({Exporter, options(receiver, metrics: [summary()])})
      end)

      assert Process.whereis(:otel_metric_exporter) == nil
    end
  end

  describe "with the endpoint set" do
    test "a recorded metric reaches the receiver with the five resource attributes", %{
      receiver: receiver
    } do
      start_supervised!({Exporter, options(receiver)})

      :telemetry.execute([:td_core, :test, :event], %{count: 1}, %{})

      assert %{
               type: :sum,
               points: [%{value: 1}],
               resource: %{
                 "service.name" => "td-ai",
                 "service.namespace" => "truedat",
                 "service.version" => "8.14.0",
                 "service.instance.id" => "td-ai-7d9f-abc12",
                 "deployment.environment.name" => "test"
               }
             } = OtlpReceiver.await_metric(receiver, "td_core.test.event.count")
    end

    test "the export is gzip protobuf", %{receiver: receiver} do
      start_supervised!({Exporter, options(receiver)})
      :telemetry.execute([:td_core, :test, :event], %{count: 1}, %{})

      assert %{path: "/v1/metrics", content_type: "application/x-protobuf", compression: :gzip} =
               OtlpReceiver.await_export(receiver)
    end
  end

  @doc false
  def boom, do: raise("setup exploded")

  @doc false
  def attach_probe do
    :telemetry.attach(
      "exporter-test-probe",
      [:td_core, :test, :probe],
      fn _, _, _, _ -> :ok end,
      nil
    )
  end

  @doc false
  def detach_probe, do: :telemetry.detach("exporter-test-probe")

  defp options(receiver, overrides \\ []) do
    Keyword.merge(
      [metrics: [metric()], endpoint: receiver.endpoint, export_period: 100],
      overrides
    )
  end

  defp tcp_sockets, do: Enum.count(Port.list(), &(Port.info(&1, :name) == {:name, ~c"tcp_inet"}))

  defp metric, do: Telemetry.Metrics.counter("td_core.test.event.count")

  defp summary, do: Telemetry.Metrics.summary("td_core.test.event.duration")
end
