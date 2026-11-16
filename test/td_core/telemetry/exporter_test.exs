defmodule TdCore.Telemetry.ExporterTest do
  # The exporter keeps a global ETS table and a persistent_term key, and some tests set
  # environment variables, so these tests cannot run concurrently.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias TdCore.Telemetry.Exporter
  alias TdCore.TestSupport.OtlpReceiver

  @event [:td_core, :test, :event]

  setup do
    {:ok, receiver: OtlpReceiver.start()}
  end

  describe "with the endpoint unset" do
    test "starts nothing and logs nothing" do
      log =
        capture_log(fn ->
          assert {:ok, :undefined} = start_supervised({Exporter, metrics: [counter()]})
        end)

      assert log == ""
      assert Process.whereis(:otel_metric_exporter) == nil
      assert :telemetry.list_handlers(@event) == []
    end

    test "an empty endpoint counts as unset" do
      assert {:ok, :undefined} = start_supervised({Exporter, metrics: [counter()], endpoint: ""})
      assert Process.whereis(:otel_metric_exporter) == nil
    end
  end

  describe "with the endpoint set" do
    test "exports gzip protobuf to /v1/metrics, even with a trailing slash on the endpoint", %{
      receiver: receiver
    } do
      start_supervised!({Exporter, options(receiver, endpoint: receiver.endpoint <> "/")})
      :telemetry.execute(@event, %{count: 1}, %{})

      assert %{path: "/v1/metrics", content_type: "application/x-protobuf", compression: :gzip} =
               OtlpReceiver.await_export(receiver)
    end

    test "adds service.instance.id from POD_NAME to the resource", %{receiver: receiver} do
      with_env(%{"POD_NAME" => "td-ai-7d9f-abc12", "OTEL_SERVICE_NAME" => "td-ai"}, fn ->
        start_supervised!({Exporter, options(receiver)})
        :telemetry.execute(@event, %{count: 1}, %{})

        assert %{
                 resource: %{
                   "service.instance.id" => "td-ai-7d9f-abc12",
                   "service.name" => "td-ai"
                 }
               } =
                 OtlpReceiver.await_metric(receiver, "td_core.test.event.count")
      end)
    end

    test "runs the attach steps once the exporter is up", %{receiver: receiver} do
      start_supervised!({Exporter, options(receiver, attach: [{__MODULE__, :attach_probe, []}])})

      assert [_probe] = :telemetry.list_handlers([:td_core, :test, :probe])
    after
      :telemetry.detach("exporter-test-probe")
    end
  end

  describe "telemetry never takes the service down" do
    test "an unsupported protocol switches telemetry off with a warning", %{receiver: receiver} do
      for var <- ["OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"] do
        with_env(%{var => "grpc"}, fn ->
          assert_off(options(receiver), "#{var} must be http/protobuf")
        end)
      end
    end

    test "a summary metric switches telemetry off with a warning", %{receiver: receiver} do
      summary = Telemetry.Metrics.summary("td_core.test.event.duration")
      assert_off(options(receiver, metrics: [counter(), summary]), "metrics")
    end

    test "a missing metrics option switches telemetry off", %{receiver: receiver} do
      assert_off(Keyword.delete(options(receiver), :metrics), "metrics")
    end

    test "an attach step that raises stops the exporter", %{receiver: receiver} do
      assert_off(options(receiver, attach: [{__MODULE__, :boom, []}]), "attach exploded")
    end

    test "a second exporter is contained", %{receiver: receiver} do
      start_supervised!({Exporter, options(receiver)})
      {:ok, sup} = Supervisor.start_link([], strategy: :one_for_one)

      capture_log(fn ->
        assert {:ok, :undefined} = Supervisor.start_child(sup, {Exporter, options(receiver)})
      end)

      assert Process.alive?(sup)
    end
  end

  @doc false
  def boom, do: raise("attach exploded")

  @doc false
  def noop(_, _, _, _), do: :ok

  @doc false
  def attach_probe do
    :telemetry.attach(
      "exporter-test-probe",
      [:td_core, :test, :probe],
      &__MODULE__.noop/4,
      nil
    )
  end

  defp assert_off(options, reason) do
    log =
      capture_log(fn -> assert {:ok, :undefined} = start_supervised({Exporter, options}) end)

    assert log =~ "telemetry is off"
    assert log =~ reason
    refute log =~ "127.0.0.1"
    assert Process.whereis(:otel_metric_exporter) == nil
  end

  defp with_env(vars, fun) do
    previous = Map.new(vars, fn {k, _} -> {k, System.get_env(k)} end)
    System.put_env(vars)

    try do
      fun.()
    after
      for {k, v} <- previous, do: if(v, do: System.put_env(k, v), else: System.delete_env(k))
    end
  end

  defp options(receiver, overrides \\ []) do
    Keyword.merge(
      [metrics: [counter()], endpoint: receiver.endpoint, export_period: 100],
      overrides
    )
  end

  defp counter, do: Telemetry.Metrics.counter("td_core.test.event.count")
end
