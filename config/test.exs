import Config

config :td_core, TdCore.Telemetry.Exporter,
  endpoint: nil,
  protocol: "http/protobuf",
  metrics_protocol: nil,
  resource: %{
    "service.name" => "td-ai",
    "service.namespace" => "truedat",
    "service.version" => "8.14.0",
    "service.instance.id" => "td-ai-7d9f-abc12",
    "deployment.environment.name" => "test"
  }
