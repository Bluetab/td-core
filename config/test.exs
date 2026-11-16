import Config

# Telemetry is off in tests whatever OTEL_* variables the shell holds. A test that needs it
# passes the stub receiver's endpoint itself.
config :td_core, TdCore.Telemetry.Exporter, endpoint: nil

config :td_core, :otlp_receiver, true
