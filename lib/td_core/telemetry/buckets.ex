defmodule TdCore.Telemetry.Buckets do
  @moduledoc """
  Explicit histogram bucket bounds, in milliseconds. The exporter's default
  bounds (0 to 10000) suit none of our durations (EXP-5).
  """

  @request_like [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000]

  @doc "Bounds for work that takes milliseconds to seconds: requests and queries."
  @spec request_like() :: [pos_integer()]
  def request_like, do: @request_like
end
