defmodule MockBulkLoadImpl do
  @moduledoc """
  A mock implementation of the TdCore.XLSX.BulkLoadProtocol for testing purposes.
  """

  defstruct []

  defimpl TdCore.XLSX.BulkLoadProtocol do
    def get_opts(_),
      do: [
        required_headers: ["external_id"],
        extra_headers: ["name"],
        discarded_headers: ["link_to_structure"]
      ]

    def bulk_load_item(_, _, _), do: {:created, {"id", "details"}}
    def on_complete(_, _), do: nil
    def sheets_to_templates(_, _), do: %{}
  end
end
