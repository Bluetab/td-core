defmodule FailingPreflightBulkLoadImpl do
  @moduledoc """
  A BulkLoadProtocol implementation whose preflight rejects every workbook.
  """

  defstruct []

  defimpl TdCore.XLSX.BulkLoadProtocol do
    def get_opts(_),
      do: [
        required_headers: ["external_id"],
        preflight: fn _sheets, _ctx -> {:error, :invalid_format} end
      ]

    def bulk_load_item(_, _, _), do: raise("bulk_load_item must not run")
    def on_complete(_, _), do: nil
    def sheets_to_templates(_, _), do: %{}
  end
end
