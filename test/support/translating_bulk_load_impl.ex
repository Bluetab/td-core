defmodule TranslatingBulkLoadImpl do
  @moduledoc """
  A BulkLoadProtocol implementation for tests with a `translate_fn`.

  Technical headers `external_id` (required) and `name` (extra) are translated
  to the synthetic labels `ID` and `Name`; `status` (discarded) to `Status`.
  Every item received by `bulk_load_item/3` is sent to the calling process.
  """

  defstruct []

  def label("external_id"), do: "ID"
  def label("name"), do: "Name"
  def label("status"), do: "Status"
  def label(_), do: nil

  defimpl TdCore.XLSX.BulkLoadProtocol do
    def get_opts(_impl),
      do: [
        required_headers: ["external_id"],
        extra_headers: ["name"],
        discarded_headers: ["status"],
        translate_fn: fn header, _lang -> TranslatingBulkLoadImpl.label(header) end
      ]

    def bulk_load_item(_impl, item, _ctx) do
      send(self(), {:item, item})
      {:created, {item["external_id"], %{}}}
    end

    def on_complete(_impl, _ids), do: nil
    def sheets_to_templates(_impl, _sheets), do: %{}
  end
end
