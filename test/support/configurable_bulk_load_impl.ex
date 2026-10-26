defmodule ConfigurableBulkLoadImpl do
  @moduledoc """
  A BulkLoadProtocol implementation for tests, whose sheets_to_templates
  answer is configurable through the struct: a sheet whose type has no
  associated template (e.g. a structure type without template) resolves to
  nil, and the generic pipeline must not crash on it.
  """

  defstruct templates: %{}

  defimpl TdCore.XLSX.BulkLoadProtocol do
    def get_opts(_impl),
      do: [required_headers: ["external_id"], extra_headers: ["name"]]

    def bulk_load_item(_impl, item, ctx) do
      if Map.has_key?(ctx.templates, item["_sheet"]) do
        {:created, {item["external_id"], %{}}}
      else
        {:error, {"template_not_found", %{external_id: item["external_id"]}}}
      end
    end

    def on_complete(_impl, _ids), do: nil

    def sheets_to_templates(%{templates: templates} = _impl, _sheets), do: templates
  end
end
