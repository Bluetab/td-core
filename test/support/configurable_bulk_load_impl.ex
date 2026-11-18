defmodule ConfigurableBulkLoadImpl do
  @moduledoc """
  A BulkLoadProtocol implementation for tests, whose sheets_to_templates
  answer is configurable through the struct: a sheet whose type has no
  associated template (e.g. a structure type without template) resolves to
  nil, and the generic pipeline must not crash on it.
  """

  defstruct templates: %{}, opts: [], observer: nil

  defimpl TdCore.XLSX.BulkLoadProtocol do
    def get_opts(%{opts: opts} = impl) do
      notify(impl, :get_opts)
      Keyword.merge([required_headers: ["external_id"], extra_headers: ["name"]], opts)
    end

    def bulk_load_item(impl, item, ctx) do
      notify(impl, {:bulk_load_item, item, ctx})

      if Map.has_key?(ctx.templates, item["_sheet"]) do
        {:created, {item["external_id"], %{}}}
      else
        {:error, {"template_not_found", %{external_id: item["external_id"]}}}
      end
    end

    def on_complete(impl, ids), do: notify(impl, {:on_complete, ids})

    def sheets_to_templates(%{templates: templates} = impl, sheets) do
      notify(impl, {:sheets_to_templates, sheets})
      templates
    end

    defp notify(%{observer: nil}, _event), do: nil
    defp notify(%{observer: pid}, event), do: send(pid, event)
  end
end
