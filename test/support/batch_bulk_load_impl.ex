defmodule BatchBulkLoadImpl do
  @moduledoc false

  defstruct [:mode, :test_pid]

  defimpl TdCore.XLSX.BulkLoadProtocol do
    def get_opts(%{mode: :without_batch}) do
      [required_headers: ["external_id"]]
    end

    def get_opts(impl) do
      [
        required_headers: ["external_id"],
        bulk_load_items: fn items, ctx -> BatchBulkLoadImpl.bulk_load_items(impl, items, ctx) end
      ]
    end

    def bulk_load_item(_impl, item, _ctx), do: {:created, {item["external_id"], %{}}}

    def on_complete(%{test_pid: test_pid}, ids), do: send(test_pid, {:on_complete, ids})
    def sheets_to_templates(_impl, _sheets), do: %{}
  end

  def bulk_load_items(%{mode: :mixed}, items, _ctx) do
    Enum.map(items, fn item ->
      result =
        case item["external_id"] do
          "created" -> {:created, {"created-id", %{}}}
          "updated" -> {:updated, {["updated-id", "extra-id"], %{}}}
          "unchanged" -> {:unchanged, %{}}
          "error" -> {:error, {"invalid_item", %{}}}
        end

      {item, result}
    end)
  end

  def bulk_load_items(%{mode: :wrong_length}, [item | _items], _ctx),
    do: [{item, {:created, {"id", %{}}}}]

  def bulk_load_items(%{mode: :raises}, _items, _ctx), do: raise("batch failed")

  def bulk_load_items(%{mode: :created_then_emit_fails}, items, _ctx) do
    Enum.map(items, &{&1, {:created, {&1["external_id"], %{}}}})
  end
end
