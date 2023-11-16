defmodule TdCore.Utils.CollectionUtils do
  @moduledoc false

  def to_struct(kind, attrs) do
    struct = struct(kind)

    Enum.reduce(Map.to_list(struct), struct, fn {k, _}, acc ->
      case Map.fetch(attrs, Atom.to_string(k)) do
        {:ok, v} -> %{acc | k => v}
        :error -> acc
      end
    end)
  end

  def stringify_keys(map, deep \\ false)

  def stringify_keys(map, deep) when is_struct(map) do
    map
    |> Map.from_struct()
    |> stringify_keys(deep)
  end

  def stringify_keys(%{} = map, deep) do
    map
    |> Enum.into(%{}, fn {k, v} -> {stringify_key(k), maybe_stringfy_children(v, deep)} end)
  end

  def stringify_keys(value, _deep), do: value

  defp maybe_stringfy_children(v, true), do: stringify_keys(v)
  defp maybe_stringfy_children(v, _), do: v

  defp stringify_key(key) when is_atom(key), do: Atom.to_string(key)
  defp stringify_key(key), do: key

  def atomize_keys(%{} = map) do
    map
    |> Enum.into(%{}, fn {k, v} -> {atomize_key(k), v} end)
  end

  defp atomize_key(key) when is_binary(key), do: String.to_atom(key)
  defp atomize_key(key), do: key
end
