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

  def stringify_keys(%DateTime{} = datetime, _), do: datetime
  def stringify_keys(%Date{} = date, _), do: date

  def stringify_keys(map, deep) when is_struct(map) do
    map
    |> Map.from_struct()
    |> Map.drop([:__meta__])
    |> stringify_keys(deep)
  end

  def stringify_keys(%{} = map, deep) do
    map
    |> Enum.into(%{}, fn {k, v} -> {stringify_key(k), maybe_stringfy_children(v, deep)} end)
  end

  def stringify_keys([_ | _] = list, deep) do
    Enum.map(list, &stringify_keys(&1, deep))
  end

  def stringify_keys(value, _deep), do: value

  defp maybe_stringfy_children(v, true), do: stringify_keys(v, true)
  defp maybe_stringfy_children(v, _), do: v

  defp stringify_key(key) when is_atom(key), do: Atom.to_string(key)
  defp stringify_key(key), do: key

  def atomize_keys(element, deep \\ false)

  def atomize_keys(%{} = map, deep) do
    Enum.into(map, %{}, fn {k, v} -> {atomize_key(k), maybe_atomize_children(v, deep)} end)
  end

  def atomize_keys([_ | _] = list, deep) do
    Enum.map(list, &atomize_keys(&1, deep))
  end

  def atomize_keys(element, _deep), do: element

  defp atomize_key(key) when is_binary(key), do: String.to_atom(key)
  defp atomize_key(key), do: key

  defp maybe_atomize_children(v, true), do: atomize_keys(v, true)
  defp maybe_atomize_children(v, _), do: v

  def merge_common(%{} = map1, %{} = map2) do
    keys1 = Map.keys(map1)
    keys2 = Map.keys(map2)

    keys = MapSet.intersection(MapSet.new(keys1), MapSet.new(keys2))
    map1 = Map.take(map1, MapSet.to_list(keys))
    map2 = Map.take(map2, MapSet.to_list(keys))

    Map.merge(map1, map2, fn _k, v1, v2 -> v1 ++ v2 end)
  end
end
