defmodule TdCore.Search.Filters do
  @moduledoc """
  Support for building filtering search queries
  """

  alias TdCache.TaxonomyCache
  alias TdCore.Search.Query

  def build_filters(filters, aggregations, acc) do
    filters
    |> Enum.map(&build_filter(&1, aggregations))
    |> Enum.reduce(acc, &merge/2)
  end

  defp merge({key, [value]}, %{} = acc) do
    Map.update(acc, key, [value], fn existing ->
      [value | List.wrap(existing)]
    end)
  end

  defp merge({key, value}, %{} = acc) do
    Map.update(acc, key, value, fn existing ->
      [value | List.wrap(existing)]
    end)
  end

  defp build_filter({"taxonomy" = key, values}, aggs) do
    values = TaxonomyCache.reachable_domain_ids(values)
    build_filter(key, values, aggs)
  end

  defp build_filter({key, values}, aggs) do
    build_filter(key, values, aggs)
  end

  defp build_filter(%{terms: %{field: field}}, values) do
    {:filter, term(field, values)}
  end

  defp build_filter(
         %{
           nested: %{path: path},
           aggs: %{distinct_search: %{terms: %{field: field}}}
         },
         values
       ) do
    nested_query = %{
      nested: %{
        path: path,
        query: term(field, values)
      }
    }

    {:filter, nested_query}
  end

  defp build_filter(
         %{nested: %{path: path}, meta: %{nested_key: [_field | nested_field]}, aggs: aggs},
         values
       ) do
    field = get_in(aggs, nested_field ++ [:terms, :field])
    nested_query = %{nested: %{path: path, query: term(field, values)}}

    {:filter, nested_query}
  end

  defp build_filter("must_not", values) do
    {:must_not, Enum.map(values, fn {key, term_values} -> term(key, term_values) end)}
  end

  defp build_filter("exists", value) do
    {:filter, %{exists: value}}
  end

  defp build_filter(field, value)
       when field in ["updated_at", "inserted_at", "start_date", "end_date", "last_change_at"] do
    {:filter, Query.range(field, value)}
  end

  defp build_filter("ids", values), do: {:filter, Query.ids(values)}

  defp build_filter(field, values) when is_binary(field) do
    {:filter, term(field, values)}
  end

  defp build_filter(key, values, aggs) do
    case String.split(key, ".") do
      [simple_key] -> filter_for_simple_field(simple_key, values, aggs)
      [_ | _] = split_key -> filter_for_nested_field(split_key, key, values, aggs)
    end
  end

  defp filter_for_simple_field(key, values, aggs) do
    aggs
    |> Map.get(key, _field = key)
    |> build_filter(values)
  end

  defp filter_for_nested_field([parent_field | _] = split_key, key, values, aggs) do
    aggs
    |> Map.get(parent_field)
    |> then(fn
      %{nested: _, meta: meta} = agg ->
        meta = Map.put(meta, :nested_key, split_key)

        agg
        |> Map.put(:meta, meta)
        |> build_filter(values)

      _other ->
        aggs
        |> Map.get(key, _field = key)
        |> build_filter(values)
    end)
  end

  defp term(field, values) do
    Query.term_or_terms(field, values)
  end
end
