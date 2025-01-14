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
    {:must, term(field, values)}
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

    {:must, nested_query}
  end

  defp build_filter("must_not", values) do
    {:must_not, Enum.map(values, fn {key, term_values} -> term(key, term_values) end)}
  end

  defp build_filter("exists", value) do
    {:must, %{exists: value}}
  end

  defp build_filter(field, value)
       when field in ["updated_at", "inserted_at", "start_date", "end_date", "last_change_at"] do
    {:must, Query.range(field, value)}
  end

  defp build_filter("ids", values), do: {:must, Query.ids(values)}

  defp build_filter(field, values) when is_binary(field) do
    {:must, term(field, values)}
  end

  defp build_filter(key, values, aggs) do
    aggs
    |> Map.get(key, _field = key)
    |> build_filter(values)
  end

  defp term(field, values) do
    Query.term_or_terms(field, values)
  end
end
