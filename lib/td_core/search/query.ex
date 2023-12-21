defmodule TdCore.Search.Query do
  @moduledoc """
  Support for building search queries
  """

  alias TdCore.Search.Filters

  @match_all %{match_all: %{}}
  @match_none %{match_none: %{}}

  def build_query(filters, params, aggs \\ %{})

  def build_query(filters, params, aggs) do
    query = Map.get(params, "query")

    filters =
      filters
      |> List.wrap()
      |> Enum.reduce(%{}, fn
        %{must_not: must_not_filters}, acu ->
          Map.update(acu, :must_not, must_not_filters, fn mn -> mn ++ must_not_filters end)

        f, acu ->
          Map.update(acu, :must, [f], fn filters -> [f | filters] end)
      end)

    params
    |> Map.take(["must", "query", "without", "with", "must_not", "filters"])
    |> Enum.reduce(filters, &reduce_query(&1, &2, aggs))
    |> maybe_optimize()
    |> add_query_should(query)
    |> bool_query()
  end

  def build_permission_filters(:none), do: @match_none
  def build_permission_filters(:all), do: @match_all
  def build_permission_filters(domain_ids), do: term_or_terms("domain_ids", domain_ids)

  def should(%{} = query, clause), do: put_clause(query, :should, clause)
  def must(%{} = query, clause), do: put_clause(query, :must, clause)
  def must_not(%{} = query, clause), do: put_clause(query, :must_not, clause)

  def put_clause(%{} = query, key, clause) do
    Map.update(query, key, [clause], &[clause | &1])
  end

  defp reduce_query({"filters", %{} = filters}, %{} = acc, aggs)
       when map_size(filters) > 0 do
    Filters.build_filters(filters, aggs, acc)
  end

  defp reduce_query({"filters", %{}}, %{} = acc, _) do
    acc
  end

  defp reduce_query({"must", %{} = must}, %{} = acc, aggs)
       when map_size(must) > 0 do
    Filters.build_filters(must, aggs, acc)
  end

  defp reduce_query({"must", %{}}, %{} = acc, _) do
    acc
  end

  defp reduce_query({"must_not", %{} = must}, %{} = acc, aggs)
       when map_size(must) > 0 do
    Filters.build_filters(must, aggs, acc)
  end

  defp reduce_query({"must_not", %{}}, %{} = acc, _) do
    acc
  end

  defp reduce_query({"query", query}, acc, _) do
    must = %{simple_query_string: %{query: maybe_wildcard(query)}}
    Map.update(acc, :must, must, &[must | List.wrap(&1)])
  end

  defp reduce_query({"without", fields}, acc, _) do
    fields
    |> List.wrap()
    |> Enum.reduce(acc, fn field, acc ->
      must_not = exists(field)
      Map.update(acc, :must_not, must_not, &[must_not | List.wrap(&1)])
    end)
  end

  defp reduce_query({"with", fields}, acc, _) do
    fields
    |> List.wrap()
    |> Enum.reduce(acc, fn field, acc ->
      filter = exists(field)
      Map.update(acc, :must, filter, &[filter | List.wrap(&1)])
    end)
  end

  defp add_query_should(filters, nil), do: filters

  defp add_query_should(filters, query) do
    should = [
      %{
        multi_match: %{
          query: maybe_wildcard(query),
          type: "best_fields",
          operator: "and"
        }
      }
    ]

    Map.put(filters, :should, should)
  end

  defp maybe_optimize(%{must: _} = bool) do
    Map.update!(bool, :must, &optimize/1)
  end

  defp maybe_optimize(%{} = bool), do: bool

  defp optimize(filters) do
    filters =
      filters
      |> List.wrap()
      |> Enum.uniq()

    case filters do
      # match_all is redundant if other filters are present
      filters when length(filters) > 1 -> Enum.reject(filters, &(&1 == @match_all))
      _ -> filters
    end
  end

  def term_or_terms(field, value_or_values) do
    case List.wrap(value_or_values) do
      [value] -> %{term: %{field => value}}
      values -> %{terms: %{field => Enum.sort(values)}}
    end
  end

  def range(field, value) do
    %{range: %{field => value}}
  end

  def exists(field) when is_binary(field) do
    %{exists: %{field: field}}
  end

  def maybe_wildcard(nil), do: nil

  def maybe_wildcard(query) when is_binary(query) do
    case String.last(query) do
      "\"" -> query
      ")" -> query
      " " -> query
      _ -> "#{query}*"
    end
  end

  def bool_query(%{} = clauses) do
    bool =
      clauses
      |> Map.take([:filter, :must, :should, :must_not, :minimum_should_match, :boost])
      |> Map.new(fn
        {key, [value]} when key in [:filter, :must, :must_not, :should] -> {key, value}
        {key, value} -> {key, value}
      end)

    %{bool: bool}
  end
end
