defmodule TdCore.Search.Query do
  @moduledoc """
  Support for building search queries
  """

  alias TdCore.Search.Filters

  @match_all %{match_all: %{}}
  @match_none %{match_none: %{}}

  def build_query(filters, params, query \\ %{}) do
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
    |> Enum.reduce(filters, &reduce_query(&1, &2, query))
    |> maybe_optimize()
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

  defp reduce_query({"filters", %{} = filters}, %{} = acc, query)
       when map_size(filters) > 0 do
    aggs = Map.get(query, :aggs, %{})
    Filters.build_filters(filters, aggs, acc)
  end

  defp reduce_query({"filters", %{}}, %{} = acc, _) do
    acc
  end

  defp reduce_query({"must", %{} = must}, %{} = acc, query)
       when map_size(must) > 0 do
    aggs = Map.get(query, :aggs, %{})
    Filters.build_filters(must, aggs, acc)
  end

  defp reduce_query({"must", %{}}, %{} = acc, _) do
    acc
  end

  defp reduce_query({"must_not", %{} = fields}, %{} = acc, query)
       when map_size(fields) > 0 do
    aggs = Map.get(query, :aggs, %{})
    Filters.build_filters(%{"must_not" => fields}, aggs, acc)
  end

  defp reduce_query({"must_not", %{}}, %{} = acc, _) do
    acc
  end

  defp reduce_query({"query", query}, acc, %{fields: [_ | _] = fields}) do
    must = %{
      multi_match: %{
        query: query,
        type: "phrase_prefix",
        fields: fields,
        lenient: true,
        slop: 2
      }
    }

    Map.update(acc, :must, must, &[must | List.wrap(&1)])
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

  def ids(values), do: %{ids: %{"values" => values}}

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
