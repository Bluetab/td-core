defmodule TdCore.Search.Query do
  @moduledoc """
  Support for building search queries
  """

  alias TdCore.Search.Filters

  @match_all %{match_all: %{}}
  @match_none %{match_none: %{}}

  def build_query(filters, params, opts \\ []) do
    filters =
      filters
      |> List.wrap()
      |> Enum.reduce(%{}, fn
        filters = %{}, acu when map_size(filters) == 0 ->
          acu

        %{must_not: must_not_filters}, acu ->
          Map.update(acu, :must_not, must_not_filters, fn mn -> mn ++ must_not_filters end)

        f, acu ->
          Map.update(acu, :filter, [f], fn filters -> [f | filters] end)
      end)

    params
    |> Map.take([
      "must",
      "query",
      "without",
      "with",
      "must_not",
      "filters",
      "minimum_should_match"
    ])
    |> Enum.reduce(filters, &reduce_query(&1, &2, opts))
    |> maybe_optimize()
    |> dependent_statements()
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

  defp reduce_query({"filters", %{} = filters}, %{} = acc, opts)
       when map_size(filters) > 0 do
    aggs = Keyword.get(opts, :aggs, %{})
    Filters.build_filters(filters, aggs, acc)
  end

  defp reduce_query({"filters", %{}}, %{} = acc, _) do
    acc
  end

  defp reduce_query({"must", %{} = must}, %{} = acc, opts)
       when map_size(must) > 0 do
    aggs = Keyword.get(opts, :aggs, %{})
    Filters.build_filters(must, aggs, acc)
  end

  defp reduce_query({"must", %{}}, %{} = acc, _) do
    acc
  end

  defp reduce_query({"must_not", %{} = fields}, %{} = acc, opts)
       when map_size(fields) > 0 do
    aggs = Keyword.get(opts, :aggs, %{})
    Filters.build_filters(%{"must_not" => fields}, aggs, acc)
  end

  defp reduce_query({"must_not", %{}}, %{} = acc, _) do
    acc
  end

  defp reduce_query({"query", query}, acc, opts) when is_binary(query) do
    query = String.trim(query)

    opts
    |> Keyword.get(:clauses, %{})
    |> then(fn
      clauses when map_size(clauses) == 0 ->
        must_clauses = [%{simple_query_string: %{query: maybe_wildcard(query)}}]
        Map.update(acc, :must, must_clauses, &(must_clauses ++ List.wrap(&1)))

      clauses ->
        Enum.reduce(clauses, acc, fn
          {:must, clauses}, acc ->
            must_clauses = fetch_clauses(query, clauses)
            Map.update(acc, :must, must_clauses, &(must_clauses ++ List.wrap(&1)))

          {:should, clauses}, acc ->
            should_clauses = fetch_clauses(query, clauses)
            Map.update(acc, :should, should_clauses, &(should_clauses ++ List.wrap(&1)))
        end)
    end)
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
      Map.update(acc, :filter, filter, &[filter | List.wrap(&1)])
    end)
  end

  defp reduce_query({"minimum_should_match", value}, acc, _opts) do
    Map.put(acc, :minimum_should_match, value)
  end

  defp maybe_optimize(%{filter: _} = bool) do
    Map.new(bool, fn
      {:filter, filters} -> {:filter, optimize(filters)}
      other -> other
    end)
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

  defp maybe_wildcard(query) when is_binary(query) do
    case String.last(query) do
      "\"" -> query
      ")" -> query
      _ -> "\"#{query}\""
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

  defp fetch_clauses(query, []) do
    [%{simple_query_string: %{query: maybe_wildcard(query)}}]
  end

  defp fetch_clauses(query, clauses) when is_list(clauses),
    do: Enum.map(clauses, &fetch_clauses(query, &1))

  defp fetch_clauses(query, %{multi_match: multi_match} = clause) do
    %{clause | multi_match: Map.put(multi_match, :query, query)}
  end

  defp fetch_clauses(query, %{term: term_query} = clause) do
    term =
      Map.new(term_query, fn
        {key, value} -> {key, Map.put(value, "value", query)}
      end)

    %{clause | term: term}
  end

  defp fetch_clauses(
         query,
         %{simple_query_string: %{quote_field_suffix: ".exact"} = simple_query_string} = clause
       ) do
    %{clause | simple_query_string: Map.put(simple_query_string, :query, maybe_wildcard(query))}
  end

  defp fetch_clauses(query, %{simple_query_string: simple_query_string} = clause) do
    %{clause | simple_query_string: Map.put(simple_query_string, :query, query)}
  end

  defp dependent_statements(%{minimum_should_match: _, should: _} = bool), do: bool
  defp dependent_statements(%{} = bool), do: Map.delete(bool, :minimum_should_match)
end
