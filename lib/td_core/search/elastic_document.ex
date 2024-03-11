defmodule TdCore.Search.ElasticDocument do
  @moduledoc """
  Defines helper functions for handling the processing of elasticsearch structures
  """
  @missing_term_name "_missing"

  def missing_term_name, do: @missing_term_name

  alias TdCache.TemplateCache
  alias TdCore.Search.Cluster
  alias TdDfLib.Format

  @raw %{raw: %{type: "keyword", null_value: ""}}

  defmacro __using__(_) do
    quote do
      alias TdCache.TemplateCache
      alias TdCore.Search.Cluster
      alias TdCore.Search.DocumentMapping
      alias TdCore.Search.ElasticDocument
      alias TdDfLib.Format

      @raw %{raw: %{type: "keyword", null_value: ""}}
      @text %{text: %{type: "text"}}
      @raw_sort %{
        raw: %{type: "keyword", null_value: ""},
        sort: %{type: "keyword", normalizer: "sortable"}
      }
      @raw_sort_ngram %{
        raw: %{type: "keyword", null_value: ""},
        sort: %{type: "keyword", normalizer: "sortable"},
        ngram: %{type: "text", analyzer: "ngram"}
      }

      def get_dynamic_mappings(scope, type \\ nil),
        do: ElasticDocument.get_dynamic_mappings(scope, type)

      def merge_dynamic_fields(static_aggs, scope, content_field \\ "df_content"),
        do: ElasticDocument.merge_dynamic_fields(static_aggs, scope, content_field)
    end
  end

  def get_dynamic_mappings(scope, type \\ nil) do
    scope
    |> TemplateCache.list_by_scope!()
    |> Enum.flat_map(&get_mappings(&1, type))
    |> Enum.into(%{})
  end

  def get_mappings(%{content: content}, nil) do
    content
    |> Format.flatten_content_fields()
    |> Enum.map(fn field ->
      field
      |> field_mapping
      |> maybe_boost(field)
      |> maybe_disable_search(field)
    end)
  end

  def get_mappings(%{content: content}, type) do
    content
    |> Format.flatten_content_fields()
    |> Enum.filter(&(Map.get(&1, "type") == type))
    |> Enum.map(fn field ->
      field
      |> field_mapping
      |> maybe_boost(field)
      |> maybe_disable_search(field)
    end)
  end

  def field_mapping(%{"name" => name, "type" => "table"}) do
    {name, %{enabled: false}}
  end

  def field_mapping(%{"name" => name, "type" => "url"}) do
    {name, %{enabled: false}}
  end

  def field_mapping(%{"name" => name, "type" => "copy"}) do
    {name, %{enabled: false}}
  end

  def field_mapping(%{"name" => name, "widget" => "identifier"}) do
    {name, %{type: "keyword"}}
  end

  def field_mapping(%{"name" => name, "type" => "domain"}) do
    {name, %{type: "long"}}
  end

  def field_mapping(%{"name" => name, "type" => "system"}) do
    {name,
     %{
       type: "nested",
       properties: %{
         id: %{type: "long"},
         name: %{type: "text", fields: @raw},
         external_id: %{type: "text", fields: @raw}
       }
     }}
  end

  def field_mapping(%{"name" => name, "type" => "enriched_text"}) do
    {name, mapping_type("enriched_text")}
  end

  def field_mapping(%{"name" => name, "values" => values}) do
    {name, mapping_type(values)}
  end

  def field_mapping(%{"name" => name}) do
    {name, mapping_type("string")}
  end

  def maybe_boost(field_tuple, %{"boost" => boost}) when boost in ["", "1"], do: field_tuple

  def maybe_boost({name, field_value}, %{"boost" => boost}) do
    {boost_float, _} = Float.parse(boost)
    {name, Map.put(field_value, :boost, boost_float)}
  end

  def maybe_boost(field_tuple, _), do: field_tuple

  def mapping_type(_default), do: %{type: "text", fields: @raw}

  def maybe_disable_search({name, field_value}, %{"searchable" => false}) do
    {name, Map.drop(field_value, [:fields])}
  end

  def maybe_disable_search(field_tuple, _), do: field_tuple

  def merge_dynamic_fields(static_aggs, scope, content_field \\ "df_content") do
    TemplateCache.list_by_scope!(scope)
    |> Enum.flat_map(&content_terms(&1, content_field))
    |> Map.new()
    |> Map.merge(static_aggs)
  end

  def content_terms(%{content: content}, content_field \\ "df_content") do
    content
    |> Format.flatten_content_fields()
    |> Enum.flat_map(fn
      %{"name" => field, "type" => "domain"} ->
        [
          {field,
           %{
             terms: %{
               field: "#{content_field}.#{field}",
               size: Cluster.get_size_field("domain")
             },
             meta: %{type: "domain"}
           }}
        ]

      %{"name" => field, "type" => "hierarchy"} ->
        [
          {field,
           %{
             terms: %{
               field: "#{content_field}.#{field}.raw",
               size: Cluster.get_size_field("hierarchy")
             },
             meta: %{type: "hierarchy"}
           }}
        ]

      %{"name" => field, "type" => "system"} ->
        [{field, nested_agg(field, content_field, "system")}]

      %{"name" => field, "type" => "user"} ->
        [
          {field,
           %{
             terms: %{
               field: "#{content_field}.#{field}.raw",
               size: Cluster.get_size_field("user")
             }
           }}
        ]

      %{"name" => field, "values" => %{}} ->
        [{field, %{terms: %{field: "#{content_field}.#{field}.raw"}}}]

      _ ->
        []
    end)
  end

  def nested_agg(field, content_field, field_type) do
    %{
      nested: %{path: "#{content_field}.#{field}"},
      aggs: %{
        distinct_search: %{
          terms: %{
            field: "#{content_field}.#{field}.external_id.raw",
            size: Cluster.get_size_field(field_type)
          }
        }
      }
    }
  end
end
