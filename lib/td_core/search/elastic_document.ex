defmodule TdCore.Search.ElasticDocument do
  @moduledoc """
  Defines helper functions for handling the processing of elasticsearch structures
  """
  @missing_term_name "_missing"

  def missing_term_name, do: @missing_term_name

  alias TdCache.I18nCache
  alias TdCache.TemplateCache
  alias TdCore.Search.Cluster
  alias TdCore.Search.ElasticDocument
  alias TdDfLib.Format

  @raw %{raw: %{type: "keyword", null_value: ""}}
  @disabled_field_types ~w(table url copy)
  @entity_types ~w(domain hierarchy system user)
  @fitrable_types @disabled_field_types ++ @entity_types

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

      def add_locales_fields_mapping(mapping, fields),
        do: ElasticDocument.add_locales_fields_mapping(mapping, fields)

      def add_locales_content_mapping(fields_properties),
        do: ElasticDocument.add_locales_content_mapping(fields_properties)

      def merge_dynamic_fields(static_aggs, scope, content_field \\ "df_content"),
        do: ElasticDocument.merge_dynamic_fields(static_aggs, scope, content_field)

      defdelegate searchable_fields(scope), to: ElasticDocument
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
      |> add_locales_content_mapping()
    end)
    |> List.flatten()
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

  def field_mapping(%{"name" => name, "type" => type}) when type not in @disabled_field_types do
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

  def add_locales_fields_mapping(mapping, fields) do
    {:ok, default_locale} = I18nCache.get_default_locale()

    locales =
      I18nCache.get_active_locales!()
      |> Enum.reject(&(&1 == default_locale))

    fields
    |> Enum.map(fn field ->
      field_property = Map.get(mapping, field)

      locales
      |> Enum.reduce(%{}, fn locale, acc ->
        Map.put(acc, :"#{field}_#{locale}", field_property)
      end)
    end)
    |> Enum.reduce(%{}, &Map.merge(&1, &2))
    |> Map.merge(mapping)
  end

  def add_locales_content_mapping({name, fields_properties}) do
    {:ok, default_locale} = I18nCache.get_default_locale()

    I18nCache.get_active_locales!()
    |> Enum.map(fn locale ->
      if locale == default_locale,
        do: {"#{name}", fields_properties},
        else: {"#{name}_#{locale}", fields_properties}
    end)
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
    scope
    |> content_terms(content_field)
    |> Map.merge(static_aggs)
  end

  def content_terms(content_or_scope, content_field \\ "df_content")

  def content_terms(scope, content_field) when is_binary(scope) do
    scope
    |> TemplateCache.list_by_scope!()
    |> Enum.flat_map(&content_terms(&1, content_field))
    |> Map.new()
  end

  def content_terms(%{content: content}, content_field) do
    content
    |> Format.flatten_content_fields()
    |> Enum.map(fn
      %{"name" => field, "type" => "domain"} ->
        {field,
         %{
           terms: %{
             field: "#{content_field}.#{field}",
             size: Cluster.get_size_field("domain")
           },
           meta: %{type: "domain"}
         }}

      %{"name" => field, "type" => "hierarchy"} ->
        {field,
         %{
           terms: %{
             field: "#{content_field}.#{field}.raw",
             size: Cluster.get_size_field("hierarchy")
           },
           meta: %{type: "hierarchy"}
         }}

      %{"name" => field, "type" => "system"} ->
        {field, nested_agg(field, content_field, "system")}

      %{"name" => field, "type" => "user"} ->
        {field,
         %{
           terms: %{
             field: "#{content_field}.#{field}.raw",
             size: Cluster.get_size_field("user")
           }
         }}

      %{"name" => field, "values" => %{}} ->
        {field,
         %{
           terms: %{
             field: "#{content_field}.#{field}.raw",
             size: Cluster.get_size_field("default")
           }
         }}

      _ ->
        nil
    end)
    |> Enum.filter(& &1)
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

  def searchable_fields(scope) do
    scope
    |> TemplateCache.list_by_scope!()
    |> Enum.flat_map(fn %{content: content} -> Format.flatten_content_fields(content) end)
    |> Enum.reject(fn
      %{"type" => type} -> type in @fitrable_types
      %{"values" => %{}} -> true
      _other -> false
    end)
    |> Enum.flat_map(&add_locales_content_mapping/1)
    |> Enum.map(fn {field, _} -> field end)
    |> Enum.uniq()
  end
end
