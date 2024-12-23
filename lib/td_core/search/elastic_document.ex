defmodule TdCore.Search.ElasticDocument do
  @moduledoc """
  Defines helper functions for handling the processing of elasticsearch structures
  """
  @missing_term_name "_missing"

  def missing_term_name, do: @missing_term_name

  alias TdCache.I18nCache
  alias TdCore.Search.Cluster
  alias TdCore.Search.ElasticDocument
  alias TdDfLib.Templates

  @raw %{raw: %{type: "keyword", null_value: ""}}
  @text_like_types ~w(text search_as_you_type)
  @supported_langs ~w(en es)
  @disabled_field_types ~w(table url copy image)
  @entity_types ~w(domain hierarchy system user)
  @date_types ~w(date datetime)
  @excluded_search_field_types @disabled_field_types ++ @entity_types ++ @date_types

  defmacro __using__(_) do
    quote do
      alias TdCache.TemplateCache
      alias TdCore.Search.Cluster
      alias TdCore.Search.DocumentMapping
      alias TdCore.Search.ElasticDocument
      alias TdDfLib.Format
      alias TdDfLib.Templates

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

      defdelegate merge_dynamic_aggregations(
                    static_aggs,
                    scope_or_content_schema,
                    content_field \\ "df_content"
                  ),
                  to: ElasticDocument

      defdelegate dynamic_search_fields(scope_or_schema, content_field \\ "df_content"),
        to: ElasticDocument

      defdelegate add_locales(fields), to: ElasticDocument
    end
  end

  def get_dynamic_mappings(scope, type \\ nil) do
    scope
    |> Templates.content_schema_for_scope()
    |> get_mappings(type)
    |> Enum.into(%{})
  end

  def get_mappings(fields, nil) do
    fields
    |> Enum.map(fn field ->
      field
      |> field_mapping
      |> maybe_boost(field)
      |> maybe_disable_search(field)
      |> add_locales_content_mapping()
    end)
    |> List.flatten()
  end

  def get_mappings(fields, type) do
    fields
    |> Enum.filter(&(Map.get(&1, "type") == type))
    |> Enum.map(fn field ->
      field
      |> field_mapping
      |> maybe_boost(field)
      |> maybe_disable_search(field)
    end)
  end

  def field_mapping(%{"name" => name, "type" => type}) when type in @disabled_field_types do
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
    locales = Enum.reject(I18nCache.get_active_locales!(), &(&1 == default_locale))

    mapping
    |> Map.take(fields)
    |> Enum.flat_map(fn {field, mapping} ->
      Enum.map(locales, &add_language_analyzer({:"#{field}_#{&1}", mapping}, &1))
    end)
    |> Map.new()
    |> Map.merge(mapping)
  end

  def add_locales(fields) when is_list(fields) do
    {:ok, default_locale} = I18nCache.get_default_locale()
    locales_applicable = I18nCache.get_active_locales!() -- [default_locale]
    binary_fields = Enum.map(fields, &"#{&1}")

    case locales_applicable do
      [] ->
        binary_fields

      [_ | _] ->
        binary_fields ++ apply_locales(locales_applicable, fields)
    end
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

  def merge_dynamic_aggregations(
        static_aggs,
        scope_or_content_schema,
        content_field \\ "df_content"
      )

  def merge_dynamic_aggregations(static_aggs, scope, content_field)
      when is_binary(scope) do
    scope
    |> Templates.content_schema_for_scope()
    |> content_terms(content_field)
    |> Map.merge(static_aggs)
  end

  def merge_dynamic_aggregations(static_aggs, content_schema, content_field)
      when is_list(content_schema) do
    content_schema
    |> content_terms(content_field)
    |> Map.merge(static_aggs)
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

  def dynamic_search_fields(scope, content_field) when is_binary(scope) do
    scope
    |> Templates.content_schema_for_scope()
    |> get_dynamic_search_fields(content_field)
  end

  def dynamic_search_fields(content_schema, content_field) when is_list(content_schema) do
    get_dynamic_search_fields(content_schema, content_field)
  end

  defp get_dynamic_search_fields(content_schema, content_field) do
    content_schema
    |> Enum.reject(fn
      %{"values" => %{}} -> true
      %{"widget" => "identifier"} -> true
      %{"type" => type} -> type in @excluded_search_field_types
      _other -> false
    end)
    |> Enum.map(fn %{"name" => name} -> "#{content_field}.#{name}" end)
    |> Enum.uniq()
  end

  defp content_terms(fields, content_field) when is_list(fields) do
    fields
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
    |> Map.new()
  end

  defp apply_locales(locales, fields) do
    Enum.flat_map(fields, fn field ->
      Enum.map(locales, fn locale -> "#{field}_#{locale}" end)
    end)
  end

  defp add_locales_content_mapping({name, mapping}) do
    {:ok, default_locale} = I18nCache.get_default_locale()

    Enum.map(I18nCache.get_active_locales!(), fn locale ->
      if locale == default_locale,
        do: {"#{name}", mapping},
        else: add_language_analyzer({"#{name}_#{locale}", mapping}, locale)
    end)
  end

  defp add_language_analyzer({field, %{type: type} = mapping}, lang)
       when type in @text_like_types and lang in @supported_langs do
    {field, Map.put(mapping, :analyzer, String.to_atom("#{lang}_analyzer"))}
  end

  defp add_language_analyzer(field_mapping, _lang), do: field_mapping
end
