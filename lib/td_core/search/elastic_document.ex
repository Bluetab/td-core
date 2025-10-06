defmodule TdCore.Search.ElasticDocument do
  @moduledoc """
  Defines helper functions for handling the processing of elasticsearch structures
  """
  @missing_term_name "_missing"

  def missing_term_name, do: @missing_term_name

  alias TdCache.I18nCache
  alias TdCluster.Cluster.TdAi.Indices
  alias TdCore.Search.Cluster
  alias TdCore.Search.ElasticDocument
  alias TdDfLib.Templates

  @raw %{raw: %{type: "keyword", null_value: ""}}
  @exact %{exact: %{type: "text", analyzer: "exact_analyzer"}}
  @text_like_types ~w(text search_as_you_type)
  @supported_langs ~w(en es)
  @disabled_field_types ~w(url copy image table)
  @entity_types ~w(domain hierarchy system user)
  @date_types ~w(date datetime)
  @translatable_widgets ~w(enriched_text string textarea)
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
      @exact %{exact: %{type: "text", analyzer: "exact_analyzer"}}

      def get_dynamic_mappings(scope, opts \\ []),
        do: ElasticDocument.get_dynamic_mappings(scope, opts)

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
      defdelegate apply_lang_settings(index_config), to: ElasticDocument
      defdelegate get_embedding_mappings(), to: ElasticDocument
    end
  end

  def get_dynamic_mappings(scope, opts \\ []) do
    scope
    |> Templates.content_schema_for_scope()
    |> get_mappings(opts)
    |> Enum.into(%{})
  end

  defp get_mappings(fields, opts \\ []) do
    {:ok, default_locale} = I18nCache.get_default_locale()
    active_locales = I18nCache.get_active_locales!() -- [default_locale]

    fields
    |> maybe_filter(opts[:type])
    |> maybe_filter_widget(opts[:widgets])
    |> Enum.map(fn field ->
      field
      |> field_mapping()
      |> maybe_disable_search(field)
      |> add_locales_content_mapping(field, active_locales, opts[:add_locales?])
    end)
    |> List.flatten()
  end

  def get_embedding_mappings do
    case Indices.list(enabled: true) do
      {:ok, indices} -> Enum.into(indices, %{}, &to_vector_mapping/1)
      _error -> %{}
    end
  end

  defp maybe_filter(fields, type) when is_binary(type),
    do: Enum.filter(fields, &(Map.get(&1, "type") == type))

  defp maybe_filter(fields, types) when is_list(types),
    do: Enum.filter(fields, &(Map.get(&1, "type") in types))

  defp maybe_filter(fields, _type), do: fields

  defp maybe_filter_widget(fields, widgets) when is_list(widgets),
    do: Enum.filter(fields, &(Map.get(&1, "widget") in widgets))

  defp maybe_filter_widget(fields, _widgets), do: fields

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

  def field_mapping(%{
        "name" => name,
        "type" => "dynamic_table",
        "values" => %{"table_columns" => columns}
      }) do
    properties = columns |> get_mappings() |> Map.new()
    {name, %{type: "nested", properties: properties}}
  end

  def field_mapping(%{"name" => name}) do
    {name, %{type: "text", fields: Map.merge(@raw, @exact)}}
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

  def apply_lang_settings(index_config) do
    update_in(index_config, [:analysis, :analyzer, :default, :filter], &(&1 ++ lang_filter()))
  end

  defp maybe_disable_search({name, field_value}, %{"searchable" => false}) do
    {name, Map.drop(field_value, [:fields])}
  end

  defp maybe_disable_search(field_tuple, _), do: field_tuple

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

  defp add_locales_content_mapping(
         {name, content} = mapping,
         %{"widget" => widget},
         active_locales,
         true
       )
       when widget in @translatable_widgets do
    mapping_locales =
      Enum.map(active_locales, fn locale ->
        add_language_analyzer({"#{name}_#{locale}", content}, locale)
      end)

    [mapping | mapping_locales]
  end

  defp add_locales_content_mapping(mapping, _field, _active_locales, _other), do: mapping

  defp add_language_analyzer({field, %{type: type} = mapping}, lang)
       when type in @text_like_types and lang in @supported_langs do
    {field, Map.put(mapping, :analyzer, String.to_atom("#{lang}_analyzer"))}
  end

  defp add_language_analyzer(field_mapping, _lang), do: field_mapping

  defp lang_filter do
    case I18nCache.get_default_locale() do
      {:ok, "es"} -> ["es_stem"]
      {:ok, "en"} -> ["porter_stem"]
      _other -> ["porter_stem"]
    end
  end

  defp to_vector_mapping(%{collection_name: name, index_params: index_params}) do
    {"vector_#{name}", Map.put(index_params || %{}, "type", "dense_vector")}
  end
end
