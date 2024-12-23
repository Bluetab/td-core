defmodule TdCore.Search.ElasticDocumentTest do
  use ExUnit.Case

  alias TdCore.TestSupport.CacheHelpers
  alias TdCore.Search.ElasticDocument

  @locales ~w(en es)
  @default_locale "en"

  describe "add_locales_fields_mapping/2" do
    setup do
      CacheHelpers.put_default_locale(@default_locale)
      CacheHelpers.put_active_locales(@locales)
    end

    test "adds locales to mappings for non default languages" do
      mappings = %{
        name: %{type: "text", fields: %{raw: %{type: "keyword", normalizer: "sortable"}}},
        ngram_name: %{type: "search_as_you_type"}
      }

      assert %{
               name: %{
                 fields: %{raw: %{normalizer: "sortable", type: "keyword"}},
                 type: "text"
               },
               name_es: %{
                 analyzer: :es_analyzer,
                 fields: %{raw: %{normalizer: "sortable", type: "keyword"}},
                 type: "text"
               },
               ngram_name: %{type: "search_as_you_type"}
             } == ElasticDocument.add_locales_fields_mapping(mappings, [:name])
    end
  end

  describe "add_locales/2" do
    setup do
      CacheHelpers.put_default_locale(@default_locale)
      CacheHelpers.put_active_locales(@locales)
    end

    test "adds locales to fields for non default languages" do
      assert ["name", "description", "name_es", "description_es"] ==
               ElasticDocument.add_locales([:name, :description])
    end
  end
end
