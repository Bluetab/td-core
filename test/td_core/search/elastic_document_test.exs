defmodule TdCore.Search.ElasticDocumentTest do
  use ExUnit.Case

  alias TdCluster.TestHelpers.TdAiMock.Indices
  alias TdCore.Search.ElasticDocument
  alias TdCore.TestSupport.CacheHelpers
  alias TestCacheHelpers

  @locales ~w(en es)
  @default_locale "en"

  describe "get_dynamic_mappings/2" do
    setup do
      template = TestCacheHelpers.insert_template()
      %{template: template}
    end

    test "returns all fields" do
      assert %{
               "field-text" => %{},
               "multi-group" => %{},
               "multi-user" => %{},
               "single-group" => %{},
               "single-user" => %{}
             } = ElasticDocument.get_dynamic_mappings("ts")
    end

    test "returns single field type" do
      assert %{
               "multi-user" => %{},
               "single-user" => %{}
             } = ElasticDocument.get_dynamic_mappings("ts", type: "user")
    end

    test "returns multiple field types" do
      assert %{
               "multi-group" => %{},
               "multi-user" => %{},
               "single-group" => %{},
               "single-user" => %{}
             } = ElasticDocument.get_dynamic_mappings("ts", type: ["user", "user_group"])
    end
  end

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

  describe "get_embedding_mappings" do
    test "gets existing embedding mappings from ai indices" do
      Indices.list_indices(
        &Mox.expect/4,
        [index_type: "suggestions", enabled: true],
        {:ok, [%{collection_name: "collection_name", index_params: %{"dims" => "384"}}]}
      )

      assert %{"vector_collection_name" => %{"type" => "dense_vector", "dims" => "384"}} ==
               ElasticDocument.get_embedding_mappings()
    end

    test "returns default response on error node down reponse" do
      Indices.list_indices(
        &Mox.expect/4,
        [index_type: "suggestions", enabled: true],
        {:error, :nodedown}
      )

      assert %{} == ElasticDocument.get_embedding_mappings()
    end
  end
end
