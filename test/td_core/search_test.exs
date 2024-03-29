defmodule TdCore.SearchTest do
  use ExUnit.Case

  alias TdCore.Search
  alias TdCore.TestSupport.CacheHelpers

  import Mox

  @moduletag sandbox: :shared

  @body %{"foo" => "bar"}
  @aggs %{"my_agg" => %{"buckets" => [%{"key" => "foo"}, %{"key" => "bar"}]}}
  @es6_total 123
  @es7_total %{"relation" => "eq", "value" => 123}

  setup :verify_on_exit!

  describe "Search.search/3" do
    test "is compatible with Elasticsearch 6.x" do
      ElasticsearchMock
      |> expect(:request, fn _, :post, "/foo/_search", body, opts ->
        assert opts == [params: %{"track_total_hits" => "true"}]
        assert body == @body
        SearchHelpers.hits_response([], @es6_total)
      end)

      assert Search.search(@body, "foo") == {:ok, %{results: [], total: 123}}
    end

    test "is compatible with Elasticsearch 7.x" do
      ElasticsearchMock
      |> expect(:request, fn _, :post, "/foo/_search", body, opts ->
        assert opts == [params: %{"track_total_hits" => "true"}]
        assert body == @body
        SearchHelpers.hits_response([], @es7_total)
      end)

      assert Search.search(@body, "foo") == {:ok, %{results: [], total: 123}}
    end

    test "translates atom to index alias" do
      ElasticsearchMock
      |> expect(:request, fn _, :post, "/string_test_alias/_search", _, _ ->
        SearchHelpers.hits_response([])
      end)

      assert {:ok, _} = Search.search(@body, :test_alias)
    end

    test "formats aggregation values from response" do
      ElasticsearchMock
      |> expect(:request, fn _, :post, "/foo/_search", _, _ ->
        SearchHelpers.aggs_response(@aggs, 123)
      end)

      assert {:ok,
              %{aggregations: %{"my_agg" => %{values: ["foo", "bar"]}}, results: [], total: 123}} =
               Search.search(%{}, "foo")
    end

    test "does not format aggregations from response if format: :raw is specified" do
      ElasticsearchMock
      |> expect(:request, fn _, :post, "/foo/_search", _, _ ->
        SearchHelpers.aggs_response(@aggs, 123)
      end)

      assert Search.search(%{}, "foo", format: :raw) ==
               {:ok, %{aggregations: @aggs, results: [], total: 123}}
    end

    test "enriches taxonomy aggregation" do
      %{id: parent_id} = CacheHelpers.insert_domain()
      %{id: domain_id} = CacheHelpers.insert_domain(parent_id: parent_id)

      ElasticsearchMock
      |> expect(:request, fn _, :post, "/foo/_search", _, _ ->
        SearchHelpers.aggs_response(%{
          "taxonomy" => %{"buckets" => [%{"key" => domain_id, "doc_count" => 12}]}
        })
      end)

      assert {:ok, %{aggregations: %{"taxonomy" => values}}} = Search.search(%{}, "foo")

      assert %{type: :domain, values: [%{id: _, external_id: _, parent_id: _, name: _}, %{id: _}]} =
               values
    end

    test "enriches template fields of type domain for filters" do
      %{id: domain_1_id, name: domain_1_name} = CacheHelpers.insert_domain()
      %{id: domain_2_id, name: domain_2_name} = CacheHelpers.insert_domain()

      ElasticsearchMock
      |> expect(:request, fn _, :post, "/foo/_search", _, _ ->
        SearchHelpers.aggs_response(%{
          "implementation_template_domain_field" => %{
            "meta" => %{"type" => "domain"},
            "buckets" => [
              %{"doc_count" => 4, "key" => domain_1_id},
              %{"doc_count" => 4, "key" => domain_2_id}
            ]
          }
        })
      end)

      assert {:ok, %{aggregations: %{"implementation_template_domain_field" => values}}} =
               Search.search(%{}, "foo")

      assert %{
               type: :domain,
               values: [
                 %{id: ^domain_1_id, name: ^domain_1_name},
                 %{id: ^domain_2_id, name: ^domain_2_name}
               ]
             } = values
    end
  end
end
