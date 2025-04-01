defmodule TdCore.SearchTest do
  use ExUnit.Case

  alias TdCore.Search
  alias TdCore.TestSupport.CacheHelpers

  import Mox

  @moduletag sandbox: :shared

  @body %{"foo" => "bar"}
  @aggs %{"my_agg" => %{"buckets" => [%{"key" => "foo"}, %{"key" => "bar"}]}}
  @body_search_after %{
    query: "",
    sort: ["_id"],
    pit: %{id: "1", keep_alive: "1m"},
    size: 1_000,
    search_after: ["1"]
  }
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

  describe "Search.create_pit/2" do
    test "creates pit and formats response" do
      expect(ElasticsearchMock, :request, fn _, :post, "/concepts/_pit", %{}, opts ->
        assert opts == [params: %{"keep_alive" => "1m"}]
        {:ok, %{"id" => "foo"}}
      end)

      assert {:ok, %{id: "foo"}} == Search.create_pit(:concepts, %{"keep_alive" => "1m"})
    end
  end

  describe "Search.delete_pit/1" do
    test "deletes pit" do
      id = "1"

      expect(ElasticsearchMock, :request, fn _, :delete, "/_pit", %{"id" => ^id}, opts ->
        assert opts == []

        {:ok,
         %HTTPoison.Response{
           status_code: 200,
           body: %{"num_freed" => 1, "succeeded" => true},
           headers: [
             {"content-type", "application/json; charset=UTF-8"},
             {"content-length", "32"}
           ],
           request_url: "http://elastic:9200/_pit",
           request: %HTTPoison.Request{
             method: :delete,
             url: "http://elastic:9200/_pit",
             headers: [{"Content-Type", "application/json"}],
             body: "{\"id\":\"#{id}\"}",
             params: %{},
             options: [timeout: 5000, recv_timeout: 40000]
           }
         }}
      end)

      assert {:ok, %HTTPoison.Response{status_code: 200}} = Search.delete_pit(id)
    end
  end

  describe "Search.search_after/1" do
    test "supports search after pagination" do
      expect(ElasticsearchMock, :request, fn _, :post, "/_search", body, _opts ->
        assert body == %{
                 size: 1000,
                 sort: ["_id"],
                 query: "",
                 pit: %{id: "1", keep_alive: "1m"},
                 search_after: ["1"]
               }

        SearchHelpers.hits_response([], @es7_total)
      end)

      assert Search.search_after(@body_search_after) == {:ok, %{results: [], total: 123}}
    end
  end
end
