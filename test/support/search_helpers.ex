defmodule SearchHelpers do
  @moduledoc """
  Helper functions for mocking search responses.
  """
  import ExUnit.Assertions

  def expect_bulk_index(url, n \\ 1) do
    ElasticsearchMock
    |> Mox.expect(:request, n, fn _, :post, expected_url, _, [] ->
      assert url == expected_url
      bulk_index_response()
    end)
  end

  def bulk_index_response do
    {:ok, %{"errors" => false, "items" => [], "took" => 0}}
  end

  def hits_response(hits, total \\ nil) when is_list(hits) do
    hits = Enum.map(hits, &encode/1)
    total = total || %{"relation" => "eq", "value" => Enum.count(hits)}
    {:ok, %{"hits" => %{"hits" => hits, "total" => total}}}
  end

  def aggs_response(aggs \\ %{}, total \\ 0) do
    {:ok,
     %{
       "aggregations" => aggs,
       "hits" => %{"hits" => [], "total" => %{"relation" => "eq", "value" => total}}
     }}
  end

  def scroll_response(hits, total \\ nil) do
    {:ok, resp} = hits_response(hits, total)
    {:ok, Map.put(resp, "_scroll_id", "some_scroll_id")}
  end

  defp encode(doc) do
    id = Elasticsearch.Document.id(doc)

    source =
      doc
      |> Elasticsearch.Document.encode()
      |> Jason.encode!()
      |> Jason.decode!()

    %{"id" => id, "_source" => source}
  end
end
