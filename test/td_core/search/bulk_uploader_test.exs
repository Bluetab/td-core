defmodule TdCore.Search.BulkUploaderTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mox

  alias TdCore.Search.BulkUploader
  alias TdCore.Search.Cluster

  @upload_config %{
    api: ElasticsearchMock,
    url: "http://none",
    json_library: Jason
  }

  setup :verify_on_exit!

  defmodule Collector do
    @moduledoc false
    @agent __MODULE__

    def start do
      case Agent.start_link(fn -> [] end, name: @agent) do
        {:ok, pid} ->
          pid

        {:error, {:already_started, pid}} ->
          Agent.update(@agent, fn _ -> [] end)
          pid
      end
    end

    def time(phase, fun) do
      Agent.update(@agent, &[phase | &1])
      fun.()
    end

    def phases, do: Agent.get(@agent, &Enum.reverse/1)
  end

  setup do
    on_exit(fn -> Application.delete_env(:td_core, :search_phase_profiler) end)
    :ok
  end

  describe "post_bulk_bodies/4 with concurrency <= 1" do
    test "posts every body preserving order and returns responses" do
      ElasticsearchMock
      |> expect(:request, 2, fn _, :post, "/idx/_bulk", body, [] ->
        {:ok, %{"echo" => body}}
      end)

      results =
        ["first", "second"]
        |> BulkUploader.post_bulk_bodies(Cluster, "/idx/_bulk", 1)
        |> Enum.to_list()

      assert results == [{:ok, %{"echo" => "first"}}, {:ok, %{"echo" => "second"}}]
    end

    test "invokes the :bulk_es phase hook once per body when profiler is configured" do
      Collector.start()
      Application.put_env(:td_core, :search_phase_profiler, {Collector, :time})

      ElasticsearchMock
      |> expect(:request, 2, fn _, :post, "/idx/_bulk", body, [] ->
        {:ok, %{"echo" => body}}
      end)

      ["a", "b"]
      |> BulkUploader.post_bulk_bodies(Cluster, "/idx/_bulk", 1)
      |> Stream.run()

      assert Collector.phases() == [:bulk_es, :bulk_es]
    end

    test "runs without hooks when profiler is not configured" do
      Application.delete_env(:td_core, :search_phase_profiler)

      ElasticsearchMock
      |> expect(:request, 1, fn _, :post, "/idx/_bulk", "body", [] ->
        {:ok, %{"errors" => false}}
      end)

      results =
        ["body"]
        |> BulkUploader.post_bulk_bodies(Cluster, "/idx/_bulk", 1)
        |> Enum.to_list()

      assert results == [{:ok, %{"errors" => false}}]
    end
  end

  describe "post_bulk_bodies/4 with concurrency > 1" do
    test "posts all bodies preserving stream order when concurrency > 1" do
      ElasticsearchMock
      |> expect(:request, 3, fn _, :post, "/idx/_bulk", body, [] ->
        {:ok, %{"echo" => body}}
      end)

      results =
        ["a", "b", "c"]
        |> BulkUploader.post_bulk_bodies(Cluster, "/idx/_bulk", 2)
        |> Enum.to_list()

      assert results == [
               {:ok, %{"echo" => "a"}},
               {:ok, %{"echo" => "b"}},
               {:ok, %{"echo" => "c"}}
             ]
    end

    test "collects errors from all responses regardless of completion order" do
      ElasticsearchMock
      |> expect(:request, 2, fn _, :post, "/idx/_bulk", body, [] ->
        case body do
          "ok" ->
            {:ok, %{"errors" => false, "items" => []}}

          "fail" ->
            {:ok, %{"errors" => true, "items" => [%{"index" => %{"error" => %{"type" => "x"}}}]}}
        end
      end)

      results =
        ["ok", "fail"]
        |> BulkUploader.post_bulk_bodies(Cluster, "/idx/_bulk", 2)
        |> Enum.to_list()

      assert length(results) == 2
      assert Enum.any?(results, &match?({:ok, %{"errors" => true}}, &1))
      assert Enum.any?(results, &match?({:ok, %{"errors" => false}}, &1))
    end
  end

  describe "record_bulk_response/4" do
    test "collects no errors on successful bulk response" do
      response =
        {:ok,
         %{"errors" => false, "items" => [%{"index" => %{}}, %{"index" => %{}}], "took" => 123}}

      assert [] == BulkUploader.record_bulk_response("structures-1", response, [], "index")
    end

    test "collects errors from a failed bulk response" do
      response =
        {:ok,
         %{
           "errors" => true,
           "items" => [
             %{
               "index" => %{
                 "_id" => "7",
                 "status" => 400,
                 "error" => %{"reason" => "boom", "type" => "mapper_parsing_exception"}
               }
             }
           ]
         }}

      capture_log(fn ->
        assert [%Elasticsearch.Exception{}] =
                 BulkUploader.record_bulk_response("structures-1", response, [], "index")
      end)
    end

    test "does not log bulk wait interval ok" do
      log =
        capture_log(fn ->
          assert [] == BulkUploader.record_bulk_response("structures-1", :ok, [], "index")
        end)

      assert log == ""
    end
  end

  describe "upload/4" do
    alias TdCore.Search.BulkUploaderTxnRequiredStore
    alias TdCore.Search.BulkUploaderUploadDoc
    alias TdCore.Search.BulkUploaderUploadStore

    test "uploads every document from the store, one page per bulk_page_size" do
      ElasticsearchMock
      |> expect(:request, fn _, :put, "/upload-idx/_bulk", body, [] ->
        assert body =~ ~s("index":)
        assert body =~ ~s("_id":1)
        assert body =~ ~s("id":1)
        {:ok, %{"errors" => false, "items" => [], "took" => 1}}
      end)
      |> expect(:request, fn _, :put, "/upload-idx/_bulk", body, [] ->
        assert body =~ ~s("_id":2)
        assert body =~ ~s("id":2)
        {:ok, %{"errors" => false, "items" => [], "took" => 1}}
      end)

      capture_log(fn ->
        assert :ok ==
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   index_config(BulkUploaderUploadStore, 1),
                   []
                 )
      end)
    end

    test "uploads the same documents when concurrency is greater than 1" do
      ElasticsearchMock
      |> expect(:request, 2, fn _, :put, "/upload-idx/_bulk", body, [] ->
        assert body =~ ~s("index":)
        assert body =~ ~r/"_id":[12]/
        assert body =~ ~r/"id":[12]/
        {:ok, %{"errors" => false, "items" => [], "took" => 1}}
      end)

      capture_log(fn ->
        assert :ok ==
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   index_config(BulkUploaderUploadStore, 4),
                   []
                 )
      end)
    end

    test "returns collected errors when a bulk page fails" do
      ElasticsearchMock
      |> expect(:request, 2, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        {:ok,
         %{
           "errors" => true,
           "items" => [
             %{
               "index" => %{
                 "_id" => "1",
                 "status" => 400,
                 "error" => %{"reason" => "boom", "type" => "mapper_parsing_exception"}
               }
             }
           ]
         }}
      end)

      capture_log(fn ->
        assert {:error, errors} =
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   index_config(BulkUploaderUploadStore, 1),
                   []
                 )

        assert length(errors) == 2
        assert Enum.all?(errors, &match?(%Elasticsearch.Exception{}, &1))
      end)
    end

    test "consumes store.stream inside store.transaction" do
      ElasticsearchMock
      |> expect(:request, 2, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        {:ok, %{"errors" => false, "items" => [], "took" => 1}}
      end)

      capture_log(fn ->
        assert :ok ==
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   index_config(BulkUploaderTxnRequiredStore, 1),
                   []
                 )
      end)
    end

    test "raises when stream is reduced outside transaction" do
      assert_raise RuntimeError, "cannot reduce stream outside of transaction", fn ->
        BulkUploaderTxnRequiredStore
        |> then(fn store -> store.stream(BulkUploaderUploadDoc) end)
        |> Enum.to_list()
      end
    end
  end

  defp index_config(store, concurrency) do
    %{
      store: store,
      sources: [TdCore.Search.BulkUploaderUploadDoc],
      bulk_page_size: 1,
      bulk_wait_interval: 0,
      bulk_action: "index",
      reindex_concurrency: concurrency
    }
  end
end
