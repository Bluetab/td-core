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
    test "logs took on successful bulk response and collects no errors" do
      response =
        {:ok,
         %{"errors" => false, "items" => [%{"index" => %{}}, %{"index" => %{}}], "took" => 123}}

      log =
        capture_log([level: :debug], fn ->
          assert [] == BulkUploader.record_bulk_response("structures-1", response, [], "index")
        end)

      assert log =~ "structures-1: bulk indexed 2 documents (took=123)"
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

  describe "upload/4 retries" do
    setup :with_bulk_retries

    test "retries a page after a closed connection and returns :ok" do
      ElasticsearchMock
      |> expect(:request, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        closed_error()
      end)
      |> expect(:request, 2, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        bulk_ok()
      end)

      capture_log(fn ->
        assert :ok ==
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   retry_index_config(1),
                   []
                 )
      end)
    end

    test "retries a page after an invalid_state error until it succeeds" do
      ElasticsearchMock
      |> expect(:request, 2, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        {:error, %HTTPoison.Error{reason: :invalid_state}}
      end)
      |> expect(:request, 2, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        bulk_ok()
      end)

      capture_log(fn ->
        assert :ok ==
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   retry_index_config(1),
                   []
                 )
      end)
    end

    test "returns the transport error after exhausting retries" do
      ElasticsearchMock
      |> expect(:request, 4, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        closed_error()
      end)

      capture_log(fn ->
        assert {:error, [%HTTPoison.Error{reason: :closed}]} =
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   retry_index_config(1, bulk_page_size: 2),
                   []
                 )
      end)
    end

    test "retries a page rejected with HTTP 429" do
      ElasticsearchMock
      |> expect(:request, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        {:error, rejected_exception()}
      end)
      |> expect(:request, 2, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        bulk_ok()
      end)

      capture_log(fn ->
        assert :ok ==
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   retry_index_config(1),
                   []
                 )
      end)
    end

    test "resends only the documents rejected with 429" do
      ElasticsearchMock
      |> expect(:request, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        rejected_page()
      end)
      |> expect(:request, fn _, :put, "/upload-idx/_bulk", body, [] ->
        assert body =~ ~s("_id":2)
        refute body =~ ~s("_id":1)
        bulk_ok()
      end)

      capture_log(fn ->
        assert :ok ==
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   retry_index_config(1, bulk_page_size: 2),
                   []
                 )
      end)
    end

    test "treats version conflicts on a retry as created" do
      ElasticsearchMock
      |> expect(:request, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        closed_error()
      end)
      |> expect(:request, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        version_conflicts("create")
      end)

      capture_log(fn ->
        assert :ok ==
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   retry_index_config(1, bulk_page_size: 2, bulk_action: "create"),
                   []
                 )
      end)
    end

    test "keeps a version conflict on the first attempt as an error" do
      ElasticsearchMock
      |> expect(:request, 1, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        version_conflicts("create")
      end)

      capture_log(fn ->
        assert {:error, errors} =
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   retry_index_config(1, bulk_page_size: 2, bulk_action: "create"),
                   []
                 )

        assert length(errors) == 2
        assert Enum.all?(errors, &match?(%Elasticsearch.Exception{}, &1))
      end)
    end

    test "does not retry document errors" do
      ElasticsearchMock
      |> expect(:request, 1, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        document_error_page()
      end)

      capture_log(fn ->
        assert {:error, [%Elasticsearch.Exception{}]} =
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   retry_index_config(1, bulk_page_size: 2),
                   []
                 )
      end)
    end

    test "logs a warning for each retry" do
      ElasticsearchMock
      |> expect(:request, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        closed_error()
      end)
      |> expect(:request, 2, fn _, :put, "/upload-idx/_bulk", _body, [] ->
        bulk_ok(0)
      end)

      log =
        capture_log(fn ->
          assert :ok ==
                   BulkUploader.upload(
                     @upload_config,
                     "upload-idx",
                     retry_index_config(1),
                     []
                   )
        end)

      assert log =~ "upload-idx"
      assert log =~ ":closed"
      assert log =~ "upload-idx: bulk retry 1/3 after :closed"
    end

    test "retries with concurrency greater than 1" do
      {:ok, attempts} = Agent.start_link(fn -> %{} end)

      ElasticsearchMock
      |> expect(:request, 4, fn _, :put, "/upload-idx/_bulk", body, [] ->
        id = document_id(body)

        count =
          Agent.get_and_update(attempts, fn counts ->
            count = Map.get(counts, id, 0) + 1
            {count, Map.put(counts, id, count)}
          end)

        if count == 1 do
          closed_error()
        else
          bulk_ok()
        end
      end)

      capture_log(fn ->
        assert :ok ==
                 BulkUploader.upload(
                   @upload_config,
                   "upload-idx",
                   retry_index_config(2),
                   []
                 )
      end)
    end
  end

  describe "post_bulk_bodies/4 retries" do
    setup :with_bulk_retries

    test "retries a body after a closed connection" do
      ElasticsearchMock
      |> expect(:request, fn _, :post, "/idx/_bulk", "foo", [] ->
        closed_error()
      end)
      |> expect(:request, fn _, :post, "/idx/_bulk", "foo", [] ->
        {:ok, %{"errors" => false}}
      end)

      capture_log(fn ->
        results =
          ["foo"]
          |> BulkUploader.post_bulk_bodies(Cluster, "/idx/_bulk", 1)
          |> Enum.to_list()

        assert results == [{:ok, %{"errors" => false}}]
      end)
    end

    test "returns the error after exhausting retries" do
      ElasticsearchMock
      |> expect(:request, 4, fn _, :post, "/idx/_bulk", "foo", [] ->
        closed_error()
      end)

      capture_log(fn ->
        results =
          ["foo"]
          |> BulkUploader.post_bulk_bodies(Cluster, "/idx/_bulk", 1)
          |> Enum.to_list()

        assert results == [{:error, %HTTPoison.Error{reason: :closed}}]
      end)
    end
  end

  defp with_bulk_retries(_context) do
    previous = Application.get_env(:td_core, TdCore.Search.Cluster)

    Application.put_env(
      :td_core,
      TdCore.Search.Cluster,
      Keyword.merge(previous || [], bulk_max_retries: 3, bulk_retry_base_ms: 0)
    )

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:td_core, TdCore.Search.Cluster)
      else
        Application.put_env(:td_core, TdCore.Search.Cluster, previous)
      end
    end)

    :ok
  end

  defp retry_index_config(concurrency, opts \\ []) do
    %{
      store: TdCore.Search.BulkUploaderUploadStore,
      sources: [TdCore.Search.BulkUploaderUploadDoc],
      bulk_page_size: Keyword.get(opts, :bulk_page_size, 1),
      bulk_wait_interval: 0,
      bulk_action: Keyword.get(opts, :bulk_action, "index"),
      reindex_concurrency: concurrency
    }
  end

  defp bulk_ok(took \\ 1) do
    {:ok, %{"errors" => false, "items" => [], "took" => took}}
  end

  defp closed_error, do: {:error, %HTTPoison.Error{reason: :closed}}

  defp rejected_exception do
    Elasticsearch.Exception.exception(
      response: %{
        "status" => 429,
        "error" => %{"type" => "es_rejected_execution_exception"}
      }
    )
  end

  defp rejected_page do
    {:ok,
     %{
       "errors" => true,
       "items" => [
         %{"index" => %{"_id" => "1", "status" => 201}},
         %{
           "index" => %{
             "_id" => "2",
             "status" => 429,
             "error" => %{"type" => "es_rejected_execution_exception"}
           }
         }
       ]
     }}
  end

  defp version_conflicts(action) do
    {:ok,
     %{
       "errors" => true,
       "items" => [
         %{
           action => %{
             "_id" => "1",
             "status" => 409,
             "error" => %{"type" => "version_conflict_engine_exception"}
           }
         },
         %{
           action => %{
             "_id" => "2",
             "status" => 409,
             "error" => %{"type" => "version_conflict_engine_exception"}
           }
         }
       ]
     }}
  end

  defp document_error_page do
    {:ok,
     %{
       "errors" => true,
       "items" => [
         %{"index" => %{"_id" => "1", "status" => 201}},
         %{
           "index" => %{
             "_id" => "2",
             "status" => 400,
             "error" => %{"reason" => "foo", "type" => "mapper_parsing_exception"}
           }
         }
       ]
     }}
  end

  defp document_id(body) do
    cond do
      String.contains?(body, ~s("_id":1)) -> 1
      String.contains?(body, ~s("_id":2)) -> 2
    end
  end
end
