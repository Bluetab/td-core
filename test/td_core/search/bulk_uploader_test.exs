defmodule TdCore.Search.BulkUploaderTest do
  use ExUnit.Case, async: false

  import Mox

  alias TdCore.Search.BulkUploader
  alias TdCore.Search.Cluster

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
          "ok" -> {:ok, %{"errors" => false, "items" => []}}
          "fail" -> {:ok, %{"errors" => true, "items" => [%{"index" => %{"error" => %{"type" => "x"}}}]}}
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
end
