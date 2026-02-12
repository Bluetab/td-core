defmodule TdCore.Search.IndexWorkerImpl do
  @moduledoc """
  GenServer to run reindex task
  """

  use GenServer

  alias TdCore.Search.Indexer
  alias TdCore.Utils.Timer

  require Logger

  def start_link(index) do
    GenServer.start_link(__MODULE__, index, name: index)
  end

  def reindex(index, ids) do
    GenServer.cast(index, {:reindex, ids})
  end

  def delete(index, ids_or_tuple) do
    GenServer.cast(index, {:delete, ids_or_tuple})
  end

  def delete_index_documents_by_query(index, query) do
    GenServer.call(index, {:delete_index_documents_by_query, query})
  end

  def put_embeddings(index, ids) do
    GenServer.cast(index, {:put_embeddings, ids})
  end

  def refresh_links(index, ids) do
    GenServer.cast(index, {:refresh_links, ids})
  end

  def get_index_workers do
    :td_core
    |> Application.get_env(TdCore.Search.Cluster, [])
    |> Keyword.fetch!(:aliases)
    |> Map.keys()
    |> Enum.map(&Supervisor.child_spec({TdCore.Search.IndexWorkerImpl, &1}, id: &1))
  end

  def index_document(index, document) do
    GenServer.call(index, {:index_document, document})
  end

  def index_documents_batch(index, documents) do
    GenServer.call(index, {:index_documents_batch, documents})
  end

  ## EventStream.Consumer Callbacks
  def consume(events) do
    index_scope = get_index_template_scope()

    Enum.map(events, fn
      %{event: "template_updated", scope: scope} ->
        Map.get(index_scope, String.to_atom(scope))

      _ ->
        nil
    end)
    |> Enum.reject(&is_nil(&1))
    |> Enum.uniq()
    |> Enum.each(&reindex(&1, :all))

    :ok
  end

  ## GenServer Callbacks

  @impl GenServer
  def init(index) do
    Logger.info("Running IndexWorker for index #{index}")
    {:ok, index}
  end

  @impl GenServer
  def handle_cast({:reindex, ids}, index) do
    Logger.info("Started indexing for #{index}")

    Timer.time(
      fn -> Indexer.reindex(index, ids) end,
      fn millis, _ -> Logger.info("#{index} indexed in #{millis}ms") end
    )

    {:noreply, index}
  end

  @impl GenServer
  def handle_cast({:put_embeddings, ids}, index) do
    Logger.info("Started embeddings update for #{index}")

    Timer.time(
      fn -> Indexer.put_embeddings(index, ids) end,
      fn millis, _ -> Logger.info("#{index} embeddings put in #{millis} ms") end
    )

    {:noreply, index}
  end

  @impl GenServer
  def handle_cast({:refresh_links, ids}, index) do
    :ok = Indexer.refresh_links(index, ids)

    {:noreply, index}
  end

  @impl GenServer
  def handle_cast({:delete, ids_or_tuple}, index) do
    Timer.time(
      fn -> Indexer.delete(index, ids_or_tuple) end,
      fn millis, _ -> Logger.info("#{index} deleted in #{millis}ms") end
    )

    {:noreply, index}
  end

  @impl GenServer
  def handle_call({:index_document, document}, _from, index) do
    Logger.info("Indexing document for #{index}")

    response =
      Timer.time(
        fn -> Indexer.index_document(index, document) end,
        fn millis, _ -> Logger.info("#{index} document indexed in #{millis}ms") end
      )

    {:reply, response, index}
  end

  @impl GenServer
  def handle_call({:index_documents_batch, documents}, _from, index) do
    Logger.info("Indexing #{length(documents)} documents for #{index}")

    response =
      Timer.time(
        fn -> Indexer.index_documents_batch(index, documents) end,
        fn millis, _ -> Logger.info("#{index} batch indexed in #{millis}ms") end
      )

    {:reply, response, index}
  end

  @impl GenServer
  def handle_call({:delete_index_documents_by_query, query}, _from, index) do
    response =
      Timer.time(
        fn -> Indexer.delete_index_documents_by_query(index, query) end,
        fn millis, _ -> Logger.info("#{index} documents deleted in #{millis}ms") end
      )

    {:reply, response, index}
  end

  defp get_indexes(module) do
    module
    |> Application.get_env(TdCore.Search.Cluster, [])
    |> Keyword.fetch!(:indexes)
  end

  defp get_index_template_scope do
    :td_core
    |> get_indexes()
    |> Map.new(fn {index, resource} ->
      {Keyword.get(resource, :template_scope), index}
    end)
  end
end
