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
    GenServer.call(index, {:delete, ids_or_tuple})
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
  def handle_call({:delete, ids_or_tuple}, _from, index) do
    reply =
      Timer.time(
        fn -> Indexer.delete(index, ids_or_tuple) end,
        fn millis, _ -> Logger.info("#{index} deleted in #{millis}ms") end
      )

    {:reply, reply, index}
  end

  defp get_indexes(module) do
    module
    |> Application.get_env(TdCore.Search.Cluster, [])
    |> Keyword.fetch!(:indexes)
  end

  defp get_index_template_scope do
    :td_core
    |> get_indexes()
    |> Enum.map(fn {index, resource} ->
      {Keyword.get(resource, :template_scope), index}
    end)
    |> Map.new()
  end
end
