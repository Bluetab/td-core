defmodule TdCore.Search.IndexWorker do
  @moduledoc """
  GenServer to run reindex task
  """

  use GenServer

  alias TdCore.Search.Indexer
  alias TdCore.Search.MockIndexWorker
  alias TdCore.Utils.Timer

  require Logger

  @cluster_config Application.compile_env(:td_core, TdCore.Search.Cluster, [])

  ## Public API that maybe uses test

  def start_link(index) do
    case Application.get_env(:td_core, :env) do
      :test -> MockIndexWorker.start_link(index)
      _ -> GenServer.start_link(__MODULE__, index, name: index)
    end
  end

  def reindex(index, ids) do
    case Application.get_env(:td_core, :env) do
      :test -> MockIndexWorker.reindex(index, ids)
      _ -> GenServer.cast(index, {:reindex, ids})
    end
  end

  def delete(index, ids) do
    case Application.get_env(:td_core, :env) do
      :test -> MockIndexWorker.delete(index, ids)
      _ -> GenServer.call(index, {:delete, ids})
    end
  end

  def get_index_workers do
    @cluster_config
    |> Keyword.fetch!(:aliases)
    |> Map.keys()
    |> Enum.map(&{TdCore.Search.IndexWorker, &1})
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
  def handle_call({:delete, ids}, _from, index) do
    reply =
      Timer.time(
        fn -> Indexer.delete(index, ids) end,
        fn millis, _ -> Logger.info("Rules deleted in #{millis}ms") end
      )

    {:reply, reply, index}
  end
end
