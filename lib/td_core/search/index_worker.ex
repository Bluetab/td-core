defmodule TdCore.Search.IndexWorker do
  @moduledoc """
  GenServer to run reindex task
  """

  @behaviour TdCache.EventStream.Consumer

  use GenServer

  alias TdCore.Search.Indexer
  alias TdCore.Search.MockIndexWorker
  alias TdCore.Utils.Timer

  require Logger

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
    :td_core
    |> Application.get_env(TdCore.Search.Cluster, [])
    |> Keyword.fetch!(:aliases)
    |> Map.keys()
    |> Enum.map(&Supervisor.child_spec({TdCore.Search.IndexWorker, &1}, id: &1))
  end

  ## EventStream.Consumer Callbacks

  @impl TdCache.EventStream.Consumer
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
  def handle_call({:delete, ids}, _from, index) do
    reply =
      Timer.time(
        fn -> Indexer.delete(index, ids) end,
        fn millis, _ -> Logger.info("Rules deleted in #{millis}ms") end
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
      {Map.get(resource, :template_scope), index}
    end)
    |> Map.new()
  end
end
