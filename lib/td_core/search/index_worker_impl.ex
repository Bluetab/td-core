defmodule TdCore.Search.IndexWorkerImpl do
  @moduledoc """
  GenServer to run reindex task
  """

  use GenServer

  alias TdCore.Search.Cluster
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

    events
    |> Enum.map(fn
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

    state =
      if buffer_partials?(index) do
        %{index: index, running: nil, pending: []}
      else
        index
      end

    {:ok, state}
  end

  @impl GenServer
  def handle_cast({:reindex, ids}, index) when is_atom(index) do
    Logger.info("Started indexing for #{index}")

    Timer.time(
      fn -> Indexer.reindex(index, ids) end,
      fn millis, _ -> Logger.info("#{index} indexed in #{millis}ms") end
    )

    {:noreply, index}
  end

  @impl GenServer
  def handle_cast({:put_embeddings, ids}, index) when is_atom(index) do
    Logger.info("Started embeddings update for #{index}")

    Timer.time(
      fn -> Indexer.put_embeddings(index, ids) end,
      fn millis, _ -> Logger.info("#{index} embeddings put in #{millis} ms") end
    )

    {:noreply, index}
  end

  @impl GenServer
  def handle_cast({:refresh_links, ids}, index) when is_atom(index) do
    :ok = Indexer.refresh_links(index, ids)

    {:noreply, index}
  end

  @impl GenServer
  def handle_cast({:delete, ids_or_tuple}, index) when is_atom(index) do
    Timer.time(
      fn -> Indexer.delete(index, ids_or_tuple) end,
      fn millis, _ -> Logger.info("#{index} deleted in #{millis}ms") end
    )

    {:noreply, index}
  end

  @impl GenServer
  def handle_cast(message, %{index: _} = state) do
    {:noreply, schedule(state, message)}
  end

  @impl GenServer
  def handle_call({:index_document, document}, _from, index) when is_atom(index) do
    Logger.info("Indexing document for #{index}")

    response =
      Timer.time(
        fn -> Indexer.index_document(index, document) end,
        fn millis, _ -> Logger.info("#{index} document indexed in #{millis}ms") end
      )

    {:reply, response, index}
  end

  @impl GenServer
  def handle_call({:index_documents_batch, documents}, _from, index) when is_atom(index) do
    Logger.info("Indexing #{length(documents)} documents for #{index}")

    response =
      Timer.time(
        fn -> Indexer.index_documents_batch(index, documents) end,
        fn millis, _ -> Logger.info("#{index} batch indexed in #{millis}ms") end
      )

    {:reply, response, index}
  end

  @impl GenServer
  def handle_call({:delete_index_documents_by_query, query}, _from, index) when is_atom(index) do
    response =
      Timer.time(
        fn -> Indexer.delete_index_documents_by_query(index, query) end,
        fn millis, _ -> Logger.info("#{index} documents deleted in #{millis}ms") end
      )

    {:reply, response, index}
  end

  @impl GenServer
  def handle_call(message, from, %{index: _} = state) do
    {:noreply, schedule(state, {:reply, from, message})}
  end

  @impl GenServer
  def handle_info({ref, _result}, %{running: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, start_next(%{state | running: nil})}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{running: ref} = state) do
    {:noreply, start_next(%{state | running: nil})}
  end

  defp buffer_partials?(index) do
    Cluster.setting(index, :buffer_partials_during_reindex) == true
  rescue
    _ -> false
  end

  defp schedule(%{running: nil} = state, message) do
    state
    |> enqueue(message)
    |> start_next()
  end

  defp schedule(state, message), do: enqueue(state, message)

  defp enqueue(state, {:reindex, :all}) do
    %{state | pending: [{:reindex, :all}]}
  end

  defp enqueue(%{pending: pending} = state, {:reindex, ids}) when is_list(ids) do
    %{state | pending: coalesce(pending, {:reindex, ids})}
  end

  defp enqueue(%{pending: pending} = state, {:delete, ids}) when is_list(ids) do
    %{state | pending: coalesce(pending, {:delete, ids})}
  end

  defp enqueue(%{pending: pending} = state, message) do
    %{state | pending: pending ++ [message]}
  end

  defp coalesce(pending, {kind, ids}) do
    case List.last(pending) do
      {^kind, previous} when is_list(previous) ->
        List.replace_at(pending, -1, {kind, Enum.uniq(previous ++ ids)})

      _ ->
        pending ++ [{kind, ids}]
    end
  end

  defp start_next(%{pending: []} = state), do: %{state | running: nil}

  defp start_next(%{index: index, pending: [op | rest]} = state) do
    %Task{ref: ref} = Task.async(fn -> execute(index, op) end)
    %{state | pending: rest, running: ref}
  end

  defp execute(index, op) do
    case Application.get_env(:td_core, :index_worker_runner) do
      runner when is_function(runner, 2) -> runner.(index, op)
      _ -> execute_op(index, op)
    end
  end

  defp execute_op(index, {:reindex, ids}) do
    Logger.info("Started indexing for #{index}")

    Timer.time(
      fn -> Indexer.reindex(index, ids) end,
      fn millis, _ -> Logger.info("#{index} indexed in #{millis}ms") end
    )
  end

  defp execute_op(index, {:put_embeddings, ids}) do
    Logger.info("Started embeddings update for #{index}")

    Timer.time(
      fn -> Indexer.put_embeddings(index, ids) end,
      fn millis, _ -> Logger.info("#{index} embeddings put in #{millis} ms") end
    )
  end

  defp execute_op(index, {:refresh_links, ids}) do
    :ok = Indexer.refresh_links(index, ids)
  end

  defp execute_op(index, {:delete, ids_or_tuple}) do
    Timer.time(
      fn -> Indexer.delete(index, ids_or_tuple) end,
      fn millis, _ -> Logger.info("#{index} deleted in #{millis}ms") end
    )
  end

  defp execute_op(index, {:reply, from, {:index_document, document}}) do
    Logger.info("Indexing document for #{index}")

    response =
      Timer.time(
        fn -> Indexer.index_document(index, document) end,
        fn millis, _ -> Logger.info("#{index} document indexed in #{millis}ms") end
      )

    GenServer.reply(from, response)
  end

  defp execute_op(index, {:reply, from, {:index_documents_batch, documents}}) do
    Logger.info("Indexing #{length(documents)} documents for #{index}")

    response =
      Timer.time(
        fn -> Indexer.index_documents_batch(index, documents) end,
        fn millis, _ -> Logger.info("#{index} batch indexed in #{millis}ms") end
      )

    GenServer.reply(from, response)
  end

  defp execute_op(index, {:reply, from, {:delete_index_documents_by_query, query}}) do
    response =
      Timer.time(
        fn -> Indexer.delete_index_documents_by_query(index, query) end,
        fn millis, _ -> Logger.info("#{index} documents deleted in #{millis}ms") end
      )

    GenServer.reply(from, response)
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
