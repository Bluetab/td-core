defmodule TdCore.Search.BulkUploader do
  @moduledoc """
  Parallel bulk upload helpers for Elasticsearch indexing.

  Transport errors and HTTP 429, 502, 503 and 504 retry the bulk page, lines
  rejected with 429 are resent, and a version conflict on a retried create
  counts as success. `:bulk_max_retries` (default 5) and `:bulk_retry_base_ms`
  (default 1_000) are read from `TdCore.Search.Cluster`.
  """

  require Logger

  alias Elasticsearch.Cluster.Config
  alias Elasticsearch.Index.Bulk
  alias TdCore.Search.Indexer
  alias TdCore.Search.PhaseProfiler

  @max_backoff_ms 30_000
  @retryable_statuses [429, 502, 503, 504]
  @version_conflict "version_conflict_engine_exception"

  @doc """
  Uploads all data from the configured store to `index_name`, posting bulk
  pages in parallel when `reindex_concurrency` is greater than 1.

  Consumes `store.stream/1` inside `store.transaction/1`, matching
  `Elasticsearch.Index.Bulk.upload/4`, so stores that use `Repo.stream/1`
  (e.g. implementations) stay inside a DB transaction.
  """
  @spec upload(Config.t(), String.t(), map(), list()) :: :ok | {:error, list()}
  def upload(_cluster, _index_name, %{sources: []}, []), do: :ok
  def upload(_cluster, _index_name, %{sources: []}, errors), do: {:error, errors}

  def upload(
        cluster,
        index_name,
        %{store: store, sources: [source | tail]} = index_config,
        errors
      ) do
    config = Config.get(cluster)
    bulk_page_size = index_config[:bulk_page_size] || 5000
    bulk_wait_interval = index_config[:bulk_wait_interval] || 0
    action = index_config[:bulk_action] || "create"
    concurrency = index_config[:reindex_concurrency] || reindex_concurrency()
    retry = bulk_retry_config()

    errors =
      store.transaction(fn ->
        source
        |> store.stream()
        |> Stream.map(&Bulk.encode!(config, &1, index_name, action))
        |> Stream.chunk_every(bulk_page_size)
        |> Stream.intersperse(bulk_wait_interval)
        |> post_bulk_pages(config, index_name, concurrency, action, retry)
        |> Enum.reduce(errors, fn response, acc ->
          record_bulk_response(index_name, response, acc, action)
        end)
      end)

    upload(config, index_name, %{index_config | sources: tail}, errors)
  end

  @doc """
  Posts pre-built bulk request bodies to Elasticsearch.
  """
  @spec post_bulk_bodies(Enumerable.t(), atom(), String.t(), pos_integer()) :: Enumerable.t()
  def post_bulk_bodies(bodies, cluster, path, concurrency) when concurrency <= 1 do
    retry = bulk_retry_config()
    Stream.map(bodies, &post_bulk_body(cluster, path, &1, retry))
  end

  def post_bulk_bodies(bodies, cluster, path, concurrency) do
    retry = bulk_retry_config()

    bodies
    |> Task.async_stream(
      &post_bulk_body(cluster, path, &1, retry),
      max_concurrency: concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Stream.map(fn
      {:ok, response} -> response
      {:exit, reason} -> {:error, reason}
    end)
  end

  defp post_bulk_body(cluster, path, body, retry, used \\ 0) do
    response =
      PhaseProfiler.phase_time(:bulk_es, fn ->
        Elasticsearch.post(cluster, path, body)
      end)

    maybe_retry_body(response, cluster, path, body, retry, used)
  end

  defp maybe_retry_body({:error, _} = response, cluster, path, body, retry, used) do
    if request_retryable?(response) and used < retry.max do
      n = used + 1
      retry_after(path, n, retry.max, request_cause(response), retry.base_ms)
      post_bulk_body(cluster, path, body, retry, n)
    else
      response
    end
  end

  defp maybe_retry_body(response, _cluster, _path, _body, _retry, _used), do: response

  defp post_bulk_pages(pages, config, index_name, concurrency, action, retry)
       when concurrency <= 1 do
    Stream.map(pages, &put_bulk_page(config, index_name, &1, action, retry))
  end

  defp post_bulk_pages(pages, config, index_name, concurrency, action, retry) do
    pages
    |> Task.async_stream(
      &put_bulk_page(config, index_name, &1, action, retry),
      max_concurrency: concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Stream.map(fn
      {:ok, response} -> response
      {:exit, reason} -> {:error, reason}
    end)
  end

  defp put_bulk_page(_config, _index_name, wait_interval, _action, _retry)
       when is_integer(wait_interval) do
    Logger.debug("Pausing #{wait_interval}ms between bulk pages")
    :timer.sleep(wait_interval)
    :ok
  end

  defp put_bulk_page(config, index_name, items, action, retry) when is_list(items) do
    lines = List.to_tuple(items)

    ctx = %{
      config: config,
      index_name: index_name,
      lines: lines,
      action: action,
      retry: retry
    }

    state = %{
      results: %{},
      took: 0,
      pending: Enum.to_list(0..(tuple_size(lines) - 1)//1),
      retried: MapSet.new()
    }

    send_pending(ctx, 0, state)
  end

  defp send_pending(ctx, used, state) do
    response =
      PhaseProfiler.phase_time(:bulk_es, fn ->
        Elasticsearch.put(
          ctx.config,
          "/#{ctx.index_name}/_bulk",
          page_body(ctx.lines, state.pending)
        )
      end)

    dispatch_page(response, ctx, used, state)
  end

  defp dispatch_page({:ok, %{"errors" => false} = body}, ctx, _used, state) do
    state
    |> apply_success(body, ctx.action)
    |> finish_page(ctx)
  end

  defp dispatch_page({:ok, %{"errors" => true, "items" => _} = body}, ctx, used, state) do
    state
    |> apply_item_errors(body, ctx.action)
    |> continue_rejected(ctx, used)
  end

  defp dispatch_page({:error, _} = response, ctx, used, state) do
    maybe_retry_page(response, ctx, used, state)
  end

  defp dispatch_page(response, _ctx, _used, _state), do: response

  defp maybe_retry_page(response, ctx, used, state) do
    if request_retryable?(response) and used < ctx.retry.max do
      n = used + 1
      retry_after(ctx.index_name, n, ctx.retry.max, request_cause(response), ctx.retry.base_ms)
      send_pending(ctx, n, mark_retried(state))
    else
      response
    end
  end

  defp continue_rejected(%{pending: []} = state, ctx, _used), do: finish_page(state, ctx)

  defp continue_rejected(state, ctx, used) when used < ctx.retry.max do
    n = used + 1
    cause = "#{length(state.pending)} documents rejected with 429"
    retry_after(ctx.index_name, n, ctx.retry.max, cause, ctx.retry.base_ms)
    send_pending(ctx, n, mark_retried(state))
  end

  defp continue_rejected(state, ctx, _used), do: finish_page(state, ctx)

  defp apply_success(state, body, action) do
    items =
      body
      |> Map.get("items", [])
      |> Stream.concat(Stream.repeatedly(fn -> nil end))

    results =
      state.pending
      |> Enum.zip(items)
      |> Enum.reduce(state.results, fn {index, item}, acc ->
        Map.put(acc, index, success_item(item, acc, index, action))
      end)

    %{state | results: results, took: state.took + took_ms(body), pending: []}
  end

  defp apply_item_errors(state, body, action) do
    {results, rejected} =
      state.pending
      |> Enum.zip(body["items"])
      |> Enum.reduce({state.results, []}, fn {index, item}, {results, rejected} ->
        item = maybe_accept_conflict(item, action, retried_index?(state, index))
        store_item(results, rejected, index, item, action)
      end)

    %{
      state
      | results: results,
        took: state.took + took_ms(body),
        pending: Enum.reverse(rejected)
    }
  end

  defp store_item(results, rejected, index, item, action) do
    results = Map.put(results, index, item)

    if rejected_document?(item, action) do
      {results, [index | rejected]}
    else
      {results, rejected}
    end
  end

  defp finish_page(state, ctx) do
    items = Enum.map(0..(tuple_size(ctx.lines) - 1)//1, &Map.fetch!(state.results, &1))

    {:ok,
     %{
       "errors" => Enum.any?(items, &item_has_error?(&1, ctx.action)),
       "items" => items,
       "took" => state.took
     }}
  end

  defp success_item(nil, results, index, action) do
    case Map.get(results, index) do
      nil -> %{action => %{"status" => 201}}
      previous -> clear_error(previous, action)
    end
  end

  defp success_item(item, _results, _index, _action), do: item

  defp clear_error(item, action) do
    entry =
      item
      |> Map.get(action, %{})
      |> Map.delete("error")
      |> Map.put("status", 201)

    Map.put(item, action, entry)
  end

  defp maybe_accept_conflict(item, "create", true) do
    if version_conflict?(item, "create") do
      Map.update!(item, "create", &Map.delete(&1, "error"))
    else
      item
    end
  end

  defp maybe_accept_conflict(item, _action, _retried?), do: item

  defp version_conflict?(item, action) do
    get_in(item, [action, "status"]) == 409 &&
      get_in(item, [action, "error", "type"]) == @version_conflict
  end

  defp rejected_document?(item, action), do: get_in(item, [action, "status"]) == 429

  defp item_has_error?(item, action), do: get_in(item, [action, "error"]) != nil

  defp retried_index?(state, index), do: MapSet.member?(state.retried, index)

  defp mark_retried(state) do
    %{state | retried: MapSet.union(state.retried, MapSet.new(state.pending))}
  end

  defp page_body(lines, indices) do
    Enum.map_join(indices, &elem(lines, &1))
  end

  defp took_ms(%{"took" => took}) when is_integer(took), do: took
  defp took_ms(_body), do: 0

  defp request_retryable?({:error, %HTTPoison.Error{}}), do: true

  defp request_retryable?({:error, %Elasticsearch.Exception{status: status}})
       when status in @retryable_statuses,
       do: true

  defp request_retryable?(_response), do: false

  defp request_cause({:error, %HTTPoison.Error{reason: reason}}), do: inspect(reason)

  defp request_cause({:error, %Elasticsearch.Exception{status: status, type: type}}) do
    "status #{status} #{type}"
  end

  defp retry_after(target, n, max, cause, base_ms) do
    Logger.warning("#{target}: bulk retry #{n}/#{max} after #{cause}")
    sleep_before_retry(base_ms, n)
  end

  defp sleep_before_retry(0, _n), do: :ok

  defp sleep_before_retry(base_ms, n) do
    case retry_delay(base_ms, n) do
      delay when delay > 0 -> Process.sleep(delay)
      _delay -> :ok
    end
  end

  defp retry_delay(base_ms, n) do
    backoff = min(base_ms * Integer.pow(2, n - 1), @max_backoff_ms)
    backoff + :rand.uniform(base_ms + 1) - 1
  end

  defp bulk_retry_config do
    config = cluster_config()

    %{
      max: Keyword.get(config, :bulk_max_retries, 5),
      base_ms: Keyword.get(config, :bulk_retry_base_ms, 1_000)
    }
  end

  @doc false
  def record_bulk_response(index_name, response, errors, action) do
    maybe_log_bulk_post(index_name, response, action)
    collect_errors(response, errors, action)
  end

  defp maybe_log_bulk_post(index_name, {:ok, _} = response, action) do
    Indexer.log_bulk_post(index_name, response, action)
  end

  defp maybe_log_bulk_post(index_name, {:error, _} = response, action) do
    Indexer.log_bulk_post(index_name, response, action)
  end

  defp maybe_log_bulk_post(_index_name, _response, _action), do: :ok

  defp collect_errors({:ok, %{"errors" => true} = response}, errors, action) do
    new_errors =
      response["items"]
      |> Enum.filter(&(&1[action]["error"] != nil))
      |> Enum.map(& &1[action])
      |> Enum.map(&Elasticsearch.Exception.exception(response: &1))

    new_errors ++ errors
  end

  defp collect_errors({:error, error}, errors, _action), do: [error | errors]
  defp collect_errors(_response, errors, _action), do: errors

  defp reindex_concurrency do
    cluster_config()
    |> Keyword.get(:reindex_concurrency, System.schedulers_online())
  end

  defp cluster_config do
    Application.get_env(:td_core, TdCore.Search.Cluster, [])
  end
end
