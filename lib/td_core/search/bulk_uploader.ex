defmodule TdCore.Search.BulkUploader do
  @moduledoc """
  Parallel bulk upload helpers for Elasticsearch indexing.
  """

  require Logger

  alias Elasticsearch.Cluster.Config
  alias Elasticsearch.Index.Bulk
  alias TdCore.Search.Indexer

  @doc """
  Uploads all data from the configured store to `index_name`, posting bulk
  pages in parallel when `reindex_concurrency` is greater than 1.
  """
  @spec upload(Config.t(), String.t(), map(), list()) :: :ok | {:error, list()}
  def upload(_cluster, _index_name, %{sources: []}, []), do: :ok
  def upload(_cluster, _index_name, %{sources: []}, errors), do: {:error, errors}

  def upload(cluster, index_name, %{store: store, sources: [source | tail]} = index_config, errors) do
    config = Config.get(cluster)
    bulk_page_size = index_config[:bulk_page_size] || 5000
    bulk_wait_interval = index_config[:bulk_wait_interval] || 0
    action = index_config[:bulk_action] || "create"
    concurrency = reindex_concurrency()

    errors =
      source
      |> store.stream()
      |> Stream.map(&Bulk.encode!(config, &1, index_name, action))
      |> Stream.chunk_every(bulk_page_size)
      |> Stream.intersperse(bulk_wait_interval)
      |> post_bulk_pages(config, index_name, concurrency)
      |> Enum.reduce(errors, fn response, acc ->
        record_bulk_response(index_name, response, acc, action)
      end)

    upload(config, index_name, %{index_config | sources: tail}, errors)
  end

  @doc """
  Posts pre-built bulk request bodies to Elasticsearch.
  """
  @spec post_bulk_bodies(Enumerable.t(), atom(), String.t(), pos_integer()) :: Enumerable.t()
  def post_bulk_bodies(bodies, cluster, path, concurrency) when concurrency <= 1 do
    Stream.map(bodies, &Elasticsearch.post(cluster, path, &1))
  end

  def post_bulk_bodies(bodies, cluster, path, concurrency) do
    bodies
    |> Task.async_stream(
      &Elasticsearch.post(cluster, path, &1),
      max_concurrency: concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Stream.map(fn
      {:ok, response} -> response
      {:exit, reason} -> {:error, reason}
    end)
  end

  defp post_bulk_pages(pages, config, index_name, concurrency) when concurrency <= 1 do
    Stream.map(pages, &put_bulk_page(config, index_name, &1))
  end

  defp post_bulk_pages(pages, config, index_name, concurrency) do
    pages
    |> Task.async_stream(
      &put_bulk_page(config, index_name, &1),
      max_concurrency: concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Stream.map(fn
      {:ok, response} -> response
      {:exit, reason} -> {:error, reason}
    end)
  end

  defp put_bulk_page(_config, _index_name, wait_interval) when is_integer(wait_interval) do
    Logger.debug("Pausing #{wait_interval}ms between bulk pages")
    :timer.sleep(wait_interval)
    :ok
  end

  defp put_bulk_page(config, index_name, items) when is_list(items) do
    Elasticsearch.put(config, "/#{index_name}/_bulk", Enum.join(items))
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
