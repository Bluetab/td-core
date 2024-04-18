defmodule TdCore.Search.Indexer do
  @moduledoc """
  Manages elasticsearch indices
  """

  require Logger

  alias Elasticsearch.Cluster.Config
  alias Elasticsearch.Index
  alias Elasticsearch.Index.Bulk
  alias TdCluster.Cluster.TdDd.Tasks
  alias TdCore.Search.Cluster
  alias TdCore.Search.ElasticDocumentProtocol

  def reindex(index, :all) do
    Tasks.log_start(index)

    alias_name = Cluster.alias_name(index)

    result =
      alias_name
      |> mappings_from_alias()
      |> Map.put(:index_patterns, "#{alias_name}-*")
      |> Jason.encode!()
      |> put_template(Cluster, alias_name)
      |> maybe_hot_swap(Cluster, alias_name)

    Tasks.log_end()
    result
  end

  @action "index"
  def reindex(index, ids) when is_list(ids) do
    alias_name = Cluster.alias_name(index)

    store = store_from_alias(alias_name)

    store.transaction(fn ->
      alias_name
      |> schema_from_alias()
      |> store.stream(ids)
      |> Stream.map(&Bulk.encode!(Cluster, &1, alias_name, @action))
      |> Stream.chunk_every(Cluster.setting(index, :bulk_page_size))
      |> Stream.map(&Enum.join(&1, ""))
      |> Stream.map(&Elasticsearch.post(Cluster, "/#{alias_name}/_doc/_bulk", &1))
      |> Stream.map(&log_bulk_post(alias_name, &1, @action))
      |> Stream.run()
    end)
  end

  def reindex(index, id), do: reindex(index, [id])

  defp store_from_alias(alias_name) do
    alias_atom = alias_to_atom(alias_name)

    Cluster
    |> Config.get()
    |> get_in([:indexes, alias_atom, :store])
  end

  defp schema_from_alias(alias_name) do
    alias_atom = alias_to_atom(alias_name)

    Cluster
    |> Config.get()
    |> get_in([:indexes, alias_atom, :sources])
    |> schema_from_sources()
  end

  defp mappings_from_alias(alias_name) do
    alias_name
    |> schema_from_alias()
    |> mappings_from_source()
  end

  defp schema_from_sources([source | _]), do: source
  defp schema_from_sources(_), do: nil

  defp mappings_from_source(nil), do: nil
  defp mappings_from_source(source), do: ElasticDocumentProtocol.mappings(struct(source))

  def delete(index, ids) do
    alias_name = Cluster.alias_name(index)
    Enum.map(ids, &Elasticsearch.delete_document(Cluster, &1, alias_name))
  end

  def list_indexes do
    {:ok, aliases} = Elasticsearch.get(Cluster, "_alias")

    alias_by_index =
      Enum.map(aliases, fn {key, value} ->
        {key, value["aliases"] |> Map.keys() |> List.first()}
      end)
      |> Enum.into(%{})

    {:ok, %{"indices" => indices}} = Elasticsearch.get(Cluster, "_stats/docs,store")

    Enum.map(indices, fn {key, value} ->
      %{
        "total" => %{
          "store" => %{"size_in_bytes" => size},
          "docs" => %{"count" => documents}
        }
      } = value

      %{
        key: key,
        alias: Map.get(alias_by_index, key),
        size: size,
        documents: documents
      }
    end)
  end

  def maybe_hot_swap({:ok, _put_template_result}, _cluster, alias_name) do
    Logger.info("Starting reindex using hot_swap...")
    hot_swap(alias_name)
  end

  def maybe_hot_swap({:error, _error} = put_template_error, _cluster, _alias_name) do
    Logger.warning("Index template update errors, will not reindex")
    put_template_error
  end

  # Modified from Elasticsearch.Index.hot_swap for better logging and
  # error handling
  def hot_swap(alias_name) do
    alias_name = alias_to_atom(alias_name)
    name = Index.build_name(alias_name)
    config = Config.get(Cluster)
    %{settings: settings} = index_config = config[:indexes][alias_name]

    with {:index_from_settings, :ok} <-
           {:index_from_settings, Index.create_from_settings(config, name, %{settings: settings})},
         {:bulk_upload, :ok} <- {:bulk_upload, Bulk.upload(config, name, index_config)},
         {:index_alias, :ok} <- {:index_alias, Index.alias(config, name, to_string(alias_name))},
         {:index_clean_starting, :ok} <-
           {:index_clean_starting, Index.clean_starting_with(config, to_string(alias_name), 2)},
         {:index_refresh, :ok} <- {:index_refresh, Index.refresh(config, name)} do
      Logger.info(
        "Hot swap successful, finished reindexing, pointing alias #{alias_name} -> #{name}"
      )

      {:ok, name}
    else
      {process_key, error} ->
        delete_index =
          Application.get_env(:td_core, TdCore.Search.Cluster)[:delete_existing_index]

        log_hot_swap_errors(name, process_key, error)

        delete_existing_index(name, alias_name, Cluster, delete_index)

        {:error, name}
    end
  end

  defp alias_to_atom(atom) when is_atom(atom), do: atom
  defp alias_to_atom(str) when is_binary(str), do: String.to_existing_atom(str)

  def put_template(template, cluster, name) do
    case Elasticsearch.put(cluster, "/_template/#{name}", template,
           params: %{"include_type_name" => "false"}
         ) do
      {:ok, result} = successful_update ->
        Logger.info("Index #{name} template update successful: #{inspect(result)}")
        successful_update

      {:error, %Elasticsearch.Exception{message: message}} = failed_update ->
        Logger.warning("Index #{name} template update failed: #{message}")
        failed_update
    end
  end

  def alias_exists?(cluster, name) do
    case Elasticsearch.head(cluster, "/_alias/#{name}") do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  def delete_existing_index(name, cluster \\ Cluster)

  def delete_existing_index(name, cluster) do
    [alias_name | _] = String.split(name, "-")
    delete_existing_index(name, alias_name, cluster, true)
  end

  def delete_existing_index(name, alias_name, cluster, false) do
    if alias_exists?(Cluster, alias_name) do
      delete_existing_index(name, alias_name, cluster, true)
    else
      Logger.info("The index #{name} has not been deleted")
      {:ok, :index_not_deleted}
    end
  end

  def delete_existing_index(name, _alias_name, cluster, _true) do
    Logger.warning("Removing index #{name}...")

    case Elasticsearch.delete(cluster, "/#{name}") do
      {:ok, result} = successful_deletion ->
        Logger.info("Successfully deleted index #{name}: #{inspect(result)}")
        successful_deletion

      {:error, %{status: 404} = not_found} ->
        Logger.warning("Index #{name} does not exist, nothing to delete.")
        {:ok, not_found}

      {:error, e} = failed_deletion when Kernel.is_exception(e) ->
        Logger.error("Failed to delete index #{name}, message: #{Exception.message(e)}")
        failed_deletion

      error ->
        Logger.error("Failed to delete index #{name}, message: #{inspect(error)}")
        error
    end
  end

  def log_bulk_post(index, {:ok, %{"errors" => false, "items" => items, "took" => took}}, _action) do
    Logger.info("#{index}: bulk indexed #{Enum.count(items)} documents (took=#{took})")
  end

  def log_bulk_post(index, {:ok, %{"errors" => true, "items" => items}}, action) do
    items
    |> Enum.filter(& &1[action]["error"])
    |> log_bulk_post_items_errors(index, action)
  end

  def log_bulk_post(index, {:error, error}, _action) do
    Logger.error("#{index}: bulk indexing encountered errors #{inspect(error)}")
  end

  def log_bulk_post(index, error, _action) do
    Logger.error("#{index}: bulk indexing encountered errors #{inspect(error)}")
  end

  def log_bulk_post_items_errors(errors, index, action) do
    errors
    |> Enum.map(&"#{info_document_id(&1, action)}: #{message(&1, action)}\n")
    |> Kernel.then(fn messages ->
      ["#{index}: bulk indexing encountered #{pluralize(errors)}:\n" | messages]
    end)
    |> Logger.error()
  end

  def log_hot_swap_errors(index, process_key, {:error, [_ | _] = exceptions}) do
    exceptions
    |> Enum.map(&"#{message(&1)}\n")
    |> Kernel.then(fn messages ->
      [
        "New index #{index} build finished in #{process_key} with #{pluralize(exceptions)}:\n"
        | messages
      ]
    end)
    |> Logger.error()
  end

  def log_hot_swap_errors(index, process_key, {:error, e}) do
    Logger.error(
      "New index #{index} build finished in #{process_key} 001 with an error:\n #{message(e)}"
    )
  end

  def log_hot_swap_errors(index, process_key, e) do
    Logger.error(
      "New index #{index} build finished in #{process_key} 002 with an error:\n #{message(e)}"
    )
  end

  def pluralize([_e]) do
    "an error"
  end

  def pluralize([_ | _] = exceptions) do
    "#{Enum.count(exceptions)} errors"
  end

  defp message(%Elasticsearch.Exception{} = e) do
    "(ES) #{info_document_id(e)}#{Exception.message(e)}"
  end

  defp message(e) when Kernel.is_exception(e) do
    "(KN) #{Exception.message(e)}"
  end

  defp message(e) do
    "#{inspect(e)}"
  end

  defp message(item, action) do
    "(EX) #{item[action]["error"]["reason"]}"
  end

  defp info_document_id(%Elasticsearch.Exception{raw: %{"_id" => id}}),
    do: "Document ID #{id}: "

  defp info_document_id(_), do: ""

  defp info_document_id(item, action) do
    "Document ID #{item[action]["_id"]}"
  end
end
