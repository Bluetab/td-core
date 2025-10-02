defmodule TdCore.Search.IndexWorkerMock do
  @moduledoc false

  use Agent

  def start_link(_) do
    Agent.start_link(fn -> [] end, name: __MODULE__)
  end

  def calls, do: Agent.get(__MODULE__, &Enum.reverse(&1))

  def clear, do: Agent.update(__MODULE__, fn _ -> [] end)

  def reindex(index, ids) do
    Agent.update(__MODULE__, &[{:reindex, index, ids} | &1])
  end

  def delete(index, ids) do
    Agent.update(__MODULE__, &[{:delete, index, ids} | &1])
  end

  def consume(_events), do: :ok

  def put_embeddings(index, ids) do
    Agent.update(__MODULE__, &[{:put_embeddings, index, ids} | &1])
  end

  def get_index_workers do
    [TdCore.Search.IndexWorkerMock]
  end

  def index_document(index, document) do
    Agent.update(__MODULE__, &[{:index_document, index, document} | &1])
  end

  def index_documents_batch(index, documents) do
    Agent.update(__MODULE__, &[{:index_documents_batch, index, documents} | &1])
  end
end
