defmodule TdCore.Search.IndexWorker do
  @moduledoc """
  Behaviour for index tasks
  """

  @type index :: atom()
  @type ids :: list(number())

  @callback start_link(index) :: {:ok, any()} | {:error, any()}
  @callback reindex(index, ids) :: :ok
  @callback delete(index, ids) :: term()
  @callback get_index_workers() :: list()

  def start_link(index),
    do: impl().start_link(index)

  def reindex(index, ids),
    do: impl().reindex(index, ids)

  def delete(index, ids),
    do: impl().delete(index, ids)

  def get_index_workers,
    do: impl().get_index_workers()

  defp impl() do
    if Application.get_env(:td_core, :env) == :test do
      TdCore.Search.IndexWorkerMock
    else
      TdCore.Search.IndexWorkerImpl
    end
  end
end
