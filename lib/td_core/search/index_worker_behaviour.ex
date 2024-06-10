defmodule TdCore.Search.IndexWorkerBehaviour do
  @moduledoc """
  Behaviour for index tasks
  """
  @type index :: atom()
  @type ids :: list(number())

  @callback start_link(index) :: {:ok, any()} | {:error, any()}
  @callback reindex(index, ids) :: :ok
  @callback delete(index, ids) :: term()
  @callback get_index_workers() :: list()
end
