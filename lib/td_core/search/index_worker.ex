defmodule TdCore.Search.IndexWorker do

  @behaviour TdCore.Search.IndexWorkerBehaviour
  @behaviour TdCache.EventStream.Consumer

  @worker Application.compile_env(:td_core, __MODULE__, TdCore.Search.IndexWorkerImpl)

  defdelegate start_link(index), to: @worker
  defdelegate calls, to: @worker
  defdelegate clear, to: @worker
  defdelegate consume(events), to: @worker
  defdelegate delete(index, ids), to: @worker
  defdelegate get_index_workers, to: @worker
  defdelegate reindex(index, ids), to: @worker
end
