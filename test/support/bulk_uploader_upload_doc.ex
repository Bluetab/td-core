defmodule TdCore.Search.BulkUploaderUploadDoc do
  @moduledoc false
  defstruct [:id]
end

defimpl Elasticsearch.Document, for: TdCore.Search.BulkUploaderUploadDoc do
  def id(%TdCore.Search.BulkUploaderUploadDoc{id: id}), do: id
  def routing(_), do: false
  def encode(%TdCore.Search.BulkUploaderUploadDoc{id: id}), do: %{id: id}
end

defmodule TdCore.Search.BulkUploaderUploadStore do
  @moduledoc false

  alias TdCore.Search.BulkUploaderUploadDoc

  def stream(_source) do
    Stream.map(1..2, fn id -> %BulkUploaderUploadDoc{id: id} end)
  end

  def transaction(fun), do: fun.()
end

defmodule TdCore.Search.BulkUploaderTxnRequiredStore do
  @moduledoc false

  alias TdCore.Search.BulkUploaderUploadDoc

  @txn_key :bulk_uploader_txn_required

  def stream(_source) do
    Stream.map(1..2, fn id ->
      unless Process.get(@txn_key) do
        raise "cannot reduce stream outside of transaction"
      end

      %BulkUploaderUploadDoc{id: id}
    end)
  end

  def transaction(fun) do
    Process.put(@txn_key, true)

    try do
      fun.()
    after
      Process.delete(@txn_key)
    end
  end
end
