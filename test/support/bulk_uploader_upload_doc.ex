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
end
