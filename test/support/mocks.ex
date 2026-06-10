defmodule TdCore.XLSX.Test.BulkLoadItemBehaviour do
  @moduledoc "Behaviour to let tests stub a single bulk_load_item/3 result via Mox"
  @callback bulk_load_item(struct(), map(), map()) :: term()
end

Mox.defmock(MockClusterHandler, for: TdCluster.ClusterHandler)
Mox.defmock(ElasticsearchMock, for: Elasticsearch.API)
Mox.defmock(MockBulkLoadItem, for: TdCore.XLSX.Test.BulkLoadItemBehaviour)
