import Config

config :td_core, TdCore.Search.Cluster,
  api: ElasticsearchMock,
  url: "http://none",
  aliases: %{test_alias: "string_test_alias"}

config :td_core, TdCore.Search.IndexWorker, TdCore.Search.IndexWorkerMock
config :td_cluster, TdCluster.ClusterHandler, MockClusterHandler

config :td_cache, redis_host: "redis", port: 6380
