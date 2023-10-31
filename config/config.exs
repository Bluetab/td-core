import Config

config :td_core, :env, Mix.env()

config :td_core, TdCore.Search.Cluster,
  api: ElasticsearchMock,
  url: "http://none"
