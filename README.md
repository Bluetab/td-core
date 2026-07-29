# TdCore

Truedats library for core functionality

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `td_core` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:td_core, "~> 0.1.0"}
  ]
end
```

## Environment variables

Most Elasticsearch cluster options (`reindex_concurrency`, `recv_timeout`,
`delete_existing_index`, forcemerge, aliases, etc.) are **not** read from the
environment by td-core. The host application (e.g. `td-dd`) maps env vars in
`config/runtime.exs` into `config :td_core, TdCore.Search.Cluster`. See that
service's README for the full list (`ES_REINDEX_CONCURRENCY`, `ES_RECV_TIMEOUT`,
`DELETE_EXISTING_INDEX`, …).

Per-page bulk indexing counts and Elasticsearch `took` are logged at `debug`
level, so they follow the service's configured `Logger` level rather than a
dedicated variable.

### Post hot-swap forcemerge

After a hot swap, td-core refreshes the new index and then forcemerges it using
`:forcemerge_options`. Whether the forcemerge runs is an infrastructure
decision, configured per service:

```elixir
config :td_core, TdCore.Search.Cluster,
  skip_forcemerge: System.get_env("ES_SKIP_FORCEMERGE", "false") |> String.to_atom(),
  forcemerge_options: [
    wait_for_completion: System.get_env("ES_WAIT_FOR_COMPLETION", "nil") |> String.to_atom(),
    max_num_segments: System.get_env("ES_MAX_NUM_SEGMENTS", "5") |> String.to_integer()
  ]
```

`skip_forcemerge` defaults to `false`, so the forcemerge runs with the
configured `wait_for_completion` and `max_num_segments` unless a service opts
out. It can also be passed per call as `Indexer.refresh(cluster, name,
skip_forcemerge: true)`.

The `_refresh` request is capped by `:refresh_recv_timeout` (default `5_000` ms)
so it does not inherit `ES_RECV_TIMEOUT`, which was adding roughly two minutes of
hot-swap wall time after the last bulk page. Set it to `nil` to fall back to the
cluster default. The `_forcemerge` request is not capped: it legitimately runs
for minutes, and services that want it to return immediately should set
`wait_for_completion: false` in `:forcemerge_options`.

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/td_core>.
