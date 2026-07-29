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

Variables read directly by td-core at runtime:

- `ES_BULK_TOOK_LOG`: when set to `1`, `true`, or `yes` (case-insensitive), logs
  Elasticsearch `took` per successful bulk page. Default: off.

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/td_core>.
