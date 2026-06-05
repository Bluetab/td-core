# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`td_core` is an Elixir **library** (not a runnable service) providing core functionality shared across Truedat backend services (td-dd, td-bg, td-ai, etc.). Consuming services add it as a git dependency and implement its protocols/behaviours to plug in their own data. It depends on three other internal Truedat libraries pinned by git tag in `mix.exs`: `td_cluster` (inter-service RPC), `td_cache` (Redis-backed shared cache), and `td_df_lib` (dynamic content / template handling).

## Commands

```bash
mix deps.get                              # fetch dependencies
mix test                                  # run all tests
mix test test/td_core/search/query_test.exs        # single file
mix test test/td_core/search/query_test.exs:42     # single test at line 42
mix format                                # format (config in .formatter.exs)
mix credo                                 # static analysis / linting
```

There is no DB and no server to boot — the test suite runs against mocks, not live Elasticsearch/Redis.

## Architecture

The library is organized under `lib/td_core/` into four areas: **search**, **auth**, **xlsx**, **i18n**, plus `util/`.

### Search (the bulk of the library)

Wraps the `elasticsearch` Elixir client (a Bluetab fork pinned in `mix.exs`) to give Truedat services a uniform indexing/search layer.

- **`Search.Cluster`** — `Elasticsearch.Cluster` GenServer. Its config (URL, `aliases` map of index-name → alias, index settings, aggregation sizes) is supplied **by the consuming service**, not by this library. `config/config.exs` here only configures the test environment.
- **`ElasticDocumentProtocol`** — protocol with `mappings/1`, `aggregations/1`, `query_data/1`. Consuming services implement it for their own document structs. **`ElasticDocument`** provides the `__using__` macro and shared helpers that those implementations build on, including dynamic field mapping/aggregation derived from `td_df_lib` templates (`get_dynamic_mappings`, `merge_dynamic_aggregations`, `dynamic_search_fields`) and i18n locale handling.
- **`Search.Indexer`** — index lifecycle: `reindex/2` (full rebuild via hot-swap of `<alias>-*` indices, or partial by ids), bulk document indexing, and `Store`-backed streaming. Uses `td_cluster`'s `Tasks` to log progress.
- **`Search.IndexWorker`** — `defdelegate` facade over a swappable implementation chosen at compile time via `Application.compile_env(:td_core, TdCore.Search.IndexWorker, ...)`. Real impl is `IndexWorkerImpl` (a GenServer, one per index alias, that also consumes `td_cache` EventStream events like `template_updated`); tests use `IndexWorkerMock`.
- **`Search.Query`** / **`Search.Filters`** — compose Elasticsearch bool queries from params. **`Search.Permissions`** maps a session's permissions (from `TdCache.Permissions`) to domain-scoped search filters; admin/service roles get `:all`.

### Auth

Guardian-based JWT auth (`Auth.Guardian`, `Auth.Claims`), Plug pipelines (`pipeline/secure.ex`, `pipeline/unsecure.ex`) and plugs (`current_resource`, `session_exists`). Token subject encodes `{id, user_name}`; audience is `"truedat"`.

### XLSX bulk load

Generic spreadsheet ingestion used by services for bulk create/update.

- **`XLSX.UploadWorker.run/1`** — entry point. Resolves a scope string to a registered implementation via `Application.get_env(:td_core, :bulk_load_implementations)`, reads the file, runs the load, and reports status through `td_cluster`'s `UploadJobs`.
- **`XLSX.BulkLoadProtocol`** — protocol the consuming service implements per scope (`get_opts`, `sheets_to_templates`, `bulk_load_item`, `on_complete`). **`XLSX.BulkLoad`** drives the generic flow: header translation/validation, parsing rows into `df_content` (each cell wrapped as `%{"value" => v, "origin" => "file"}`), then per-item dispatch tallying created/updated/unchanged/error counts.

## Conventions

- **Pluggable seams everywhere.** This library intentionally inverts control: behaviours (`IndexWorkerBehaviour`), protocols (`ElasticDocumentProtocol`, `BulkLoadProtocol`), and config-driven module selection let each consuming service inject its own data and indices. When adding functionality, follow this pattern rather than hardcoding service specifics.
- **Dynamic content fields** (`df_content` and similar `jsonb` maps) follow `td_df_lib` conventions — the `{value, origin}` wrapper and Markdown (not Slate) enriched text. Reuse `TdDfLib` helpers; see the global notes in `~/.claude/CLAUDE.md`.
- **Testing** uses Mox. `config/config.exs` wires the mocks: `ElasticsearchMock` (for `Elasticsearch.API`), `MockClusterHandler` (for `td_cluster`), and `IndexWorkerMock`. Mock definitions live in `test/support/mocks.ex`; helpers in `test/support/` (`cache_helpers.ex`, `search_helpers.ex`). `test/support/` and `lib/td_core/test_support/` are compiled in `:test` env and provide reusable helpers for downstream services too.
- Version in `mix.exs` and `CHANGELOG.md` track Truedat-wide release numbers (currently 8.x); bump both when releasing.
