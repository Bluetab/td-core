# Changelog

## [7.14.0] 2025-11-27

### Added

- [TD-7302]:
  - Index_type for elastic emmedings
  - Add elastic index for RAG functionality

## [7.12.1] 2025-12-03

### Changed

- [TD-7746] Update td-df-lib

## [7.12.0] 2025-10-28

### Added

- [TD-7340] Support for dynamic_table template field search capabilities

## [7.11.2] 2025-09-27

### Changed

- [TD-7617] Enhance stringify_keys function to handle DateTime

## [7.11.1] 2025-09-30

### Changed

- [TD-7401] Update td-cache, td-df-lib and td-cluster versions

## [7.11.0] 2025-09-24

### Added

- [TD-7301] Update td-cache and td-df-lib to add Link Approvals.

## [7.10.4] 2025-09-22

### Added

- # [TD-7446] Add support for filtering by multiple types in maybe_filter function

## [7.10.3] 2025-09-19

### Added

- [TD-7417]:
  - Pass config or options to forcemerge in refresh function:
    - Force merge can run async.
    - Tune max num of shards.

### Changed

- [TD-7417] Refresh is not a blocking operation.

## [7.10.2] 2025-09-16

### Changed

- [TD-7345] Update `td-cluster` version

## [7.10.1] 2025-09-15

### Changed

- [TD-7364] Embedding management supports inserting all embeddings or targeting specific ones by ID.

## [7.10.0] 2025-09-09

### Added

- [TD-7175] Upgrade td-cluster versions

## [7.8.1] 2025-07-22

### Added

- [TD-7327] Use the store to manage deletions in Elasticsearch.

## [7.8.0] 2025-07-03

### Added

- [TD-7231] Add search enhancements for quoted text query

## [7.7.1] 2025-06-16

### Changed

- [TD-7299] Update `elasticsearch-elixir` dependency for vulnerabilities

## [7.7.0] 2025-06-20

### Added

- [TD-7300] Update td-df-lib and td-cache lib to add Link origin

## [7.6.0] 2025-06-06

### Changed

- [TD-6468] Update td-df-lib to expand parsed fields in all active languages if translations

## [7.5.3] 2025-06-04

### Changed

- [TD-6219] Add a default clause to handle node-down responses when returning mappings for AI indices

## [7.5.2] 2025-05-22

### Added

- [TD-6219]:
  - Functions to fetch embedding mappings
  - Functions to put embeddings in elastic

## [7.5.1] 2025-04-14

### Added

- [TD-7053]:
  - Filter widget for multi-language ordering
  - Add locales only for translatable widgets

## [7.5.0] 2025-04-10

### Added

- [TD-7074] Support for [search after pagination](https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#search-after)

## [7.4.0] 2025-04-09

### Changed

- License

## [7.3.0] 2025-03-10

### Added

- [TD-6927] Update `td-cluster` version to add task log mocks

## [7.1.3] 2025-02-04

### Added

- [TD-5119] Update `td-df-lib` version for add validation for template dependant fields

## [7.1.2] 2025-02-03

### Changed

- [TD-6022] Update `td-df-lib` version for hierarchy field validation from file

## [7.1.1] 2025-01-29

### Changed

- [TD-6921] Update `td-df-lib` version for url field load from file

## [7.1.0] 2025-01-27

### Changed

- [TD-6862]
  - Filter for permission functionality
  - Update td-cluster

## [7.0.2] 2025-01-13

### Changed

- [TD-3612] Updates td-cache and df-lib versions

## [7.0.1] 2025-01-10

### Added

- [TD-5713]:
  - Takes specific search clauses into account.
  - Enables fetching searchable fields from template.
  - Allows to pass on content schema to functions relying on templates so that we reduce calls to redis.
  - Prioritizes settings in service mappings.

## [7.0.0] 2025-01-07

### Changed

- [TD-6911] Bump to Elixir 1.18 and updated dependencies

## [6.16.0] 2024-12-02

### Added

- [TD-6938] When creating filters, treat "inserted_at" and "last_change_at" as ranges

## [6.15.1] 2024-11-27

### Changed

- [TD-6908] `td-df-lib` bump version.

## [6.15.0] 2024-11-13

- [TD-6783] Locale dynamic mapping for content and properties

## [6.14.0] 2024-10-25

### Changed

- [TD-6880] `td-df-lib` bump version.

## [6.13.3] 2024-10-17

### Changed

- [TD-6743] Remove incompatibilities with Elasticsearch v8
  - "include_type_name" param.
  - "\_doc" prefix on bulk URL
  - Elasticsearch dependency updated to Bluetab fork

## [6.13.2] 2024-10-14

### Changed

- [TD-6617] `td-df-lib` bump version.

## [6.13.1] 2024-10-10

### Changed

- [TD-6773] `td-df-lib` bump version.

## [6.13.0] 2024-10-09

### Added

- [TD-6469] Plug to fetch `accept-language` from request headers.

## [6.9.6] 2024-07-29

### Changed

- [TD-6734] Update td-df-lib

## [6.9.5] 2024-07-26

### Changed

- [TD-6733] Update td-df-lib

## [6.9.4] 2024-07-24

### Changed

- [TD-6723-2] Update td-df-lib

## [6.9.3] 2024-07-24

### Changed

- [TD-6689] Update td-df-lib and td-cache

## [6.9.2] 2024-07-24

### Changed

- [TD-6723] Update td-df-lib

## [6.9.1] 2024-07-19

### Changed

- [TD-6713] Update td-df-lib

## [6.9.0] 2024-07-10

### Changed

- [TD-6602] Update td-cache and td-df-lib

## [6.8.3] 2024-06-26

### Changed

- [TD-4647] Default aggregation size

## [6.8.2] 2024-06-05

### Added

- [TD-6499] Update td-df-lib

## [6.8.1] 2024-06-17

### Added

- [TD-6499] Update td-df-lib

## [6.8.0] 2024-06-13

### Added

- [TD-6499] Update td-df-lib to add template content origin

## [6.7.3] 2024-06-11

### Fixed

- [TD-6440] Update td-df-lib

## [6.7.2] 2024-06-10

### Added

- [TD-6399] Add atomize_keys multi level functionality

## [6.7.1] 2024-06-10

### Changed

- [TD-6402] IndexWorker behaviour

## [6.7.0] 2024-06-07

### Changed

- [TD-6561] Use keywork list for elastic search configuration

## [6.5.3] 2024-04-30

### Added

- [TD-6492] Update td-df-lib for path enrichment

## [6.5.2] 2024-04-18

### Fixed

- [TD-6535] Delete elasticsearch index only with the index name

## [6.5.1] 2024-04-15

### Added

- [TD-6535] Enrich log hot swap errors and remove index variable configuration

## [6.5.0] 2024-04-10

### Fixed

- [TD-6424] Update td-df-lib

## [6.4.2] 2024-04-15

### Added

- [TD-6535] Enrich log hot swap errors and remove index variable configuration

## [6.4.1] 2024-04-03

### Fixed

- [TD-6507] Update td-df-lib version for add url case to format search value

## [6.4.0] 2024-04-01

### Fixed

- [TD-6401] Fixed Content aggregations have a maximum of 10 values

## [6.3.3] 2024-04-15

### Added

- [TD-6535] Enrich log hot swap errors and remove index variable configuration

## [6.3.2] 2024-04-03

## [6.3.1] 2024-04-03

### Added

- [TD-6507] Update td-df-lib version for add url case to format search value

## [6.3.0] 2024-03-13

### Added

- [TD-4110] Allow structure scoped permissions management

## [6.1.3] 2024-04-15

### Added

- [TD-6535] Enrich log hot swap errors and remove index variable configuration

## [6.1.2] 2024-04-03

### Fixed

- [TD-6507] Update td-df-lib version for add url case to format search value

## [6.1.1] 2024-02-20

### Added

- [TD-6243] Support for deleting Elasticsearch indexes

## [6.1.0] 2024-01-31

### Added

- [TD-6342] Elasticsearch.Document Integer implementation

## [6.0.4] 2024-01-16

### Changed

- [TD-6195] Update td-df-lib

## [6.0.3] 2024-01-09

### Added

- [TD-6165] Search by data structure ids

## [6.0.2] 2024-01-09

### Changed

- [TD-6221] Moved test support to lib folder to be available for other modules

## [6.0.1] 2023-12-21

### Fixed

- [TD-6181] Fix must not query

## [6.0.0] 2023-12-21

### Added

- [TD-6181] Refactor for td-dd

## [5.20.0] 2023-12-13

### Added

- [TD-6215] Improvements have been added to make it usable by other services.

## [5.19.0] 2023-11-16

### Added

- [TD-6140] Support for deep strigify_keys

## [5.17.0] 2023-10-31

### Added

- [TD-6059]
  - Common modules for Truedat's auth
  - Generic support for Elasticsearch
