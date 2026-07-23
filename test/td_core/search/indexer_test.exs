defmodule TdCore.Search.IndexerTest do
  use ExUnit.Case

  import ExUnit.CaptureLog
  import Mox

  alias TdCore.Search.Cluster
  alias TdCore.Search.Indexer

  describe "log_bulk_post" do
    setup do
      errored_item_1 = %{
        "index" => %{
          "_id" => "1769343",
          "_index" => "structures-1691648696299822",
          "_type" => "_doc",
          "error" => %{
            "caused_by" => %{
              "caused_by" => %{
                "reason" => "Failed to parse with all enclosed parsers",
                "type" => "date_time_parse_exception"
              },
              "reason" =>
                "failed to parse date field [2022-11-28 09:32:55.036000000] with format [strict_date_optional_time||epoch_millis]",
              "type" => "illegal_argument_exception"
            },
            "reason" =>
              "failed to parse field [metadata.createdAt] of type [date] in document with id '1769343'. Preview of field's value: '2022-11-28 09:32:55.036000000'",
            "type" => "mapper_parsing_exception"
          },
          "status" => 400
        }
      }

      errored_item_2 = %{
        "index" => %{
          "_id" => "1769350",
          "_index" => "structures-1691648696299822",
          "_type" => "_doc",
          "error" => %{
            "caused_by" => %{
              "caused_by" => %{
                "reason" => "Failed to parse with all enclosed parsers",
                "type" => "date_time_parse_exception"
              },
              "reason" =>
                "failed to parse date field [2022-11-25 12:48:05.233999872] with format [strict_date_optional_time||epoch_millis]",
              "type" => "illegal_argument_exception"
            },
            "reason" =>
              "failed to parse field [metadata.createdAt] of type [date] in document with id '1769350'. Preview of field's value: '2022-11-25 12:48:05.233999872'",
            "type" => "mapper_parsing_exception"
          },
          "status" => 400
        }
      }

      successful_item = %{
        "index" => %{
          "_id" => "1769351",
          "_index" => "structures-1691648696299822",
          "_primary_term" => 1,
          "_seq_no" => 26_637,
          "_shards" => %{"failed" => 0, "successful" => 1, "total" => 2},
          "_type" => "_doc",
          "_version" => 19,
          "result" => "updated",
          "status" => 200
        }
      }

      [
        errored_item_1: errored_item_1,
        errored_item_2: errored_item_2,
        successful_item: successful_item
      ]
    end

    test "two errors", %{
      errored_item_1: errored_item_1,
      errored_item_2: errored_item_2,
      successful_item: successful_item
    } do
      post_bulk_response_items = [errored_item_1, errored_item_2, successful_item]

      log =
        capture_log(fn ->
          Indexer.log_bulk_post(
            "structures",
            {:ok, %{"errors" => true, "items" => post_bulk_response_items}},
            "index"
          )
        end)

      assert log =~ "structures"
      assert log =~ "bulk indexing encountered 2 errors"
      assert log =~ "Document ID 1769350"
      assert log =~ "Document ID 1769343"
      assert log =~ "failed to parse field"
      assert log =~ "2022-11-25 12:48:05.233999872"
      assert log =~ "2022-11-28 09:32:55.036000000"
    end
  end

  describe "log_hot_swap" do
    setup do
      elasticsearch_exception_1 = %Elasticsearch.Exception{
        status: 400,
        line: nil,
        col: nil,
        message:
          "failed to parse field [metadata.createdAt] of type [date] in document with id '1769350'. Preview of field's value: '2022-11-25 12:48:05.233999872'",
        type: "mapper_parsing_exception",
        query: nil,
        raw: %{
          "_id" => "1769350",
          "_index" => "structures-1691599336795214",
          "_type" => "_doc",
          "error" => %{
            "caused_by" => %{
              "caused_by" => %{
                "reason" => "Failed to parse with all enclosed parsers",
                "type" => "date_time_parse_exception"
              },
              "reason" =>
                "failed to parse date field [2022-11-25 12:48:05.233999872] with format [strict_date_optional_time||epoch_millis]",
              "type" => "illegal_argument_exception"
            },
            "reason" =>
              "failed to parse field [metadata.createdAt] of type [date] in document with id '1769350'. Preview of field's value: '2022-11-25 12:48:05.233999872'",
            "type" => "mapper_parsing_exception"
          },
          "status" => 400
        }
      }

      elasticsearch_exception_2 = %Elasticsearch.Exception{
        status: 400,
        line: nil,
        col: nil,
        message:
          "failed to parse field [metadata.createdAt] of type [date] in document with id '1769343'. Preview of field's value: '2022-11-28 09:32:55.036000000'",
        type: "mapper_parsing_exception",
        query: nil,
        raw: %{
          "_id" => "1769343",
          "_index" => "structures-1691599336795214",
          "_type" => "_doc",
          "error" => %{
            "caused_by" => %{
              "caused_by" => %{
                "reason" => "Failed to parse with all enclosed parsers",
                "type" => "date_time_parse_exception"
              },
              "reason" =>
                "failed to parse date field [2022-11-28 09:32:55.036000000] with format [strict_date_optional_time||epoch_millis]",
              "type" => "illegal_argument_exception"
            },
            "reason" =>
              "failed to parse field [metadata.createdAt] of type [date] in document with id '1769343'. Preview of field's value: '2022-11-28 09:32:55.036000000'",
            "type" => "mapper_parsing_exception"
          },
          "status" => 400
        }
      }

      elasticsearch_exception_3 = %Elasticsearch.Exception{
        status: 400,
        line: nil,
        col: nil,
        message:
          "Invalid alias name [structures]: an index or data stream exists with the same name as the alias",
        type: "invalid_alias_name_exception",
        query: nil,
        raw: %{
          "error" => %{
            "reason" =>
              "Invalid alias name [structures]: an index or data stream exists with the same name as the alias",
            "root_cause" => [
              %{
                "reason" =>
                  "Invalid alias name [structures]: an index or data stream exists with the same name as the alias",
                "type" => "invalid_alias_name_exception"
              }
            ],
            "type" => "invalid_alias_name_exception"
          },
          "status" => 400
        }
      }

      exception_connection_refused = %HTTPoison.Error{reason: :econnrefused, id: nil}
      exception_connection_closed = %HTTPoison.Error{reason: :closed, id: nil}

      [
        elasticsearch_exception_1: elasticsearch_exception_1,
        elasticsearch_exception_2: elasticsearch_exception_2,
        elasticsearch_exception_3: elasticsearch_exception_3,
        exception_connection_refused: exception_connection_refused,
        exception_connection_closed: exception_connection_closed
      ]
    end

    test "one exception, one element list", %{
      elasticsearch_exception_1: elasticsearch_exception_1
    } do
      log =
        capture_log(fn ->
          Indexer.log_hot_swap_errors(
            "structures-1691599336795214",
            :foo,
            {:error, [elasticsearch_exception_1]}
          )
        end)

      assert log =~ "build finished in foo with an error"
      assert log =~ "structures-1691599336795214"
      assert log =~ "Document ID 1769350"
      assert log =~ "mapper_parsing_exception"
      assert log =~ "2022-11-25 12:48:05.233999872"
    end

    test "two exceptions", %{
      elasticsearch_exception_1: elasticsearch_exception_1,
      elasticsearch_exception_2: elasticsearch_exception_2
    } do
      log =
        capture_log(fn ->
          Indexer.log_hot_swap_errors(
            "structures-1691599336795214",
            :foo,
            {:error, [elasticsearch_exception_1, elasticsearch_exception_2]}
          )
        end)

      assert log =~ "build finished in foo with 2 errors"
      assert log =~ "structures-1691599336795214"
      assert log =~ "Document ID 1769350"
      assert log =~ "Document ID 1769343"
      assert log =~ "mapper_parsing_exception"
      assert log =~ "2022-11-25 12:48:05.233999872"
      assert log =~ "2022-11-28 09:32:55.036000000"
    end

    test "one exception without containing list (index with same name already exists)", %{
      elasticsearch_exception_3: elasticsearch_exception_3
    } do
      log =
        capture_log(fn ->
          Indexer.log_hot_swap_errors(
            "structures-1691599336795214",
            :foo,
            {:error, elasticsearch_exception_3}
          )
        end)

      assert log =~ "structures-1691599336795214"
      assert log =~ "build finished in foo 001 with an error"
      assert log =~ "an index or data stream exists with the same name as the alias"
    end

    test "one exception without containing list (connection refused before starting hot_swap)", %{
      exception_connection_refused: exception_connection_refused
    } do
      log =
        capture_log(fn ->
          Indexer.log_hot_swap_errors(
            "structures-1691599336795214",
            :foo,
            {:error, exception_connection_refused}
          )
        end)

      assert log =~ "structures-1691599336795214"
      assert log =~ "build finished in foo 001 with an error"
      assert log =~ ":econnrefused"
    end

    test "multiple exceptions (connection refused and closed in the middle of hot_swap)", %{
      exception_connection_refused: exception_connection_refused,
      exception_connection_closed: exception_connection_closed
    } do
      log =
        capture_log(fn ->
          Indexer.log_hot_swap_errors(
            "structures-1691599336795214",
            :foo,
            {
              :error,
              [
                exception_connection_refused,
                exception_connection_refused,
                exception_connection_closed
              ]
            }
          )
        end)

      assert log =~ "structures-1691599336795214"
      assert log =~ "build finished in foo with 3 errors"
      assert log =~ ":econnrefused"
    end
  end

  describe "list_indexes" do
    test "parses Elasticsearch response to render indexes" do
      ElasticsearchMock
      |> Mox.expect(:request, 1, fn _, :get, "_alias", _, [] ->
        {:ok,
         %{
           "index_1" => %{"aliases" => %{"alias_1" => %{}}},
           "index_2" => %{"aliases" => %{"alias_2" => %{}}},
           "index_3" => %{"aliases" => %{}}
         }}
      end)

      ElasticsearchMock
      |> Mox.expect(:request, 1, fn _, :get, "_stats/docs,store", _, [] ->
        {:ok,
         %{
           "indices" => %{
             "index_1" => %{
               "total" => %{
                 "store" => %{"size_in_bytes" => 123_456},
                 "docs" => %{"count" => 42}
               }
             },
             "index_2" => %{
               "total" => %{
                 "store" => %{"size_in_bytes" => 789_123},
                 "docs" => %{"count" => 24}
               }
             },
             "index_3" => %{
               "total" => %{
                 "store" => %{"size_in_bytes" => 42},
                 "docs" => %{"count" => 55}
               }
             }
           }
         }}
      end)

      assert [
               %{alias: "alias_1", documents: 42, key: "index_1", size: 123_456},
               %{alias: "alias_2", documents: 24, key: "index_2", size: 789_123},
               %{alias: nil, documents: 55, key: "index_3", size: 42}
             ] = Indexer.list_indexes()
    end
  end

  describe "refresh" do
    test "refreshes index with default params" do
      Mox.expect(ElasticsearchMock, :request, 1, fn _, :post, "/concepts/_refresh", _, [] ->
        {:ok, :success}
      end)

      Mox.expect(ElasticsearchMock, :request, 1, fn _,
                                                    :post,
                                                    "/concepts/_forcemerge?max_num_segments=5",
                                                    _,
                                                    [] ->
        {:ok, :success}
      end)

      assert :ok == Indexer.refresh(Cluster, "concepts")
    end

    test "refreshes index with opt params" do
      Mox.expect(ElasticsearchMock, :request, 1, fn _, :post, "/concepts/_refresh", _, [] ->
        {:ok, :success}
      end)

      Mox.expect(ElasticsearchMock, :request, 1, fn _,
                                                    :post,
                                                    "/concepts/_forcemerge?max_num_segments=10&wait_for_completion=true",
                                                    _,
                                                    [] ->
        {:ok, :success}
      end)

      assert :ok ==
               Indexer.refresh(Cluster, "concepts",
                 forcemerge_options: [
                   max_num_segments: 10,
                   wait_for_completion: true
                 ]
               )
    end

    test "rejects nil opt param" do
      Mox.expect(ElasticsearchMock, :request, 1, fn _, :post, "/concepts/_refresh", _, [] ->
        {:ok, :success}
      end)

      Mox.expect(ElasticsearchMock, :request, 1, fn _,
                                                    :post,
                                                    "/concepts/_forcemerge?max_num_segments=10",
                                                    _,
                                                    [] ->
        {:ok, :success}
      end)

      assert :ok ==
               Indexer.refresh(Cluster, "concepts",
                 forcemerge_options: [
                   max_num_segments: 10,
                   wait_for_completion: nil
                 ]
               )
    end
  end

  describe "ensure_index_exists/1" do
    test "returns :ok if index exists" do
      ElasticsearchMock
      |> expect(:request, fn _, :get, "/string_test_alias", _, [] ->
        {:ok, %{"string_test_alias" => %{}}}
      end)

      assert :ok = Indexer.ensure_index_exists(:test_alias)
    end
  end

  describe "bulk_load_index_settings/1" do
    test "overrides refresh_interval and number_of_replicas for bulk load" do
      settings = %{
        "refresh_interval" => "5s",
        "number_of_replicas" => 1,
        analysis: %{tokenizer: %{}}
      }

      assert %{
               "refresh_interval" => "-1",
               "number_of_replicas" => 0,
               analysis: %{tokenizer: %{}}
             } = Indexer.bulk_load_index_settings(settings)
    end
  end

  describe "production_index_settings/1" do
    test "builds restore body from string keys" do
      settings = %{"refresh_interval" => "10s", "number_of_replicas" => 2}

      assert %{
               "index" => %{
                 "refresh_interval" => "10s",
                 "number_of_replicas" => 2
               }
             } = Indexer.production_index_settings(settings)
    end

    test "falls back to defaults when keys are missing" do
      assert %{
               "index" => %{
                 "refresh_interval" => "5s",
                 "number_of_replicas" => 1
               }
             } = Indexer.production_index_settings(%{})
    end
  end

  describe "restore_index_settings/3" do
    test "puts production settings to the index" do
      settings = %{"refresh_interval" => "10s", "number_of_replicas" => 2}

      ElasticsearchMock
      |> expect(:request, fn _, :put, "/structures-1/_settings", body, [] ->
        assert body == %{
                 "index" => %{
                   "refresh_interval" => "10s",
                   "number_of_replicas" => 2
                 }
               }

        {:ok, %{"acknowledged" => true}}
      end)

      assert :ok = Indexer.restore_index_settings(Cluster, "structures-1", settings)
    end

    test "returns error when Elasticsearch rejects the update" do
      error = %HTTPoison.Error{reason: :timeout, id: nil}

      ElasticsearchMock
      |> expect(:request, fn _, :put, "/structures-1/_settings", _, [] ->
        {:error, error}
      end)

      assert {:error, ^error} =
               Indexer.restore_index_settings(Cluster, "structures-1", %{
                 "refresh_interval" => "5s",
                 "number_of_replicas" => 1
               })
    end
  end

  describe "finalize_hot_swap_index/2" do
    test "refreshes without forcemerge using dedicated recv_timeout" do
      ElasticsearchMock
      |> expect(:request, fn _, :post, "/structures-1/_refresh", _, opts ->
        assert Keyword.get(opts, :recv_timeout) == 5_000
        {:ok, %{"_shards" => %{"successful" => 1}}}
      end)

      assert :ok = Indexer.finalize_hot_swap_index(Cluster, "structures-1")
    end

    test "returns error when refresh times out" do
      error = %HTTPoison.Error{reason: :timeout, id: nil}

      ElasticsearchMock
      |> expect(:request, fn _, :post, "/structures-1/_refresh", _, opts ->
        assert Keyword.get(opts, :recv_timeout) == 5_000
        {:error, error}
      end)

      assert {:error, ^error} = Indexer.finalize_hot_swap_index(Cluster, "structures-1")
    end
  end

  describe "refresh skip_forcemerge" do
    test "skips forcemerge when skip_forcemerge is true" do
      ElasticsearchMock
      |> expect(:request, fn _, :post, "/concepts/_refresh", _, [] ->
        {:ok, :success}
      end)

      assert :ok == Indexer.refresh(Cluster, "concepts", skip_forcemerge: true)
    end

    test "passes recv_timeout to refresh and forcemerge requests" do
      ElasticsearchMock
      |> expect(:request, fn _, :post, "/concepts/_refresh", _, opts ->
        assert Keyword.get(opts, :recv_timeout) == 5_000
        {:ok, :success}
      end)
      |> expect(:request, fn _,
                             :post,
                             "/concepts/_forcemerge?max_num_segments=5",
                             _,
                             opts ->
        assert Keyword.get(opts, :recv_timeout) == 5_000
        {:ok, :success}
      end)

      assert :ok == Indexer.refresh(Cluster, "concepts", recv_timeout: 5_000)
    end
  end

  describe "log_bulk_post took= env gate" do
    setup do
      previous = System.get_env("ES_BULK_TOOK_LOG")

      on_exit(fn ->
        if previous do
          System.put_env("ES_BULK_TOOK_LOG", previous)
        else
          System.delete_env("ES_BULK_TOOK_LOG")
        end
      end)

      response =
        {:ok,
         %{
           "errors" => false,
           "items" => [%{"index" => %{}}, %{"index" => %{}}],
           "took" => 42
         }}

      [response: response]
    end

    test "logs took when ES_BULK_TOOK_LOG is 1", %{response: response} do
      System.put_env("ES_BULK_TOOK_LOG", "1")

      log =
        capture_log(fn ->
          Indexer.log_bulk_post("structures", response, "index")
        end)

      assert log =~ "structures: bulk indexed 2 documents (took=42)"
    end

    test "logs took when ES_BULK_TOOK_LOG is true", %{response: response} do
      System.put_env("ES_BULK_TOOK_LOG", "TRUE")

      log =
        capture_log(fn ->
          Indexer.log_bulk_post("structures", response, "index")
        end)

      assert log =~ "took=42"
    end

    test "does not log took when ES_BULK_TOOK_LOG is unset", %{response: response} do
      System.delete_env("ES_BULK_TOOK_LOG")

      log =
        capture_log(fn ->
          Indexer.log_bulk_post("structures", response, "index")
        end)

      refute log =~ "took="
    end

    test "does not log took when ES_BULK_TOOK_LOG is 0", %{response: response} do
      System.put_env("ES_BULK_TOOK_LOG", "0")

      log =
        capture_log(fn ->
          Indexer.log_bulk_post("structures", response, "index")
        end)

      refute log =~ "took="
    end
  end

  describe "reindex/2 with ids" do
    setup do
      alias Elasticsearch.Cluster.Config
      alias TdCore.Search.BulkUploaderTxnRequiredStore

      previous_config = Config.get(Cluster)
      previous_env = Application.get_env(:td_core, TdCore.Search.Cluster, [])

      Application.put_env(
        :td_core,
        TdCore.Search.Cluster,
        Keyword.put(previous_env, :reindex_concurrency, 1)
      )

      updated =
        previous_config
        |> Map.put_new(:json_library, Jason)
        |> Map.put(:indexes, %{
          string_test_alias: %{
            store: BulkUploaderTxnRequiredStore,
            sources: [TdCore.Search.BulkUploaderUploadDoc],
            bulk_page_size: 1,
            bulk_wait_interval: 0,
            settings: %{}
          }
        })

      :sys.replace_state(Cluster, fn _ -> updated end)
      GenServer.call(Cluster, :save_config)

      on_exit(fn ->
        :sys.replace_state(Cluster, fn _ -> previous_config end)
        GenServer.call(Cluster, :save_config)
        Application.put_env(:td_core, TdCore.Search.Cluster, previous_env)
      end)

      :ok
    end

    test "consumes store.stream(ids) inside store.transaction" do
      ElasticsearchMock
      |> expect(:request, fn _, :post, "/string_test_alias/_bulk", _body, [] ->
        {:ok, %{"errors" => false, "items" => [], "took" => 1}}
      end)

      capture_log(fn ->
        assert :ok == Indexer.reindex(:test_alias, [12619])
      end)
    end
  end
end
