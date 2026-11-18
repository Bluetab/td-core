defmodule TdCore.XLSX.UploadWorkerTest do
  use ExUnit.Case

  import ExUnit.CaptureLog
  import Mox

  alias TdCluster.TestHelpers.TdAuditMock.UploadJobs
  alias TdCore.Auth.Claims
  alias TdCore.XLSX.UploadWorker

  setup :verify_on_exit!

  setup_all do
    claims =
      %Claims{
        user_id: 123,
        user_name: "upload_admin",
        role: "admin",
        jti: 123,
        exp: DateTime.add(DateTime.utc_now(), 10)
      }
      |> Jason.encode!()
      |> Jason.decode!()

    [claims: claims]
  end

  describe "perform/1" do
    test "preflight rejects a header-only second sheet before processing any rows", %{
      claims: claims
    } do
      path =
        Path.join(
          System.tmp_dir!(),
          "td_core_preflight_#{System.unique_integer([:positive])}.xlsx"
        )

      on_exit(fn -> File.rm(path) end)

      %Elixlsx.Workbook{
        sheets: [
          %Elixlsx.Sheet{name: "type_1", rows: [["external_id"], ["id-1"]]},
          %Elixlsx.Sheet{name: "type_2", rows: [["name_es"]]}
        ]
      }
      |> Elixlsx.write_to(path)

      job_id = 1001

      Mox.expect(MockBulkLoadItem, :get_opts, fn _ ->
        [
          preflight: fn sheets, _ctx ->
            assert %{
                     "type_1" => {["external_id"], [["id-1"]]},
                     "type_2" => {["name_es"], []}
                   } = sheets

            {:error, :invalid_format}
          end
        ]
      end)

      UploadJobs.create_started(&Mox.expect/4, job_id)
      UploadJobs.create_failed(&Mox.expect/4, job_id, :invalid_format)

      assert capture_log(fn ->
               assert {:ok, nil} =
                        UploadWorker.run(%{
                          "path" => path,
                          "job_id" => job_id,
                          "scope" => "mock_bulk_load",
                          "opts" => %{
                            "lang" => "en",
                            "auto_publish" => "false",
                            "claims" => claims
                          }
                        })
             end) == ""
    end

    test "header-only uploads without preflight still fail as empty_sheets", %{claims: claims} do
      Mox.expect(MockBulkLoadItem, :get_opts, 1, fn _ -> [] end)
      UploadJobs.create_started(&Mox.expect/4, 1002)
      UploadJobs.create_failed(&Mox.expect/4, 1002, :empty_sheets)

      assert {:ok, nil} =
               UploadWorker.run(%{
                 "path" => "test/fixtures/xlsx/only_headers.xlsx",
                 "job_id" => 1002,
                 "scope" => "mock_bulk_load",
                 "opts" => %{"lang" => "en", "auto_publish" => "false", "claims" => claims}
               })
    end

    test "preflight rejection of a header-only upload takes precedence over empty_sheets", %{
      claims: claims
    } do
      Mox.expect(MockBulkLoadItem, :get_opts, fn _ ->
        [
          preflight: fn sheets, _ctx ->
            assert %{"type_1" => {[_ | _], []}} = sheets
            {:error, :invalid_format}
          end
        ]
      end)

      UploadJobs.create_started(&Mox.expect/4, 1003)
      UploadJobs.create_failed(&Mox.expect/4, 1003, :invalid_format)

      assert {:ok, nil} =
               UploadWorker.run(%{
                 "path" => "test/fixtures/xlsx/only_headers.xlsx",
                 "job_id" => 1003,
                 "scope" => "mock_bulk_load",
                 "opts" => %{"lang" => "en", "auto_publish" => "false", "claims" => claims}
               })
    end

    test "creates only STARTED and FAILED with invalid_format when preflight returns a map error",
         %{claims: claims} do
      job_id = 997
      reason = %{reason: "bad"}

      Mox.expect(MockBulkLoadItem, :get_opts, fn _ ->
        [
          preflight: fn sheets, ctx ->
            assert %{"type_1" => {_headers, rows}} = sheets
            assert length(rows) == 3
            assert ctx.job_id == job_id
            assert ctx.lang == "es"
            send(self(), :preflight_called)
            {:error, reason}
          end
        ]
      end)

      UploadJobs.create_started(&Mox.expect/4, job_id)
      UploadJobs.create_failed(&Mox.expect/4, job_id, :invalid_format)

      assert capture_log(fn ->
               assert {:ok, nil} =
                        UploadWorker.run(%{
                          "path" => "test/fixtures/xlsx/upload_tiny.xlsx",
                          "job_id" => job_id,
                          "scope" => "mock_bulk_load",
                          "opts" => %{
                            "lang" => "es",
                            "auto_publish" => "false",
                            "claims" => claims
                          }
                        })
             end) == ""

      assert_received :preflight_called
      refute_received :preflight_called
    end

    test "reports a generic failure when preflight returns a malformed result", %{claims: claims} do
      job_id = 1005

      Mox.expect(MockBulkLoadItem, :get_opts, fn _ -> [preflight: fn _, _ -> :invalid end] end)

      UploadJobs.create_started(&Mox.expect/4, job_id)
      UploadJobs.create_failed(&Mox.expect/4, job_id, :invalid_format)

      assert capture_log(fn ->
               assert {:ok, nil} =
                        UploadWorker.run(%{
                          "path" => "test/fixtures/xlsx/upload_tiny.xlsx",
                          "job_id" => job_id,
                          "scope" => "mock_bulk_load",
                          "opts" => %{
                            "lang" => "es",
                            "auto_publish" => "false",
                            "claims" => claims
                          }
                        })
             end) == ""
    end

    test "fails normally without item events when preflight returns {:ok, nil}", %{claims: claims} do
      job_id = 1006

      Mox.expect(MockBulkLoadItem, :get_opts, fn _ ->
        [preflight: fn _, _ -> {:ok, nil} end]
      end)

      UploadJobs.create_started(&Mox.expect/4, job_id)
      UploadJobs.create_failed(&Mox.expect/4, job_id, :invalid_format)

      assert capture_log(fn ->
               assert {:ok, nil} =
                        UploadWorker.run(%{
                          "path" => "test/fixtures/xlsx/upload_tiny.xlsx",
                          "job_id" => job_id,
                          "scope" => "mock_bulk_load",
                          "opts" => %{
                            "lang" => "es",
                            "auto_publish" => "false",
                            "claims" => claims
                          }
                        })
             end) == ""
    end

    for malformed_result <- [{:ok, [:bad]}, {:error, {:bad, :reason}}] do
      test "fails safely when preflight returns #{inspect(malformed_result)}", %{claims: claims} do
        job_id = 1007

        Mox.expect(MockBulkLoadItem, :get_opts, fn _ ->
          [preflight: fn _, _ -> unquote(Macro.escape(malformed_result)) end]
        end)

        UploadJobs.create_started(&Mox.expect/4, job_id)
        UploadJobs.create_failed(&Mox.expect/4, job_id, :invalid_format)

        assert capture_log(fn ->
                 assert {:ok, nil} =
                          UploadWorker.run(%{
                            "path" => "test/fixtures/xlsx/upload_tiny.xlsx",
                            "job_id" => job_id,
                            "scope" => "mock_bulk_load",
                            "opts" => %{
                              "lang" => "es",
                              "auto_publish" => "false",
                              "claims" => claims
                            }
                          })
               end) == ""
      end
    end

    test "creates STARTED event before processing", %{claims: claims} do
      path = "test/fixtures/xlsx/upload_tiny.xlsx"
      job_id = 888
      lang = "es"
      auto_publish = "false"

      Mox.expect(MockBulkLoadItem, :get_opts, 1, fn _ -> [] end)

      Mox.expect(MockBulkLoadItem, :bulk_load_item, 3, fn _, _, _ ->
        {:created, {"id", "details"}}
      end)

      UploadJobs.create_started(&Mox.expect/4, job_id)

      Enum.each(2..4, fn row_number ->
        UploadJobs.create_info(&Mox.expect/4, job_id, %{
          type: "created",
          details: "details",
          sheet: "type_1",
          row_number: row_number
        })
      end)

      UploadJobs.create_completed(&Mox.expect/4, job_id, %{
        insert_count: 3,
        update_count: 0,
        error_count: 0,
        unchanged_count: 0,
        invalid_sheet_count: 0
      })

      assert {:ok, nil} =
               UploadWorker.run(%{
                 "path" => path,
                 "job_id" => job_id,
                 "scope" => "mock_bulk_load",
                 "opts" => %{
                   "lang" => lang,
                   "auto_publish" => auto_publish,
                   "claims" => claims
                 }
               })
    end

    test "logs unexpected bulk_load_item results as errors without aborting", %{claims: claims} do
      path = "test/fixtures/xlsx/upload_tiny.xlsx"
      job_id = 890
      lang = "es"
      auto_publish = "false"

      Mox.expect(MockBulkLoadItem, :get_opts, 1, fn _ -> [] end)
      Mox.expect(MockBulkLoadItem, :bulk_load_item, 3, fn _, _, _ -> :error end)

      UploadJobs.create_started(&Mox.expect/4, job_id)

      Enum.each(2..4, fn row_number ->
        UploadJobs.create_error(&Mox.expect/4, job_id, %{
          type: "unexpected_error",
          details: %{message: ":error"},
          sheet: "type_1",
          row_number: row_number
        })
      end)

      UploadJobs.create_completed(&Mox.expect/4, job_id, %{
        insert_count: 0,
        update_count: 0,
        error_count: 3,
        unchanged_count: 0,
        invalid_sheet_count: 0
      })

      log =
        capture_log(fn ->
          assert {:ok, nil} =
                   UploadWorker.run(%{
                     "path" => path,
                     "job_id" => job_id,
                     "scope" => "mock_bulk_load",
                     "opts" => %{
                       "lang" => lang,
                       "auto_publish" => auto_publish,
                       "claims" => claims
                     }
                   })
        end)

      assert log =~ "Unexpected bulk_load_item result: :error"
    end

    test "logs raised bulk_load_item exceptions as errors without aborting", %{claims: claims} do
      path = "test/fixtures/xlsx/upload_tiny.xlsx"
      job_id = 891
      lang = "es"
      auto_publish = "false"

      Mox.expect(MockBulkLoadItem, :get_opts, 1, fn _ -> [] end)

      Mox.expect(MockBulkLoadItem, :bulk_load_item, 3, fn _, _, _ ->
        raise "boom"
      end)

      UploadJobs.create_started(&Mox.expect/4, job_id)

      Enum.each(2..4, fn row_number ->
        UploadJobs.create_error(&Mox.expect/4, job_id, %{
          type: "unexpected_error",
          details: %{message: "boom"},
          sheet: "type_1",
          row_number: row_number
        })
      end)

      UploadJobs.create_completed(&Mox.expect/4, job_id, %{
        insert_count: 0,
        update_count: 0,
        error_count: 3,
        unchanged_count: 0,
        invalid_sheet_count: 0
      })

      log =
        capture_log(fn ->
          assert {:ok, nil} =
                   UploadWorker.run(%{
                     "path" => path,
                     "job_id" => job_id,
                     "scope" => "mock_bulk_load",
                     "opts" => %{
                       "lang" => lang,
                       "auto_publish" => auto_publish,
                       "claims" => claims
                     }
                   })
        end)

      assert log =~ "bulk_load_item raised:"
    end

    test "reports invalid format without resolving bulk-load options", %{claims: claims} do
      path = "test/fixtures/xlsx/invalid.xlsx"
      job_id = 889
      lang = "es"
      auto_publish = "false"

      UploadJobs.create_started(&Mox.expect/4, job_id)
      UploadJobs.create_failed(&Mox.expect/4, job_id, :invalid_format)

      assert {:ok, nil} =
               UploadWorker.run(%{
                 "path" => path,
                 "job_id" => job_id,
                 "scope" => "mock_bulk_load",
                 "opts" => %{
                   "lang" => lang,
                   "auto_publish" => auto_publish,
                   "claims" => claims
                 }
               })
    end

    test "handles file not found error", %{claims: claims} do
      path = "test/fixtures/xlsx/nonexistent.xlsx"
      job_id = 911
      lang = "es"
      auto_publish = "false"

      UploadJobs.create_started(&Mox.expect/4, job_id)
      UploadJobs.create_failed(&Mox.expect/4, job_id, "file not found")

      assert {:ok, nil} =
               UploadWorker.run(%{
                 "path" => path,
                 "job_id" => job_id,
                 "scope" => "mock_bulk_load",
                 "opts" => %{
                   "lang" => lang,
                   "auto_publish" => auto_publish,
                   "claims" => claims
                 }
               })
    end

    test "handles empty sheets error", %{claims: claims} do
      path = "test/fixtures/xlsx/empty.xlsx"
      job_id = 998
      lang = "es"
      auto_publish = "false"

      UploadJobs.create_started(&Mox.expect/4, job_id)
      UploadJobs.create_failed(&Mox.expect/4, job_id, :empty_sheets)

      assert {:ok, nil} =
               UploadWorker.run(%{
                 "path" => path,
                 "job_id" => job_id,
                 "scope" => "mock_bulk_load",
                 "opts" => %{
                   "lang" => lang,
                   "auto_publish" => auto_publish,
                   "claims" => claims
                 }
               })
    end
  end
end
