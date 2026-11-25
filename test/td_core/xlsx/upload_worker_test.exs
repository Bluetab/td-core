defmodule TdCore.XLSX.UploadWorkerTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias TdCluster.TestHelpers.TdAuditMock.UploadJobs
  alias TdCore.Auth.Claims
  alias TdCore.TestSupport.CacheHelpers
  alias TdCore.XLSX.UploadWorker

  @moduletag sandbox: :shared

  setup_all do
    user = CacheHelpers.insert_user(role: "admin")

    claims =
      %Claims{
        user_id: user.id,
        user_name: user.user_name,
        role: user.role,
        jti: 123,
        exp: DateTime.add(DateTime.utc_now(), 10)
      }
      |> Jason.encode!()
      |> Jason.decode!()

    [claims: claims, user: user]
  end

  describe "perform/1" do
    test "explicit target status takes precedence over legacy auto_publish", %{claims: claims} do
      Mox.expect(MockBulkLoadItem, :bulk_load_item, 3, fn _item, _claims, ctx ->
        send(self(), {:requested_status, ctx.to_status})
        {:created, {"id", "details"}}
      end)

      UploadJobs.create_started(&Mox.expect/4, 700)

      Enum.each(2..4, fn row_number ->
        UploadJobs.create_info(&Mox.expect/4, 700, %{
          type: "created",
          details: "details",
          sheet: "type_1",
          row_number: row_number
        })
      end)

      UploadJobs.create_completed(&Mox.expect/4, 700, %{
        insert_count: 3,
        update_count: 0,
        error_count: 0,
        unchanged_count: 0,
        invalid_sheet_count: 0
      })

      assert {:ok, nil} =
               UploadWorker.run(%{
                 "path" => "test/fixtures/xlsx/upload_tiny.xlsx",
                 "job_id" => 700,
                 "scope" => "mock_bulk_load",
                 "opts" => %{
                   "lang" => "en",
                   "auto_publish" => "true",
                   "to_status" => "pending_approval",
                   "claims" => claims
                 }
               })

      assert_receive {:requested_status, "pending_approval"}
    end

    test "legacy auto_publish is preserved when target status is absent", %{claims: claims} do
      Mox.expect(MockBulkLoadItem, :bulk_load_item, 3, fn _item, _claims, ctx ->
        send(self(), {:requested_status, ctx.to_status})
        {:created, {"id", "details"}}
      end)

      UploadJobs.create_started(&Mox.expect/4, 701)

      Enum.each(2..4, fn row_number ->
        UploadJobs.create_info(&Mox.expect/4, 701, %{
          type: "created",
          details: "details",
          sheet: "type_1",
          row_number: row_number
        })
      end)

      UploadJobs.create_completed(&Mox.expect/4, 701, %{
        insert_count: 3,
        update_count: 0,
        error_count: 0,
        unchanged_count: 0,
        invalid_sheet_count: 0
      })

      assert {:ok, nil} =
               UploadWorker.run(%{
                 "path" => "test/fixtures/xlsx/upload_tiny.xlsx",
                 "job_id" => 701,
                 "scope" => "mock_bulk_load",
                 "opts" => %{"lang" => "en", "auto_publish" => "true", "claims" => claims}
               })

      assert_receive {:requested_status, "published"}
    end

    test "creates STARTED event before processing", %{claims: claims} do
      path = "test/fixtures/xlsx/upload_tiny.xlsx"
      job_id = 888
      lang = "es"
      auto_publish = "false"

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

    test "fails the job without row events when the preflight rejects the workbook", %{
      claims: claims
    } do
      job_id = 889

      UploadJobs.create_started(&Mox.expect/4, job_id)
      UploadJobs.create_failed(&Mox.expect/4, job_id, :invalid_format)

      assert {:ok, nil} =
               UploadWorker.run(%{
                 "path" => "test/fixtures/xlsx/upload_tiny.xlsx",
                 "job_id" => job_id,
                 "scope" => "failing_preflight_bulk_load",
                 "opts" => %{"lang" => "en", "auto_publish" => "false", "claims" => claims}
               })
    end

    test "logs unexpected bulk_load_item results as errors without aborting", %{claims: claims} do
      path = "test/fixtures/xlsx/upload_tiny.xlsx"
      job_id = 890
      lang = "es"
      auto_publish = "false"

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

    test "handles invalid file format", %{claims: claims} do
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
