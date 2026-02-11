defmodule TdCore.XLSX.UploadWorkerTest do
  use ExUnit.Case

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
    test "creates STARTED event before processing", %{claims: claims} do
      path = "test/fixtures/xlsx/upload_tiny.xlsx"
      job_id = 888
      lang = "es"
      auto_publish = "false"

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
