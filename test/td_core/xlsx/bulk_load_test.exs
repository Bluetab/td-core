defmodule TdCore.XLSX.BulkLoadTest do
  @moduledoc """
  Tests the generic bulk load pipeline with inline scope implementations.
  """
  use ExUnit.Case

  import Mox

  alias TdCluster.TestHelpers.TdAuditMock.UploadJobs, as: UploadJobsMock
  alias TdCore.XLSX.BulkLoad
  alias TdCore.XLSX.Reader

  setup :verify_on_exit!

  @fixture "test/fixtures/xlsx/upload_tiny.xlsx"

  describe "bulk_load/2" do
    test "skips sheets without template instead of crashing" do
      {:ok, sheets} = Reader.read(@fixture)
      job_id = 991

      UploadJobsMock.create_error(
        &Mox.expect/4,
        job_id,
        %{type: "template_not_found"},
        {:ok, nil},
        3
      )

      ctx = %{impl_for: %ConfigurableBulkLoadImpl{templates: %{"type_1" => nil}}, job_id: job_id, lang: "en"}

      assert {:ok, result} = BulkLoad.bulk_load(sheets, ctx)
      assert result.error_count == 3
      assert result.insert_count == 0
    end

    test "processes sheets with template" do
      {:ok, sheets} = Reader.read(@fixture)
      job_id = 992

      UploadJobsMock.create_info(
        &Mox.expect/4,
        job_id,
        %{type: "created", sheet: "type_1"},
        {:ok, nil},
        3
      )

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{templates: %{"type_1" => %{content: []}}},
        job_id: job_id,
        lang: "en"
      }

      assert {:ok, result} = BulkLoad.bulk_load(sheets, ctx)
      assert result.insert_count == 3
      assert result.error_count == 0
    end
  end
end
