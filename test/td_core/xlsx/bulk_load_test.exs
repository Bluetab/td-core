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

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{templates: %{"type_1" => nil}},
        job_id: job_id,
        lang: "en"
      }

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

  describe "bulk_load/2 header resolution with translate_fn" do
    @job_id 993

    defp run_sheet(headers, rows) do
      UploadJobsMock.create_info(
        &Mox.expect/4,
        @job_id,
        %{type: "created"},
        {:ok, nil},
        length(rows)
      )

      ctx = %{impl_for: %TranslatingBulkLoadImpl{}, job_id: @job_id, lang: "en"}
      assert {:ok, result} = BulkLoad.bulk_load(%{"sheet" => {headers, rows}}, ctx)
      result
    end

    defp received_items do
      receive do
        {:item, item} -> [item | received_items()]
      after
        0 -> []
      end
    end

    test "accepts technical header names only" do
      result = run_sheet(["external_id", "name", "other"], [["e1", "n1", "o1"]])

      assert result.insert_count == 1
      assert [item] = received_items()
      assert %{"external_id" => "e1", "name" => "n1"} = item
      assert %{"other" => %{"value" => "o1"}} = item["df_content"]
    end

    test "accepts translated labels only" do
      result = run_sheet(["ID", "Name", "other"], [["e1", "n1", "o1"]])

      assert result.insert_count == 1
      assert [item] = received_items()
      assert %{"external_id" => "e1", "name" => "n1"} = item
      assert %{"other" => %{"value" => "o1"}} = item["df_content"]
    end

    test "accepts a mix of technical names and labels in the same sheet" do
      result = run_sheet(["ID", "name"], [["e1", "n1"]])

      assert result.insert_count == 1
      assert [%{"external_id" => "e1", "name" => "n1"}] = received_items()

      result = run_sheet(["external_id", "Name"], [["e2", "n2"]])

      assert result.insert_count == 1
      assert [%{"external_id" => "e2", "name" => "n2"}] = received_items()
    end

    test "discarded header is recognised by technical name" do
      run_sheet(["external_id", "status"], [["e1", "s1"]])

      assert [item] = received_items()
      refute Map.has_key?(item, "status")
      refute Map.has_key?(item["df_content"], "status")
    end

    test "discarded header is recognised by label" do
      run_sheet(["external_id", "Status"], [["e1", "s1"]])

      assert [item] = received_items()
      refute Map.has_key?(item, "Status")
      refute Map.has_key?(item["df_content"], "Status")
    end

    test "collision on a discarded column: the label wins, the technical name stays as template data" do
      run_sheet(["external_id", "status", "Status"], [["e1", "s1", "s2"]])

      assert [item] = received_items()
      refute Map.has_key?(item["df_content"], "Status")
      assert %{"status" => %{"value" => "s1"}} = item["df_content"]
    end

    test "reports the label of missing required headers" do
      UploadJobsMock.create_error(
        &Mox.expect/4,
        @job_id,
        %{type: "missing_required_headers", details: %{missing_headers: ["ID"]}},
        {:ok, nil},
        1
      )

      ctx = %{impl_for: %TranslatingBulkLoadImpl{}, job_id: @job_id, lang: "en"}

      assert {:ok, %{invalid_sheet_count: 1, insert_count: 0}} =
               BulkLoad.bulk_load(%{"sheet" => {["name"], [["n1"]]}}, ctx)
    end

    test "collision: label and technical name of the same column, the label wins" do
      # Previous behaviour: only the label was recognised, the technical name
      # was treated as any other (template) column.
      run_sheet(["external_id", "ID", "Name"], [["tech", "label", "n1"]])

      assert [item] = received_items()
      assert item["external_id"] == "label"
      assert %{"external_id" => %{"value" => "tech"}} = item["df_content"]
    end

    test "collision: technical name placed after the label gives the same result" do
      run_sheet(["ID", "Name", "external_id"], [["label", "n1", "tech"]])

      assert [item] = received_items()
      assert item["external_id"] == "label"
      assert %{"external_id" => %{"value" => "tech"}} = item["df_content"]
    end

    test "collision: duplicated header keeps the first column" do
      run_sheet(["ID", "Name", "ID"], [["first", "n1", "second"]])

      assert [item] = received_items()
      assert item["external_id"] == "first"
    end
  end

  describe "bulk_load/2 with bulk_load_items" do
    @job_id 994

    defp batch_sheets(ids) do
      %{"sheet" => {["external_id"], Enum.map(ids, &[&1])}}
    end

    defp batch_ctx(mode) do
      %{impl_for: %BatchBulkLoadImpl{mode: mode, test_pid: self()}, job_id: @job_id, lang: "en"}
    end

    defp capture_upload_events do
      test_pid = self()

      Mox.stub(MockClusterHandler, :call!, fn :audit,
                                              TdAudit.UploadJobs,
                                              event,
                                              [@job_id, attrs] ->
        send(test_pid, {:upload_event, event, attrs})
        {:ok, nil}
      end)
    end

    defp upload_events do
      Stream.repeatedly(fn ->
        receive do
          {:upload_event, event, attrs} -> {event, attrs}
        after
          0 -> nil
        end
      end)
      |> Enum.take_while(& &1)
    end

    test "emits events and counts results as the row-by-row mode" do
      capture_upload_events()

      assert {:ok, result} =
               BulkLoad.bulk_load(
                 batch_sheets(["created", "updated", "unchanged", "error"]),
                 batch_ctx(:mixed)
               )

      assert %{insert_count: 1, update_count: 1, unchanged_count: 1, error_count: 1} = result

      assert [
               {:create_info, %{type: "created"}},
               {:create_info, %{type: "updated"}},
               {:create_info, %{type: "unchanged"}},
               {:create_error, %{type: "invalid_item"}}
             ] = upload_events()

      assert_receive {:on_complete, ids}
      assert Enum.sort(ids) == ["created-id", "extra-id", "updated-id"]
    end

    test "marks every item with an unexpected error when the batch result has a different length" do
      capture_upload_events()

      assert {:ok, %{error_count: 2, insert_count: 0}} =
               BulkLoad.bulk_load(batch_sheets(["first", "second"]), batch_ctx(:wrong_length))

      refute_received {:on_complete, _}

      assert [
               {:create_error, %{type: "unexpected_error"}},
               {:create_error, %{type: "unexpected_error"}}
             ] =
               upload_events()
    end

    test "marks every item with an unexpected error when the batch function raises" do
      capture_upload_events()

      assert {:ok, %{error_count: 2, insert_count: 0}} =
               BulkLoad.bulk_load(batch_sheets(["first", "second"]), batch_ctx(:raises))

      refute_received {:on_complete, _}

      assert [
               {:create_error, %{type: "unexpected_error", details: %{message: "batch failed"}}},
               {:create_error, %{type: "unexpected_error", details: %{message: "batch failed"}}}
             ] = upload_events()
    end

    test "does not re-emit batch errors when notifying a result raises" do
      expect(MockClusterHandler, :call!, 1, fn :audit,
                                               TdAudit.UploadJobs,
                                               :create_info,
                                               [@job_id, _attrs] ->
        raise "event failed"
      end)

      assert_raise RuntimeError, "event failed", fn ->
        BulkLoad.bulk_load(batch_sheets(["first", "second"]), batch_ctx(:created_then_emit_fails))
      end
    end

    test "keeps the row-by-row mode when the batch option is absent" do
      UploadJobsMock.create_info(&Mox.expect/4, @job_id, %{type: "created"}, {:ok, nil}, 2)

      assert {:ok, %{insert_count: 2, error_count: 0}} =
               BulkLoad.bulk_load(batch_sheets(["first", "second"]), batch_ctx(:without_batch))

      assert_receive {:on_complete, ["second", "first"]}
    end
  end
end
