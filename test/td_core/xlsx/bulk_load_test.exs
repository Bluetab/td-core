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
    test "validates header-only sheets from direct callers" do
      data_sheet = {"type_1", {["external_id"], [["id-1"]]}}
      header_only_sheet = {"type_2", {["wrong_header"], []}}

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{templates: %{"type_1" => %{content: []}}},
        job_id: 998,
        lang: "en"
      }

      UploadJobsMock.create_info(&Mox.expect/4, ctx.job_id, %{type: "created"})

      assert {:ok, expected} = BulkLoad.bulk_load([data_sheet], ctx)

      UploadJobsMock.create_error(
        &Mox.expect/4,
        ctx.job_id,
        %{type: "missing_required_headers", sheet: "type_2"}
      )

      UploadJobsMock.create_info(&Mox.expect/4, ctx.job_id, %{type: "created"})

      assert {:ok, result} = BulkLoad.bulk_load([data_sheet, header_only_sheet], ctx)
      assert result.invalid_sheet_count == 1
      assert expected.invalid_sheet_count == 0
    end

    test "preflight sees header-only sheets and rejection takes precedence over empty_sheets" do
      sheets = [{"type_2", {["name_es"], []}}]

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{
          observer: self(),
          opts: [preflight: fn ^sheets, _ctx -> {:error, :invalid_format} end]
        },
        job_id: 999,
        lang: "en"
      }

      assert BulkLoad.bulk_load(sheets, ctx) == {:error, :invalid_format}
      refute_received {:sheets_to_templates, _}
      refute_received {:bulk_load_item, _, _}
      refute_received {:on_complete, _}
    end

    test "validates header-only sheets after a successful direct preflight" do
      sheets = [{"type_2", {["wrong_header"], []}}]

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{
          observer: self(),
          opts: [preflight: fn ^sheets, _ctx -> {:ok, sheets} end]
        },
        job_id: 1000,
        lang: "en"
      }

      UploadJobsMock.create_error(
        &Mox.expect/4,
        ctx.job_id,
        %{type: "missing_required_headers", sheet: "type_2"}
      )

      assert {:ok, %{invalid_sheet_count: 1}} = BulkLoad.bulk_load(sheets, ctx)
      assert_received {:sheets_to_templates, []}
    end

    test "does not enable preflight from active_locales in extra_context" do
      sheets = [{"type_1", {["external_id", "name_es"], [["id-1", "Nombre"]]}}]

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{
          observer: self(),
          templates: %{"type_1" => %{content: []}},
          opts: [extra_context: %{active_locales: ["es", "en"]}]
        },
        job_id: 993,
        lang: "en"
      }

      UploadJobsMock.create_info(&Mox.expect/4, ctx.job_id, %{type: "created"})

      assert {:ok,
              %{
                insert_count: 1,
                update_count: 0,
                error_count: 0,
                unchanged_count: 0,
                invalid_sheet_count: 0
              }} = BulkLoad.bulk_load(sheets, ctx)

      assert_received {:bulk_load_item, item, %{active_locales: ["es", "en"]}}
      assert item["external_id"] == "id-1"
      assert item["df_content"] == %{"name_es" => %{"value" => "Nombre", "origin" => "file"}}
      assert_received {:on_complete, ["id-1"]}
    end

    test "preflight normalizes required headers before validating and processing" do
      sheets = [{"type_1", {["Identifier", "name"], [["id-1", "Name"]]}}]

      preflight = fn [{sheet, {["Identifier" | headers], rows}}], _ctx ->
        {:ok, [{sheet, {["external_id" | headers], rows}}]}
      end

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{
          observer: self(),
          templates: %{"type_1" => %{content: []}},
          opts: [preflight: preflight]
        },
        job_id: 994,
        lang: "en"
      }

      UploadJobsMock.create_info(&Mox.expect/4, ctx.job_id, %{type: "created", row_number: 2})

      assert {:ok, %{insert_count: 1, error_count: 0, invalid_sheet_count: 0}} =
               BulkLoad.bulk_load(sheets, ctx)

      assert_received {:bulk_load_item, %{"external_id" => "id-1", "name" => "Name"}, _ctx}
      assert_received {:on_complete, ["id-1"]}
    end

    test "preflight receives all sheets and original context once before extra_context overrides" do
      sheets = [
        {"type_1", {["external_id"], [["id-1"]]}},
        {"type_2", {["external_id"], []}}
      ]

      preflight = fn raw_sheets, ctx ->
        send(self(), {:preflight, raw_sheets, ctx})
        {:ok, raw_sheets}
      end

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{
          observer: self(),
          templates: %{"type_1" => %{content: []}},
          opts: [
            preflight: preflight,
            extra_context: %{lang: "es", active_locales: ["es"], marker: :extra}
          ]
        },
        job_id: 995,
        lang: "en",
        marker: :original
      }

      UploadJobsMock.create_info(&Mox.expect/4, ctx.job_id, %{type: "created"})

      assert {:ok, %{insert_count: 1}} = BulkLoad.bulk_load(sheets, ctx)
      assert_received :get_opts
      assert_received {:preflight, ^sheets, ^ctx}
      assert_received {:sheets_to_templates, ["type_1", "type_2"]}
      assert_received {:bulk_load_item, _, %{lang: "es", active_locales: ["es"], marker: :extra}}
      refute_received :get_opts
      refute_received {:preflight, _, _}
    end

    test "preflight map error becomes invalid_format before any sheet or row effects" do
      sheets = [
        {"type_1", {["external_id"], [["id-1"]]}},
        {"type_2", {["wrong_header"], [["id-2"]]}}
      ]

      reason = %{reason: "bad"}

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{
          observer: self(),
          templates: %{"type_1" => %{content: []}},
          opts: [preflight: fn ^sheets, _ctx -> {:error, reason} end]
        },
        job_id: 996,
        lang: "en"
      }

      assert ExUnit.CaptureLog.capture_log(fn ->
               assert BulkLoad.bulk_load(sheets, ctx) == {:error, :invalid_format}
             end) == ""

      assert_received :get_opts
      refute_received :get_opts
      refute_received {:sheets_to_templates, _}
      refute_received {:bulk_load_item, _, _}
      refute_received {:on_complete, _}
    end

    test "malformed preflight result fails without processing sheets or rows" do
      sheets = [{"type_1", {["external_id"], [["id-1"]]}}]

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{
          observer: self(),
          templates: %{"type_1" => %{content: []}},
          opts: [preflight: fn ^sheets, _ctx -> :invalid end]
        },
        job_id: 1004,
        lang: "en"
      }

      assert BulkLoad.bulk_load(sheets, ctx) == {:error, :invalid_format}
      refute_received {:sheets_to_templates, _}
      refute_received {:bulk_load_item, _, _}
      refute_received {:on_complete, _}
    end

    test "preflight {:ok, nil} fails without processing sheets or rows" do
      sheets = [{"type_1", {["external_id"], [["id-1"]]}}]

      ctx = %{
        impl_for: %ConfigurableBulkLoadImpl{
          observer: self(),
          templates: %{"type_1" => %{content: []}},
          opts: [preflight: fn ^sheets, _ctx -> {:ok, nil} end]
        },
        job_id: 1006,
        lang: "en"
      }

      assert BulkLoad.bulk_load(sheets, ctx) == {:error, :invalid_format}
      refute_received {:sheets_to_templates, _}
      refute_received {:bulk_load_item, _, _}
      refute_received {:on_complete, _}
    end

    for malformed_result <- [{:ok, [:bad]}, {:error, {:bad, :reason}}] do
      test "preflight #{inspect(malformed_result)} fails safely" do
        sheets = [{"type_1", {["external_id"], [["id-1"]]}}]

        ctx = %{
          impl_for: %ConfigurableBulkLoadImpl{
            observer: self(),
            templates: %{"type_1" => %{content: []}},
            opts: [preflight: fn ^sheets, _ctx -> unquote(Macro.escape(malformed_result)) end]
          },
          job_id: 1007,
          lang: "en"
        }

        assert BulkLoad.bulk_load(sheets, ctx) == {:error, :invalid_format}
        refute_received {:sheets_to_templates, _}
        refute_received {:bulk_load_item, _, _}
        refute_received {:on_complete, _}
      end
    end

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
end
