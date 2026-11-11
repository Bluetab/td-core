defmodule TdCore.XLSX.DownloadTest do
  @moduledoc """
  Tests for the shared XLSX download helpers.
  """
  use ExUnit.Case

  import Plug.Test

  alias TdCore.XLSX.Download

  doctest Download

  describe "sanitize_sheet_name/1" do
    test "replaces characters not allowed in sheet names" do
      assert Download.sanitize_sheet_name("a[b]c:d*e?f/g\\h") == "a_b_c_d_e_f_g_h"
    end

    test "truncates to 31 characters" do
      name = String.duplicate("x", 40)
      assert Download.sanitize_sheet_name(name) == String.duplicate("x", 31)
    end

    test "keeps valid names unchanged" do
      assert Download.sanitize_sheet_name("type_1") == "type_1"
    end

    test "returns Sheet for empty or non binary values" do
      assert Download.sanitize_sheet_name("") == "Sheet"
      assert Download.sanitize_sheet_name(nil) == "Sheet"
      assert Download.sanitize_sheet_name(:atom) == "Sheet"
    end
  end

  describe "content_disposition/1" do
    test "quotes the filename" do
      assert Download.content_disposition("concepts.xlsx") ==
               ~s(attachment; filename="concepts.xlsx")
    end

    test "replaces quotes, backslashes and line breaks" do
      assert Download.content_disposition(~s(a"b\\c\nd.xlsx)) ==
               ~s(attachment; filename="a_b_c_d.xlsx")
    end
  end

  describe "send_xlsx/3" do
    test "sends the blob with xlsx content type and quoted content-disposition" do
      conn = conn(:get, "/") |> Download.send_xlsx("notes.xlsx", "BLOB")

      assert conn.status == 200
      assert conn.resp_body == "BLOB"

      assert ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;charset=utf-8"] ==
               Plug.Conn.get_resp_header(conn, "content-type")

      assert Plug.Conn.get_resp_header(conn, "content-disposition") ==
               [~s(attachment; filename="notes.xlsx")]
    end
  end
end
