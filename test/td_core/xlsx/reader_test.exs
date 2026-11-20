defmodule TdCore.XLSX.ReaderTest do
  @moduledoc """
  Tests for the XLSX Reader module.
  """
  use ExUnit.Case

  alias Elixlsx.Sheet
  alias Elixlsx.Workbook
  alias TdCore.XLSX.Reader

  @xlsx_path "test/fixtures/xlsx"

  defp xlsx_path(file) do
    Path.join(@xlsx_path, "#{file}.xlsx")
  end

  defp tmp_xlsx_path do
    Path.join(System.tmp_dir!(), "td_core_xlsx_#{System.unique_integer([:positive])}.xlsx")
  end

  describe "read/2" do
    test "processes xlsx" do
      {:ok, result} =
        "upload_tiny"
        |> xlsx_path()
        |> Reader.read()

      assert %{
               "type_1" =>
                 {["external_id", "text" | _],
                  [
                    ["ex_id1", "text" | _],
                    ["ex_id2", "text2" | _],
                    ["ex_id3", "" | _]
                  ]}
             } = result
    end

    test "processes multiple sheets sequentially" do
      {:ok, result} =
        "upload"
        |> xlsx_path()
        |> Reader.read()

      assert is_map(result)

      assert %{
               "type_1" => {_, [_ | _] = type_1},
               "type_2" => {_, [_ | _] = type_2}
             } = result

      assert length(type_1) == 11
      assert length(type_2) == 10
    end

    test "handles empty sheets" do
      assert {:error, :empty_sheets} =
               "empty"
               |> xlsx_path()
               |> Reader.read()
    end

    test "handles XLSX file open errors" do
      assert {:error, "file not found"} = Reader.read("nonexistent.xlsx")
    end

    test "handles rows with missing columns by padding with empty strings" do
      {:ok, result} =
        "missing_column"
        |> xlsx_path()
        |> Reader.read()

      assert %{
               "type_1" =>
                 {[
                    "external_id",
                    "text",
                    "name",
                    "tech_name",
                    "alias_name",
                    "link_to_structure",
                    "domain",
                    "type",
                    "path",
                    "value1",
                    "value2",
                    "value3"
                  ],
                  [
                    [
                      "ex_id1",
                      "text",
                      "structure_1",
                      "tech_structure_1",
                      "alias_structure_1",
                      "http://test.truedat.io/structures/1",
                      "domain",
                      "type_1",
                      "system > structure_1",
                      "a",
                      "b",
                      "c"
                    ],
                    [
                      "ex_id2",
                      "text2",
                      "structure_2",
                      "tech_structure_2",
                      "alias_structure_2",
                      "http://test.truedat.io/structures/2",
                      "domain",
                      "type_1",
                      "system > structure_2",
                      "a",
                      "b",
                      nil
                    ],
                    [
                      "ex_id3",
                      "",
                      "structure_3",
                      "tech_structure_3",
                      "alias_structure_3",
                      "http://test.truedat.io/structures/3",
                      "domain",
                      "type_1",
                      "system > structure_3",
                      "a",
                      nil,
                      nil
                    ]
                  ]}
             } = result
    end
  end

  describe "error handling and edge cases" do
    test "handles malformed XLSX files gracefully" do
      assert {:error, :invalid_format} =
               "invalid"
               |> xlsx_path()
               |> Reader.read()
    end

    test "handles sheets with only headers" do
      {:error, :empty_sheets} =
        "only_headers"
        |> xlsx_path()
        |> Reader.read()
    end

    test "handles sheets with empty rows" do
      {:ok, result} =
        "empty_rows"
        |> xlsx_path()
        |> Reader.read()

      assert %{
               "type_1" =>
                 {["external_id", "text" | _],
                  [
                    ["ex_id1", "text" | _],
                    ["ex_id3", "" | _]
                  ]}
             } = result
    end

    test "normalizes Excel CRLF and CR line endings to LF in cells and headers" do
      path = tmp_xlsx_path()

      try do
        %Workbook{
          sheets: [
            %Sheet{
              name: "type_1",
              rows: [
                ["external_id", "markdown\r\nfield"],
                ["ex_1", "line1\r\nline2\rline3"],
                ["ex_2", "already\nlf"],
                ["ex_3", ""]
              ]
            }
          ]
        }
        |> Elixlsx.write_to(path)

        {:ok, result} = Reader.read(path)

        assert %{
                 "type_1" =>
                   {["external_id", "markdown\nfield"],
                    [
                      ["ex_1", "line1\nline2\nline3"],
                      ["ex_2", "already\nlf"],
                      ["ex_3", ""]
                    ]}
               } = result
      after
        File.rm(path)
      end
    end

    test "handles special characters in sheet names" do
      {:ok, result} =
        "special_character"
        |> xlsx_path()
        |> Reader.read()

      assert %{
               "spéciâl" =>
                 {[
                    "external_id",
                    "text",
                    "name",
                    "tech_name",
                    "alias_name",
                    "link_to_structure",
                    "domain",
                    "type",
                    "path",
                    "special_char"
                  ],
                  [
                    [
                      "ex_id1",
                      "text",
                      "structure_1",
                      "tech_structure_1",
                      "alias_structure_1",
                      "http://test.truedat.io/structures/1",
                      "domain",
                      "type_1",
                      "system > structure_1",
                      "spéciâl"
                    ]
                  ]}
             } = result
    end
  end

  describe "normalize_line_endings/1" do
    test "converts CRLF and CR to LF in binaries" do
      assert Reader.normalize_line_endings("line1\r\nline2") == "line1\nline2"
      assert Reader.normalize_line_endings("line1\rline2") == "line1\nline2"
      assert Reader.normalize_line_endings("line1\nline2") == "line1\nline2"
      assert Reader.normalize_line_endings("no breaks") == "no breaks"
    end

    test "handles empty, leading, trailing and consecutive line endings" do
      assert Reader.normalize_line_endings("") == ""
      assert Reader.normalize_line_endings("\r\n") == "\n"
      assert Reader.normalize_line_endings("\r") == "\n"
      assert Reader.normalize_line_endings("\n") == "\n"
      assert Reader.normalize_line_endings("\r\nline") == "\nline"
      assert Reader.normalize_line_endings("line\r\n") == "line\n"
      assert Reader.normalize_line_endings("a\r\n\r\nb") == "a\n\nb"
      assert Reader.normalize_line_endings("a\r\n\rb") == "a\n\nb"
    end

    test "does not turn CRLF into two LFs" do
      assert Reader.normalize_line_endings("a\r\nb") == "a\nb"
      refute Reader.normalize_line_endings("a\r\nb") == "a\n\nb"
    end

    test "leaves non-binary values unchanged" do
      assert Reader.normalize_line_endings(nil) == nil
      assert Reader.normalize_line_endings(12) == 12
      assert Reader.normalize_line_endings(12.5) == 12.5
    end
  end

  describe "move_file!/2" do
    test "creates and moves file to directory" do
      source = xlsx_path("upload_tiny")
      temp_file = xlsx_path("move_file_test")
      File.cp!(source, temp_file)

      target_dir = "nonexistent_folder"
      target = Path.join(target_dir, "move_file_test.xlsx")

      assert Reader.move_file!(temp_file, target_dir) == target
      assert File.exists?(target)
      # After move, temp_file should not exist
      refute File.exists?(temp_file)
      # Clean up
      File.rm_rf(target_dir)
    end
  end
end
