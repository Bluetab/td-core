defmodule TdCore.XLSX.Reader do
  @moduledoc """
  A module responsible for reading and parsing XLSX files.

  This module provides functionality to extract data from XLSX spreadsheets using
  the `XlsxReader` library and process it into a structured format for bulk updates.

  ## Functionality
  - Reads an XLSX file and extracts data from its sheets.
  - Transforms the data into a format suitable for processing.

  ## Functions

  - `read/1` - Reads and processes an XLSX file, returning parsed data.
  - `normalize_line_endings/1` - Converts Excel CRLF/CR line endings to LF.
  """

  def read(path) do
    with {:ok, package} <- XlsxReader.open(path),
         sheet_names <- XlsxReader.sheet_names(package) do
      sheet_names
      |> Enum.map(&parse_sheet(&1, package))
      |> Enum.filter(fn
        {_, {_, [_ | _]}} -> true
        _ -> false
      end)
      |> case do
        [] -> {:error, :empty_sheets}
        data -> {:ok, Enum.into(data, %{})}
      end
    else
      {:error, "invalid zip file"} -> {:error, :invalid_format}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Converts Excel CRLF (`\\r\\n`) and CR (`\\r`) line endings to LF (`\\n`).

  Excel stores multiline cells as CRLF, while Truedat stores LF. Without this
  conversion, re-uploading an unchanged download would mark markdown/textarea
  fields as updated.
  """
  def normalize_line_endings(value) when is_binary(value) do
    value
    |> String.replace("\r\n", "\n")
    |> String.replace("\r", "\n")
  end

  def normalize_line_endings(value), do: value

  defp parse_sheet(sheet, package) do
    case XlsxReader.sheet(package, sheet, number_type: String) do
      {:ok, [headers | content]} ->
        headers = Enum.map(headers, &normalize_line_endings/1)
        headers_len = length(headers)

        sheet_data =
          content
          |> Enum.reject(&Enum.all?(&1, fn x -> x == "" end))
          |> Enum.map(&create_row(&1, headers_len))

        {sheet, {headers, sheet_data}}

      _ ->
        nil
    end
  end

  defp create_row(row, headers_len) do
    row = Enum.map(row, &normalize_line_endings/1)
    row ++ List.duplicate(nil, max(0, headers_len - length(row)))
  end

  def move_file!(path, upload_dir) do
    :ok = File.mkdir_p!(upload_dir)
    source_file_name = path |> Path.split() |> List.last()
    file_path = Path.join([upload_dir, source_file_name])
    :ok = File.rename!(path, file_path)
    file_path
  end
end
