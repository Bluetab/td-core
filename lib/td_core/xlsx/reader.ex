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
  - `move_file!/2` - Moves an uploaded file into the upload directory with mode `0600`.
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

  def move_file!(path, upload_dir), do: move_file!(path, upload_dir, &File.rename/2)

  @doc false
  # `rename` is injectable so the cross-device fallback can be tested without
  # mounting two volumes.
  def move_file!(path, upload_dir, rename) do
    :ok = File.mkdir_p!(upload_dir)
    source_file_name = path |> Path.split() |> List.last()
    file_path = Path.join([upload_dir, source_file_name])
    # Restrict the file before it is moved, so that it is never readable by others
    # in the upload directory. It is best effort (the source may not be ours); the
    # final chmod guarantees the mode of the stored file.
    _ = File.chmod(path, 0o600)
    :ok = move!(path, file_path, rename)
    File.chmod!(file_path, 0o600)
    file_path
  end

  # Plug's temporary file may live on another volume than the upload directory,
  # in which case rename fails with :exdev. Fall back to copy + chmod + remove.
  defp move!(source, target, rename) do
    case rename.(source, target) do
      :ok ->
        :ok

      {:error, :exdev} ->
        File.cp!(source, target)
        File.chmod!(target, 0o600)
        File.rm!(source)
        :ok

      {:error, reason} ->
        raise File.RenameError,
          reason: reason,
          action: "rename",
          source: source,
          destination: target
    end
  end
end
