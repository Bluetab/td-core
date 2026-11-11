defmodule TdCore.XLSX.Download do
  @moduledoc """
  Shared helpers for XLSX downloads.

  Entities keep their own sheet writers; this module only provides the small
  pieces that were copied in each of them:

    * `sanitize_sheet_name/1` — valid Excel sheet names.
    * `content_disposition/1` — `content-disposition` header value with a quoted
      filename.
    * `send_xlsx/3` — sends an XLSX binary over a `Plug.Conn`.

  Which status to answer for an empty download (200 or 204) is a decision of each
  endpoint and is intentionally not made here.
  """

  import Plug.Conn

  @content_type "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;charset=utf-8"
  @default_sheet_name "Sheet"
  @max_sheet_name_length 31

  @doc """
  Returns a valid Excel sheet name.

  Characters not allowed by Excel (`[ ] : * ? / \\`) are replaced by `_` and the
  result is truncated to 31 characters. An empty result, or a value that is not a
  binary, yields `"Sheet"`.

  ## Examples

      iex> TdCore.XLSX.Download.sanitize_sheet_name("a/b:c")
      "a_b_c"

      iex> TdCore.XLSX.Download.sanitize_sheet_name("")
      "Sheet"

      iex> TdCore.XLSX.Download.sanitize_sheet_name(nil)
      "Sheet"
  """
  @spec sanitize_sheet_name(term()) :: String.t()
  def sanitize_sheet_name(name) when is_binary(name) do
    name
    |> String.replace(~r/[[\]:*?\/\\]/, "_")
    |> String.slice(0, @max_sheet_name_length)
    |> case do
      "" -> @default_sheet_name
      name -> name
    end
  end

  def sanitize_sheet_name(_), do: @default_sheet_name

  @doc """
  Returns the value of a `content-disposition` header for an attachment, with the
  filename between double quotes.

  Double quotes, backslashes and line breaks in the filename are replaced by `_`
  so that the header stays well formed.

  ## Examples

      iex> TdCore.XLSX.Download.content_disposition("concepts.xlsx")
      ~s(attachment; filename="concepts.xlsx")

      iex> TdCore.XLSX.Download.content_disposition(~s(my "file".xlsx))
      ~s(attachment; filename="my _file_.xlsx")
  """
  @spec content_disposition(String.t()) :: String.t()
  def content_disposition(file_name) when is_binary(file_name) do
    safe = String.replace(file_name, ~r/["\\\r\n]/, "_")
    ~s(attachment; filename="#{safe}")
  end

  @doc """
  Sends an XLSX binary with status 200, the XLSX content type and a
  `content-disposition` header with a quoted filename.
  """
  @spec send_xlsx(Plug.Conn.t(), String.t(), binary()) :: Plug.Conn.t()
  def send_xlsx(%Plug.Conn{} = conn, file_name, blob) when is_binary(blob) do
    conn
    |> put_resp_content_type(@content_type, nil)
    |> put_resp_header("content-disposition", content_disposition(file_name))
    |> send_resp(:ok, blob)
  end
end
