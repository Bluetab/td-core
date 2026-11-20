defprotocol TdCore.XLSX.BulkLoadProtocol do
  @moduledoc """
  `get_opts/1` may return `:preflight`, a `fn sheets, ctx -> {:ok, sheets} | {:error, reason} end` run on the raw sheets before header validation.
  """

  def get_opts(impl_for)
  def bulk_load_item(impl_for, data, ctx)
  def on_complete(impl_for, ids)
  def sheets_to_templates(impl_for, sheets)
end
