defprotocol TdCore.XLSX.BulkLoadProtocol do
  @moduledoc """
  Defines XLSX bulk-load implementations.

  `get_opts/1` may include `:preflight`, a callback invoked with the parsed
  sheets and upload context. In worker uploads, its input includes sheets that
  have headers but no data rows. It must return `{:ok, sheets}` where each entry
  is `{binary_sheet_name, {headers, rows}}` and both headers and rows are lists,
  or `{:error, reason}` where reason is an atom or binary. Direct `bulk_load/2`
  callers retain their supplied header-only sheets; workers discard them after a
  successful preflight.
  """

  def get_opts(impl_for)
  def bulk_load_item(impl_for, data, ctx)
  def on_complete(impl_for, ids)
  def sheets_to_templates(impl_for, sheets)
end
