defprotocol TdCore.XLSX.BulkLoadProtocol do
  @doc """
  Returns bulk load options. Besides headers, translation and extra context,
  options may include `:bulk_load_items`, a function that receives all parsed
  items and the context and returns `{item, result}` pairs in the same order.
  Each result has the same format as `bulk_load_item/3`.
  """
  def get_opts(impl_for)
  def bulk_load_item(impl_for, data, ctx)
  def on_complete(impl_for, ids)
  def sheets_to_templates(impl_for, sheets)
end
