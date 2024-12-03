defprotocol TdCore.Search.ElasticDocumentProtocol do
  def mappings(data)
  def aggregations(data)
  def query(data)
end
