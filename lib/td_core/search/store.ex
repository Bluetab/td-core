defmodule TdCore.Search.Store do
  @moduledoc """
  Store for Elasticsearch
  """

  def fetch(:concepts, ids) do
    {:ok, data} = TdCluster.TdBg.Store.fetch(ids)
    data
  end
end
