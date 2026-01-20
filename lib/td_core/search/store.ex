defmodule TdCore.Search.Store do
  @moduledoc """
  Store for Elasticsearch
  """

  def fetch(:concepts, ids) do
    {:ok, data} = TdCluster.TdBg.Store.fetch(ids)
    data
  end

  def fetch(:quality_controls, ids) do
    {:ok, data} = TdCluster.TdQx.Store.fetch(quality_control_ids: ids)
    data
  end

  def fetch(:data_structures, ids) do
    {:ok, data} = TdCluster.TdDd.Store.fetch(ids)
    data
  end
end
