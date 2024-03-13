defmodule TdCore.Application do
  @moduledoc false

  use Application

  alias TdCore.Search.IndexWorker

  @impl true
  def start(_type, _args) do
    children =
      :td_core
      |> Application.get_env(TdCore.Search.Cluster)
      |> workers()

    opts = [strategy: :one_for_one, name: TdCore.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp workers(nil), do: []
  defp workers(_), do: [TdCore.Search.Cluster] ++ IndexWorker.get_index_workers()
end
