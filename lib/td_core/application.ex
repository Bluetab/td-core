defmodule TdCore.Application do
  @moduledoc false

  use Application

  alias TdCore.Search.IndexWorker

  @impl true
  def start(_type, _args) do
    children =
      [
        TdCore.Search.Cluster
      ] ++ IndexWorker.get_index_workers()

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: TdCore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
