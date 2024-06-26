defmodule TdCore.Search.Cluster do
  @moduledoc "Elasticsearch cluster configuration"

  use Elasticsearch.Cluster, otp_app: :td_core
  @default_size 500

  @impl GenServer
  def init(%{aliases: aliases, indexes: indexes, default_settings: defaults} = config) do
    indexes =
      Enum.reduce(aliases, %{}, fn
        {index, alias_name}, acc ->
          with config <- Keyword.fetch!(indexes, index),
               settings <- Keyword.fetch!(config, :settings),
               [_ | _] = config <- Keyword.put(config, :settings, Map.merge(settings, defaults)) do
            Map.put(acc, String.to_existing_atom(alias_name), Map.new(config))
          else
            _ -> acc
          end
      end)

    {:ok, %{config | indexes: indexes}}
  end

  @impl GenServer
  def init(config), do: {:ok, config}

  @spec alias_name(atom) :: binary
  def alias_name(name) do
    __MODULE__
    |> Config.get()
    |> Map.get(:aliases)
    |> Map.get(name)
  end

  def setting(index_name, setting_name \\ :settings) do
    with %{aliases: aliases, indexes: indexes} <- Config.get(__MODULE__),
         alias_name <- Map.fetch!(aliases, index_name),
         index_config <- Map.fetch!(indexes, String.to_existing_atom(alias_name)) do
      Map.get(index_config, setting_name)
    end
  end

  def get_size_field(field_type) do
    __MODULE__
    |> Config.get()
    |> get_in([:aggregations, field_type])
    |> case do
      nil -> @default_size
      size -> size
    end
  end
end
