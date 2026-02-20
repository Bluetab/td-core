defmodule TdCore.Search.Permissions do
  @moduledoc """
  Maps session permissions to search scopes
  """

  alias TdCore.Search.Query

  def filter_for_permissions(permissions, claims, opts \\ []) do
    permissions
    |> get_search_permissions(claims)
    |> Map.values()
    |> Enum.reject(&(&1 == :all))
    |> Enum.reduce_while([], fn
      :none, _ -> {:halt, nil}
      domains, [] -> {:cont, MapSet.new(domains)}
      domains, acc -> {:cont, MapSet.intersection(acc, MapSet.new(domains))}
    end)
    |> case do
      [] -> [Query.build_permission_filters(:all)]
      nil -> [Query.build_permission_filters(:none)]
      domains -> [Query.build_permission_filters(MapSet.to_list(domains), opts)]
    end
  end

  def get_search_permissions(permissions, claims, resource_type \\ "domain")

  def get_search_permissions(permissions, %{role: role} = _claims, _resource_type)
      when role in ["admin", "service"] and is_list(permissions) do
    Map.new(permissions, &{&1, :all})
  end

  def get_search_permissions(permissions, claims, resource_type) when is_list(permissions) do
    permissions
    |> Map.new(&{&1, :none})
    |> do_get_search_permissions(claims, resource_type)
  end

  defp do_get_search_permissions(defaults, %{jti: jti} = _claims, resource_type) do
    session_permissions = TdCache.Permissions.get_session_permissions(jti)
    default_permissions = get_default_permissions(defaults)

    session_permissions
    |> Map.get(resource_type, %{})
    |> Map.take(Map.keys(defaults))
    |> Map.merge(default_permissions, fn
      _, _, :all -> :all
      _, scope, _ -> scope
    end)
  end

  defp get_default_permissions(defaults) do
    case TdCache.Permissions.get_default_permissions() do
      {:ok, permissions} -> Enum.reduce(permissions, defaults, &Map.replace(&2, &1, :all))
      _ -> defaults
    end
  end
end
