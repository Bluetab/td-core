defmodule TdCore.TestSupport.CacheHelpers do
  @moduledoc """
  Support creation of domains in cache
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias TdCache.AclCache
  alias TdCache.Permissions
  alias TdCache.Redix
  alias TdCache.TaxonomyCache
  alias TdCache.UserCache

  def insert_domain(params \\ %{}) do
    %{id: domain_id} = domain = build(:domain, params)
    on_exit(fn -> TaxonomyCache.delete_domain(domain_id, clean: true) end)
    TaxonomyCache.put_domain(domain)
    domain
  end

  def insert_user(params \\ %{}) do
    %{id: id} = user = build(:user, params)
    on_exit(fn -> UserCache.delete(id) end)
    {:ok, _} = UserCache.put(user)
    user
  end

  def insert_acl(domain_id, role, user_ids) do
    on_exit(fn ->
      AclCache.delete_acl_roles("domain", domain_id)
      AclCache.delete_acl_role_users("domain", domain_id, role)
    end)

    AclCache.set_acl_roles("domain", domain_id, [role])
    AclCache.set_acl_role_users("domain", domain_id, role, user_ids)
    :ok
  end

  def put_permission_on_role(permission, role_name) do
    put_permissions_on_roles(%{permission => [role_name]})
  end

  def put_permissions_on_roles(permissions) do
    TdCache.Permissions.put_permission_roles(permissions)
  end

  def put_session_permissions(%{} = claims, domain_id, permissions) do
    domain_ids_by_permission = Map.new(permissions, &{to_string(&1), [domain_id]})
    put_session_permissions(claims, domain_ids_by_permission)
  end

  def put_session_permissions(%{jti: session_id, exp: exp}, %{} = domain_ids_by_permission) do
    put_sessions_permissions(session_id, exp, domain_ids_by_permission)
  end

  def put_sessions_permissions(session_id, exp, domain_ids_by_permission) do
    on_exit(fn -> Redix.del!("session:#{session_id}:domain:permissions") end)

    Permissions.cache_session_permissions!(session_id, exp, %{
      "domain" => domain_ids_by_permission
    })
  end

  def put_default_permissions(permissions) do
    on_exit(fn -> TdCache.Permissions.put_default_permissions([]) end)
    TdCache.Permissions.put_default_permissions(permissions)
  end

  defp build(:domain, params) do
    {id, string_id} = next_unique_ids()

    Enum.into(params, %{
      name: "domain_name" <> string_id,
      id: id,
      external_id: "domain_external_id" <> string_id,
      updated_at: DateTime.utc_now(),
      parent_id: nil
    })
  end

  defp build(:user, params) do
    {id, string_id} = next_unique_ids()

    Enum.into(
      params,
      %{
        id: id,
        role: "user",
        user_name: "user_name" <> string_id,
        full_name: "full_name" <> string_id,
        external_id: "user_external_id" <> string_id,
        email: "email" <> string_id <> "@example.com"
      }
    )
  end

  defp next_unique_ids do
    id = System.unique_integer([:positive])
    string_id = Integer.to_string(id)
    {id, string_id}
  end
end
