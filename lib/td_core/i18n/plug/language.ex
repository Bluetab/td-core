defmodule TdCore.I18n.Plug.Language do
  @moduledoc """
  A plug to assign locale to the locale key in the connection
  """

  alias Plug.Conn

  def init(opts), do: opts

  def call(conn, _opts) do
    case Conn.get_req_header(conn, "accept-language") do
      [locale | _] ->
        Plug.Conn.assign(conn, :locale, locale)

      _ ->
        conn
    end
  end
end
