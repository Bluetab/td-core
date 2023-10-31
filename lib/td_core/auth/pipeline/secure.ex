defmodule TdCore.Auth.Pipeline.Secure do
  @moduledoc """
  Plug pipeline for routes requiring authentication
  """

  use Guardian.Plug.Pipeline,
    otp_app: :td_core,
    error_handler: TdCore.Auth.ErrorHandler,
    module: TdCore.Auth.Guardian

  plug(Guardian.Plug.EnsureAuthenticated, claims: %{"aud" => "truedat", "iss" => "tdauth"})
  plug(Guardian.Plug.LoadResource)
  plug(TdCore.Auth.Plug.SessionExists)
  plug(TdCore.Auth.Plug.CurrentResource)
end
