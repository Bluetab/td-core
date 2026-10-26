defmodule TdCore.Auth.ClaimsTest do
  use ExUnit.Case, async: true

  alias TdCore.Auth.Claims

  describe "Jason.Encoder" do
    test "serializes jti so permission checks survive a JSON round trip" do
      claims = %Claims{
        user_id: 42,
        user_name: "jane",
        role: "user",
        jti: "a-session-jti",
        exp: 1_700_000_000
      }

      decoded =
        claims
        |> Jason.encode!()
        |> Jason.decode!()

      assert decoded["jti"] == "a-session-jti"
      assert decoded["user_id"] == 42
      assert decoded["user_name"] == "jane"
      assert decoded["role"] == "user"
    end
  end
end
