defmodule TdCore.Factory do
  @moduledoc """
  An `ExMachina` factory for `TdCore` tests.
  """

  use ExMachina

  def domain_factory do
    %{
      name: sequence("domain_name"),
      id: System.unique_integer([:positive]),
      external_id: sequence("domain_external_id"),
      updated_at: DateTime.utc_now(),
      parent_id: nil
    }
  end
end
