defmodule TdCore.Utils.QueryUtils do
  @moduledoc """
  This module contains functions to help in Ecto.Query based functions
  """
  import Ecto.Query

  def maybe_add_limit(query, %{size: limit}),
    do: maybe_add_limit(query, %{"size" => limit})

  def maybe_add_limit(query, %{"size" => limit}) when is_binary(limit),
    do: maybe_add_limit(query, %{"size" => String.to_integer(limit)})

  def maybe_add_limit(query, %{"size" => limit}) when is_integer(limit),
    do: limit(query, ^limit)

  def maybe_add_limit(query, _),
    do: query

  def maybe_order_by(query, %{min_id: _}),
    do: order_by(query, asc: :id)

  def maybe_order_by(query, %{"min_id" => _}),
    do: order_by(query, asc: :id)

  def maybe_order_by(query, params) when is_list(params),
    do: maybe_order_by(query, Map.new(params))

  def maybe_order_by(query, _),
    do: query
end
