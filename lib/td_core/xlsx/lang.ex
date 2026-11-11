defmodule TdCore.XLSX.Lang do
  @moduledoc """
  Language used by the XLSX uploads and downloads.

  The language of a request is resolved in this order:

    1. The `lang` parameter of the request.
    2. The `Accept-Language` header of the request (`conn.assigns[:locale]`, assigned
       by `TdCore.I18n.Plug.Language`).
    3. The default language.

  Languages are compared by their lowercase primary subtag, so `"es-ES"`, `"es_ES"`
  and an `Accept-Language` value such as `"es-ES,es;q=0.9"` all become `"es"`.
  """

  @doc """
  Returns the lowercase primary subtag of `lang`, or `nil` when there is no
  usable language (not a binary, empty, or `*`).

  ## Examples

      iex> TdCore.XLSX.Lang.normalize("es-ES,es;q=0.9")
      "es"

      iex> TdCore.XLSX.Lang.normalize("EN_us")
      "en"

      iex> TdCore.XLSX.Lang.normalize("*")
      nil
  """
  @spec normalize(term()) :: String.t() | nil
  def normalize(lang) when is_binary(lang) do
    primary =
      lang
      |> String.split(",", parts: 2)
      |> hd()
      |> String.split(";", parts: 2)
      |> hd()
      |> String.split(["-", "_"], parts: 2)
      |> hd()
      |> String.trim()
      |> String.downcase()

    if primary in ["", "*"], do: nil, else: primary
  end

  def normalize(_lang), do: nil

  @doc """
  Resolves the language of a request: the normalized `lang_param` when it is usable,
  otherwise the normalized `locale` (`Accept-Language`), otherwise `default`.

  `default` is returned as is (it is the configured language).

  ## Examples

      iex> TdCore.XLSX.Lang.resolve("es-ES", "en", "en")
      "es"

      iex> TdCore.XLSX.Lang.resolve(nil, "es-ES,es;q=0.9", "en")
      "es"

      iex> TdCore.XLSX.Lang.resolve("", nil, "en")
      "en"
  """
  @spec resolve(term(), term(), String.t()) :: String.t()
  def resolve(lang_param, locale, default) do
    normalize(lang_param) || normalize(locale) || default
  end
end
