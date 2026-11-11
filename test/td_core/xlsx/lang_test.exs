defmodule TdCore.XLSX.LangTest do
  use ExUnit.Case, async: true

  alias TdCore.XLSX.Lang

  doctest TdCore.XLSX.Lang

  describe "normalize/1" do
    test "returns the lowercase primary subtag" do
      assert Lang.normalize("es") == "es"
      assert Lang.normalize("es-ES") == "es"
      assert Lang.normalize("EN_us") == "en"
      assert Lang.normalize(" es-ES ") == "es"
    end

    test "takes the first language of an Accept-Language value" do
      assert Lang.normalize("es-ES,es;q=0.9,en;q=0.8") == "es"
      assert Lang.normalize("en;q=0.5") == "en"
    end

    test "returns nil when there is no usable language" do
      assert Lang.normalize(nil) == nil
      assert Lang.normalize("") == nil
      assert Lang.normalize("*") == nil
      assert Lang.normalize(1) == nil
    end
  end

  describe "resolve/3" do
    test "an explicit lang wins over the locale and the default" do
      assert Lang.resolve("es", "en", "en") == "es"
    end

    test "uses the locale when there is no lang" do
      assert Lang.resolve(nil, "es-ES,es;q=0.9", "en") == "es"
      assert Lang.resolve("", "es", "en") == "es"
    end

    test "uses the default when there is neither lang nor locale" do
      assert Lang.resolve(nil, nil, "en") == "en"
      assert Lang.resolve("*", "*", "en") == "en"
    end

    test "normalizes the lang" do
      assert Lang.resolve("es-ES", nil, "en") == "es"
    end
  end
end
