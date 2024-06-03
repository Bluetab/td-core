defmodule TdCore.MixProject do
  use Mix.Project

  def project do
    [
      app: :td_core,
      version: "6.1.3",
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {TdCore.Application, []},
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:plug, "~> 1.15"},
      {:jason, "~> 1.1"},
      {:guardian, "~> 2.0"},
      {:mox, "~> 1.0", only: :test},
      {:ex_machina, "~> 2.7"},
      {:elasticsearch, "~> 1.1"},
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false},
      {:td_cluster, git: "https://github.com/Bluetab/td-cluster.git", tag: "5.19.0"},
      {:td_cache, git: "https://github.com/Bluetab/td-cache.git", tag: "5.20.0"},
      {:td_df_lib, git: "https://github.com/Bluetab/td-df-lib.git", tag: "6.1.0"}
    ]
  end
end
