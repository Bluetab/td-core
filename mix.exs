defmodule TdCore.MixProject do
  use Mix.Project

  def project do
    [
      app: :td_core,
      version: "7.11.0",
      elixir: "~> 1.18",
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
      {:plug, "~> 1.16.1"},
      {:jason, "~> 1.4.4"},
      {:guardian, "~> 2.3.2"},
      {:mox, "~> 1.2", only: :test},
      {:elasticsearch, git: "https://github.com/Bluetab/elasticsearch-elixir.git"},
      {:credo, "~> 1.7.11", only: [:dev, :test], runtime: false},
      {:td_cluster, git: "https://github.com/Bluetab/td-cluster.git", branch: "feature/td-7401"},
      {:td_cache, git: "https://github.com/Bluetab/td-cache.git", branch: "feature/td-7401"},
      {:td_df_lib, git: "https://github.com/Bluetab/td-df-lib.git", branch: "feature/td-7401"}
    ]
  end
end
