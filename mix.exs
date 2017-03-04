defmodule ExKcl.Mixfile do
  use Mix.Project

  def project do
    [app: :ex_kcl,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application do
    [extra_applications: [:logger, :hackney, :ex_aws]]
  end

  defp deps do
    [
      {:hackney,   "~> 1.7.0",  override: true},
      {:gen_stage, "~> 0.11.0", override: true},
      {:ex_aws,    "~> 1.1.0"},
      {:uuid,      "~> 1.1" },
      {:poison,    "~> 2.0"},
    ]
  end
end
