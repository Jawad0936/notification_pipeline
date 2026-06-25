defmodule NotificationPipeline.MixProject do
  use Mix.Project

  def project do
    [
      app: :notification_pipeline,
      version: "0.1.0",
      elixir: "~> 1.16",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {NotificationPipeline.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Pipeline
      {:gen_stage, "~> 1.2"},
      {:broadway, "~> 1.0"},

      # Phoenix + LiveView
      {:phoenix, "~> 1.7"},
      {:phoenix_live_view, "~> 0.20"},
      {:phoenix_html, "~> 4.0"},
      {:phoenix_live_reload, "~> 1.4", only: :dev},

      # Telemetry
      {:telemetry, "~> 1.2"},
      {:telemetry_metrics, "~> 0.6"},
      {:telemetry_poller, "~> 1.0"},

      # Plug / HTTP
      {:plug_cowboy, "~> 2.6"},
      {:jason, "~> 1.4"},

      # Dev / Test
      {:stream_data, "~> 0.6", only: [:dev, :test]}
    ]
  end
end
