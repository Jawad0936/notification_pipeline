defmodule PipelineWeb.Router do
  use Phoenix.Router

  import Phoenix.LiveView.Router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  scope "/", PipelineWeb do
    pipe_through :browser
    live "/", DashboardLive, :index
    live "/dashboard", DashboardLive, :index
  end
end
