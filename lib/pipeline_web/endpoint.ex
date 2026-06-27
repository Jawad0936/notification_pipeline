defmodule PipelineWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :notification_pipeline

  socket "/live", Phoenix.LiveView.Socket,
    websocket: [connect_info: [session: [store: :cookie, key: "_pipeline_key", signing_salt: "pipeline_salt_2024"]]]

  if code_reloading? do
    socket "/phoenix/live_reload/socket", Phoenix.LiveReloader.Socket
    plug Phoenix.LiveReloader
    plug Phoenix.CodeReloader
  end

  plug Plug.Session,
    store: :cookie,
    key: "_pipeline_key",
    signing_salt: "pipeline_salt_2024"

  plug PipelineWeb.Router
end
