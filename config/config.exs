import Config

config :notification_pipeline, PipelineWeb.Endpoint,
  http: [port: 4000],
  url: [host: "localhost"],
  render_errors: [formats: [html: PipelineWeb.ErrorHTML]],
  pubsub_server: NotificationPipeline.PubSub,
  live_view: [signing_salt: "pipeline_lv_salt"],
  secret_key_base: "a very long secret key base that is at least 64 bytes long for security purposes ok",
  debug_errors: true,
  code_reloader: true,
  check_origin: false

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]
