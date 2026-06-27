defmodule NotificationPipeline.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Pipeline.Store,
      Pipeline.Producer,
      Pipeline.PriorityFilter
    ]

    opts = [strategy: :one_for_one, name: NotificationPipeline.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
