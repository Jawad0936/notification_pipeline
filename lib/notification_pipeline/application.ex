defmodule NotificationPipeline.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Pipeline.ConsumerRegistry},
      {Phoenix.PubSub, name: NotificationPipeline.PubSub},
      Pipeline.Store,
      Pipeline.Producer,
      Pipeline.PriorityFilter,
      Pipeline.ConsumerSupervisor
    ]

    opts = [strategy: :one_for_one, name: NotificationPipeline.Supervisor]
    {:ok, sup} = Supervisor.start_link(children, opts)

    # Seed the initial consumer pool after the supervisor is up
    Enum.each(1..3, fn id ->
      DynamicSupervisor.start_child(
        Pipeline.ConsumerSupervisor,
        {Pipeline.Consumer, [id: id]}
      )
    end)

    {:ok, sup}
  end
end
