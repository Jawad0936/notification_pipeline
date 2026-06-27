defmodule Pipeline.Consumer do
  @moduledoc """
  GenStage consumer — the delivery layer of the notification pipeline.

  Each consumer process:
    1. Pulls sorted events from PriorityFilter
    2. Broadcasts them via Phoenix.PubSub (fan-out to LiveView + Channels)
    3. Updates delivery state in the ETS store (delivered | failed)

  Multiple consumers run concurrently under a DynamicSupervisor.
  Back-pressure is enforced via max_demand — each consumer limits
  how many events it holds at once.
  """

  use GenStage

  require Logger

  @pubsub NotificationPipeline.PubSub
  @topic  "notifications"

  ## ── Public API ────────────────────────────────────────────────────────────

  def start_link(opts \\ []) do
    GenStage.start_link(__MODULE__, opts, name: via(opts[:id]))
  end

  defp via(nil), do: __MODULE__
  defp via(id),  do: {:via, Registry, {Pipeline.ConsumerRegistry, id}}

  ## ── GenStage callbacks ────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    consumer_id = opts[:id] || 1

    Logger.info("[Consumer #{consumer_id}] started")

    # max_demand: 5 means this consumer holds at most 5 events at once.
    # The pipeline will not send more until these are acknowledged.
    # This is the back-pressure contract with the upstream stage.
    {:consumer, %{id: consumer_id},
     subscribe_to: [
       {Pipeline.PriorityFilter, min_demand: 0, max_demand: 5}
     ]}
  end

  @impl true
  def handle_events(events, _from, state) do
    Logger.debug("[Consumer #{state.id}] received #{length(events)} events")

    Enum.each(events, fn notification ->
      deliver(notification, state.id)
    end)

    {:noreply, [], state}
  end

  ## ── Private ───────────────────────────────────────────────────────────────

  defp deliver(notification, consumer_id) do
    Logger.info(
      "[Consumer #{consumer_id}] delivering id=#{notification.id} " <>
      "type=#{notification.type} priority=#{notification.priority}"
    )

    case fan_out(notification) do
      :ok ->
        Pipeline.Store.mark_delivered(notification.id)
        Logger.info("[Consumer #{consumer_id}] delivered id=#{notification.id}")

      {:error, reason} ->
        Pipeline.Store.mark_failed(notification.id)
        Logger.error(
          "[Consumer #{consumer_id}] failed id=#{notification.id} reason=#{inspect(reason)}"
        )
    end
  end

  defp fan_out(notification) do
    # Broadcast to all LiveView and Channel subscribers on the topic.
    # Any connected dashboard or client receives this immediately.
    Phoenix.PubSub.broadcast(
      @pubsub,
      @topic,
      {:notification, notification}
    )
  rescue
    e ->
      {:error, Exception.message(e)}
  end
end
