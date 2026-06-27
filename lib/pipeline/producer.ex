defmodule Pipeline.Producer do
  @moduledoc """
  GenStage producer — the entry point of the notification pipeline.

  Holds an internal queue of notification events and emits them downstream
  only when consumers request more (demand-driven back-pressure).

  Public API:
    Pipeline.Producer.notify(%{id, type, message, priority})

  Notification types:  :upcoming | :overdue | :system
  Priority values:     1 (low) … 3 (high) — assigned automatically by type
  """

  use GenStage

  require Logger

  @priorities %{overdue: 3, system: 2, upcoming: 1}

  ## ── Public API ────────────────────────────────────────────────────────────

  def start_link(_opts \\ []) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Inject a notification event into the pipeline.

      Pipeline.Producer.notify(%{
        id:      "notif-1",
        type:    :overdue,
        message: "Task past deadline"
      })
  """
  def notify(notification) do
    GenStage.cast(__MODULE__, {:notify, enrich(notification)})
  end

  ## ── GenStage callbacks ────────────────────────────────────────────────────

  @impl true
  def init(:ok) do
    Logger.info("[Producer] started")
    # {:producer, state}
    # state holds the event queue and any unfulfilled demand
    {:producer, %{queue: :queue.new(), pending_demand: 0}}
  end

  @impl true
  def handle_demand(incoming_demand, state) do
    total_demand = state.pending_demand + incoming_demand
    {events, new_queue, remaining_demand} = drain(state.queue, total_demand, [])

    Logger.debug("[Producer] demand=#{incoming_demand} dispatching=#{length(events)} remaining_demand=#{remaining_demand}")

    {:noreply, events, %{state | queue: new_queue, pending_demand: remaining_demand}}
  end

  @impl true
  def handle_cast({:notify, notification}, state) do
    # Register the notification in the ETS store immediately on arrival
    Pipeline.Store.put(notification)
    Pipeline.Metrics.produced(notification)

    new_queue = :queue.in(notification, state.queue)

    if state.pending_demand > 0 do
      # Consumers are already waiting — fulfil demand immediately
      {events, final_queue, remaining_demand} = drain(new_queue, state.pending_demand, [])

      Logger.debug("[Producer] queued+flushed id=#{notification.id} dispatching=#{length(events)}")

      {:noreply, events, %{state | queue: final_queue, pending_demand: remaining_demand}}
    else
      # No pending demand — hold in queue until consumers ask
      Logger.debug("[Producer] buffered id=#{notification.id} queue_size=#{:queue.len(new_queue)}")

      {:noreply, [], %{state | queue: new_queue}}
    end
  end

  ## ── Private ───────────────────────────────────────────────────────────────

  # Drain up to `demand` events from the queue, return remaining demand.
  defp drain(queue, 0, acc), do: {Enum.reverse(acc), queue, 0}
  defp drain(queue, demand, acc) do
    case :queue.out(queue) do
      {{:value, event}, new_queue} -> drain(new_queue, demand - 1, [event | acc])
      {:empty, queue}              -> {Enum.reverse(acc), queue, demand}
    end
  end

  # Attach priority and timestamp to every notification on arrival.
  defp enrich(%{type: type} = notification) do
    notification
    |> Map.put_new(:id, generate_id())
    |> Map.put(:priority, Map.get(@priorities, type, 1))
    |> Map.put(:inserted_at, DateTime.utc_now())
  end

  defp generate_id do
    :crypto.strong_rand_bytes(6) |> Base.encode16(case: :lower)
  end
end
