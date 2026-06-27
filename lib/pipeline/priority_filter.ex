defmodule Pipeline.PriorityFilter do
  @moduledoc """
  GenStage ProducerConsumer — the priority sorting stage.

  Sits between Producer and the Consumer pool. Receives batches of
  notification events, sorts them highest-priority first, then re-emits
  the sorted batch downstream.

  Priority order (descending):
    3 → :overdue   (must deliver first)
    2 → :system    (medium urgency)
    1 → :upcoming  (low urgency)

  Back-pressure is preserved — this stage never buffers more than it
  receives, so demand signals propagate cleanly to the Producer.
  """

  use GenStage

  require Logger

  ## ── Public API ────────────────────────────────────────────────────────────

  def start_link(_opts \\ []) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  ## ── GenStage callbacks ────────────────────────────────────────────────────

  @impl true
  def init(:ok) do
    Logger.info("[PriorityFilter] started")

    # Subscribe to the Producer automatically on startup.
    # min_demand / max_demand control how many events we pull at once —
    # pulling in batches of up to 10 lets us sort meaningfully.
    {:producer_consumer, %{},
     subscribe_to: [
       {Pipeline.Producer, min_demand: 0, max_demand: 10}
     ]}
  end

  @impl true
  def handle_events(events, _from, state) do
    sorted =
      events
      |> Enum.sort_by(& &1.priority, :desc)
      |> Enum.map(&tag_filtered/1)

  Pipeline.Metrics.filtered(length(sorted))

    Logger.debug(
      "[PriorityFilter] received=#{length(events)} sorted_head=#{inspect(sorted_summary(sorted))}"
    )

    {:noreply, sorted, state}
  end

  ## ── Private ───────────────────────────────────────────────────────────────

  defp tag_filtered(notification) do
    Map.put(notification, :filtered_at, DateTime.utc_now())
  end

  # Build a compact summary of the sorted batch for debug logging.
  defp sorted_summary(events) do
    Enum.map(events, fn e -> {e.type, e.priority} end)
  end
end
