defmodule Pipeline.Metrics do
  @moduledoc """
  Telemetry handler and counter store for pipeline metrics.

  Attaches to telemetry events emitted by Producer, PriorityFilter,
  and Consumer stages. Maintains an ETS counter table for:
    - notifications.produced    — total events injected
    - notifications.filtered    — total events through PriorityFilter
    - notifications.delivered   — total successful deliveries
    - notifications.failed      — total failed deliveries
    - broadway.processed        — total Broadway messages processed
    - broadway.delivered        — total Broadway deliveries
    - broadway.failed           — total Broadway failures

  After every event, broadcasts a metrics snapshot via PubSub
  so the LiveView dashboard updates in real time.
  """

  use GenServer

  require Logger

  @table   :pipeline_metrics
  @pubsub  NotificationPipeline.PubSub
  @topic   "metrics"

  @counters [
    :notifications_produced,
    :notifications_filtered,
    :notifications_delivered,
    :notifications_failed,
    :broadway_processed,
    :broadway_delivered,
    :broadway_failed
  ]

  ## ── Public API ────────────────────────────────────────────────────────────

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Return the current metrics snapshot as a map."
  def snapshot do
    Enum.reduce(@counters, %{}, fn key, acc ->
      case :ets.lookup(@table, key) do
        [{^key, val}] -> Map.put(acc, key, val)
        []            -> Map.put(acc, key, 0)
      end
    end)
    |> Map.put(:store_stats, Pipeline.Store.stats())
    |> Map.put(:timestamp, DateTime.utc_now())
  end

  @doc "Reset all counters to zero."
  def reset do
    Enum.each(@counters, fn key ->
      :ets.insert(@table, {key, 0})
    end)
  end

  ## ── Telemetry event emitters (called by pipeline stages) ─────────────────

  def produced(notification) do
    :telemetry.execute(
      [:pipeline, :notification, :produced],
      %{count: 1},
      %{type: notification.type, priority: notification.priority}
    )
  end

  def filtered(count) do
    :telemetry.execute(
      [:pipeline, :notification, :filtered],
      %{count: count},
      %{}
    )
  end

  def delivered(notification) do
    :telemetry.execute(
      [:pipeline, :notification, :delivered],
      %{count: 1},
      %{type: notification.type, id: notification.id}
    )
  end

  def failed(notification) do
    :telemetry.execute(
      [:pipeline, :notification, :failed],
      %{count: 1},
      %{type: notification.type, id: notification.id}
    )
  end

  def broadway_processed(count) do
    :telemetry.execute(
      [:pipeline, :broadway, :processed],
      %{count: count},
      %{}
    )
  end

  def broadway_delivered(count) do
    :telemetry.execute(
      [:pipeline, :broadway, :delivered],
      %{count: count},
      %{}
    )
  end

  def broadway_failed(count) do
    :telemetry.execute(
      [:pipeline, :broadway, :failed],
      %{count: count},
      %{}
    )
  end

  ## ── GenServer callbacks ───────────────────────────────────────────────────

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    # Initialise all counters to zero
    Enum.each(@counters, fn key ->
      :ets.insert(table, {key, 0})
    end)

    # Attach telemetry handlers
    attach_handlers()

    Logger.info("[Metrics] started, counters initialised")
    {:ok, %{table: table}}
  end

  ## ── Telemetry handlers ────────────────────────────────────────────────────

  defp attach_handlers do
    :telemetry.attach_many(
      "pipeline-metrics-handler",
      [
        [:pipeline, :notification, :produced],
        [:pipeline, :notification, :filtered],
        [:pipeline, :notification, :delivered],
        [:pipeline, :notification, :failed],
        [:pipeline, :broadway, :processed],
        [:pipeline, :broadway, :delivered],
        [:pipeline, :broadway, :failed]
      ],
      &__MODULE__.handle_event/4,
      nil
    )
  end

  # Public because telemetry calls it by MFA reference
  def handle_event([:pipeline, :notification, :produced], %{count: n}, _meta, _config) do
    increment(:notifications_produced, n)
    broadcast_snapshot()
  end

  def handle_event([:pipeline, :notification, :filtered], %{count: n}, _meta, _config) do
    increment(:notifications_filtered, n)
    broadcast_snapshot()
  end

  def handle_event([:pipeline, :notification, :delivered], %{count: n}, _meta, _config) do
    increment(:notifications_delivered, n)
    broadcast_snapshot()
  end

  def handle_event([:pipeline, :notification, :failed], %{count: n}, _meta, _config) do
    increment(:notifications_failed, n)
    broadcast_snapshot()
  end

  def handle_event([:pipeline, :broadway, :processed], %{count: n}, _meta, _config) do
    increment(:broadway_processed, n)
    broadcast_snapshot()
  end

  def handle_event([:pipeline, :broadway, :delivered], %{count: n}, _meta, _config) do
    increment(:broadway_delivered, n)
    broadcast_snapshot()
  end

  def handle_event([:pipeline, :broadway, :failed], %{count: n}, _meta, _config) do
    increment(:broadway_failed, n)
    broadcast_snapshot()
  end

  ## ── Private ───────────────────────────────────────────────────────────────

  defp increment(key, n) do
    :ets.update_counter(@table, key, {2, n})
  end

  defp broadcast_snapshot do
    Phoenix.PubSub.broadcast(@pubsub, @topic, {:metrics, snapshot()})
  end
end
