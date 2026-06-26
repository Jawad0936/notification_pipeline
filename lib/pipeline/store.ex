defmodule Pipeline.Store do
  @moduledoc """
  ETS-backed delivery state store for in-flight notifications.

  Each notification is tracked through its lifecycle:
    pending   → notification received, not yet delivered
    delivered → successfully fanned out to all destinations
    failed    → delivery attempted and exhausted retries

  The table is owned by this GenServer so it survives individual
  consumer crashes — the same pattern used in the Rate Limiter.
  """

  use GenServer

  require Logger

  @table :notification_store

  ## ── Public API ────────────────────────────────────────────────────────────

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Register a new notification as pending."
  def put(notification) do
    :ets.insert(@table, {notification.id, :pending, notification, timestamp()})
    :ok
  end

  @doc "Mark a notification delivered."
  def mark_delivered(id) do
    update_state(id, :delivered)
  end

  @doc "Mark a notification failed."
  def mark_failed(id) do
    update_state(id, :failed)
  end

  @doc "Look up current state for a notification id."
  def get(id) do
    case :ets.lookup(@table, id) do
      [{^id, state, notification, ts}] -> {:ok, %{state: state, notification: notification, inserted_at: ts}}
      [] -> {:error, :not_found}
    end
  end

  @doc "Return counts grouped by state — used by the dashboard."
  def stats do
    all = :ets.tab2list(@table)

    Enum.reduce(all, %{pending: 0, delivered: 0, failed: 0}, fn {_id, state, _n, _ts}, acc ->
      Map.update(acc, state, 1, &(&1 + 1))
    end)
  end

  @doc "Return all notifications matching a given state."
  def list_by_state(state) do
    :ets.match_object(@table, {:_, state, :_, :_})
    |> Enum.map(fn {id, ^state, notification, ts} ->
      %{id: id, notification: notification, inserted_at: ts}
    end)
  end

  @doc "Flush entries older than `seconds` that are in a terminal state."
  def prune(seconds \\ 300) do
    cutoff = timestamp() - seconds

    :ets.select_delete(@table, [
      {{:_, :"$1", :_, :"$2"},
       [{:andalso, {:"/=", :"$1", :pending}, {:<, :"$2", cutoff}}],
       [true]}
    ])
  end

  ## ── GenServer callbacks ───────────────────────────────────────────────────

  @impl true
  def init(_opts) do
    table =
      :ets.new(@table, [
        :named_table,
        :public,           # consumers write directly — no bottleneck through mailbox
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])

    Logger.info("[Store] ETS table #{@table} created (owner: #{inspect(self())})")
    {:ok, %{table: table}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("[Store] unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## ── Private ───────────────────────────────────────────────────────────────

  defp update_state(id, new_state) do
    case :ets.lookup(@table, id) do
      [{^id, _old_state, notification, ts}] ->
        :ets.insert(@table, {id, new_state, notification, ts})
        :ok

      [] ->
        {:error, :not_found}
    end
  end

  defp timestamp, do: System.monotonic_time(:second)
end
