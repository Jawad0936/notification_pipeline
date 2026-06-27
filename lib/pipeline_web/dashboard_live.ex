defmodule PipelineWeb.DashboardLive do
  @moduledoc """
  Real-time pipeline metrics dashboard.

  Subscribes to the metrics PubSub topic and re-renders on every
  telemetry event — throughput, queue depth, delivery rates, failures.
  """

  use Phoenix.LiveView

  @pubsub NotificationPipeline.PubSub
  @metrics_topic "metrics"
  @notif_topic   "notifications"
  @broadway_topic "notifications:broadway"

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Phoenix.PubSub.subscribe(@pubsub, @metrics_topic)
      Phoenix.PubSub.subscribe(@pubsub, @notif_topic)
      Phoenix.PubSub.subscribe(@pubsub, @broadway_topic)
    end

    {:ok,
     socket
     |> assign(:metrics, Pipeline.Metrics.snapshot())
     |> assign(:recent, [])}
  end

  @impl true
  def handle_info({:metrics, metrics}, socket) do
    {:noreply, assign(socket, :metrics, metrics)}
  end

  def handle_info({:notification, notification}, socket) do
    recent =
      [notification | socket.assigns.recent]
      |> Enum.take(10)

    {:noreply, assign(socket, :recent, recent)}
  end

  @impl true
  def handle_event("inject_genStage", _params, socket) do
    Pipeline.Producer.notify(%{type: :overdue,  message: "Overdue task (dashboard)"})
    Pipeline.Producer.notify(%{type: :system,   message: "System alert (dashboard)"})
    Pipeline.Producer.notify(%{type: :upcoming, message: "Upcoming deadline (dashboard)"})
    {:noreply, socket}
  end

  def handle_event("inject_broadway", _params, socket) do
  Pipeline.BroadwayPipeline.notify(%{id: UUID.uuid4(), type: :overdue,  message: "Broadway overdue",  priority: 3, inserted_at: DateTime.utc_now()})
  Pipeline.BroadwayPipeline.notify(%{id: UUID.uuid4(), type: :system,   message: "Broadway alert",    priority: 2, inserted_at: DateTime.utc_now()})
  Pipeline.BroadwayPipeline.notify(%{id: UUID.uuid4(), type: :upcoming, message: "Broadway upcoming", priority: 1, inserted_at: DateTime.utc_now()})
  {:noreply, socket}
end

  def handle_event("reset_metrics", _params, socket) do
    Pipeline.Metrics.reset()
    {:noreply, assign(socket, :metrics, Pipeline.Metrics.snapshot())}
  end

  @impl true
def render(assigns) do
  ~H"""
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <meta name="csrf-token" content={Plug.CSRFProtection.get_csrf_token()} />
    <title>Notification Pipeline Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/phoenix@1.7.14/priv/static/phoenix.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/phoenix_live_view@0.20.17/priv/static/phoenix_live_view.min.js"></script>
    <script>
      let csrfToken = document.querySelector("meta[name='csrf-token']").getAttribute("content")
      let liveSocket = new window.LiveView.LiveSocket("/live", window.Phoenix.Socket, {
        params: {_csrf_token: csrfToken}
      })
      liveSocket.connect()
      window.liveSocket = liveSocket
    </script>
  </head>
  <body class="bg-gray-950 text-gray-100 min-h-screen font-mono p-6">

    <div class="max-w-5xl mx-auto">

      <%!-- Header --%>
      <div class="mb-8">
        <h1 class="text-2xl font-bold text-white tracking-tight">
          Notification Pipeline
        </h1>
        <p class="text-gray-400 text-sm mt-1">
          Real-time metrics · GenStage + Broadway · Pinterest architecture
        </p>
      </div>

      <%!-- Stat cards --%>
      <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <.stat_card label="Produced"  value={@metrics.notifications_produced}  color="text-blue-400" />
        <.stat_card label="Filtered"  value={@metrics.notifications_filtered}  color="text-purple-400" />
        <.stat_card label="Delivered" value={@metrics.notifications_delivered} color="text-green-400" />
        <.stat_card label="Failed"    value={@metrics.notifications_failed}    color="text-red-400" />
      </div>

      <%!-- Broadway cards --%>
      <div class="mb-6">
        <h2 class="text-xs text-gray-500 uppercase tracking-widest mb-3">Broadway Layer</h2>
        <div class="grid grid-cols-3 gap-4">
          <.stat_card label="Processed" value={@metrics.broadway_processed} color="text-yellow-400" />
          <.stat_card label="Delivered" value={@metrics.broadway_delivered} color="text-green-300" />
          <.stat_card label="Failed"    value={@metrics.broadway_failed}    color="text-red-300" />
        </div>
      </div>

      <%!-- ETS Store stats --%>
      <div class="mb-8 bg-gray-900 rounded-lg p-4 border border-gray-800">
        <h2 class="text-xs text-gray-500 uppercase tracking-widest mb-3">ETS Store</h2>
        <div class="grid grid-cols-3 gap-4 text-center">
          <div>
            <div class="text-yellow-400 text-xl font-bold"><%= @metrics.store_stats.pending %></div>
            <div class="text-gray-500 text-xs mt-1">Pending</div>
          </div>
          <div>
            <div class="text-green-400 text-xl font-bold"><%= @metrics.store_stats.delivered %></div>
            <div class="text-gray-500 text-xs mt-1">Delivered</div>
          </div>
          <div>
            <div class="text-red-400 text-xl font-bold"><%= @metrics.store_stats.failed %></div>
            <div class="text-gray-500 text-xs mt-1">Failed</div>
          </div>
        </div>
      </div>

      <%!-- Controls --%>
      <div class="flex gap-3 mb-8">
        <button phx-click="inject_genStage"
          class="px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-sm rounded-lg transition">
          Inject GenStage (3)
        </button>
        <button phx-click="inject_broadway"
          class="px-4 py-2 bg-purple-600 hover:bg-purple-500 text-white text-sm rounded-lg transition">
          Inject Broadway (3)
        </button>
        <button phx-click="reset_metrics"
          class="px-4 py-2 bg-gray-700 hover:bg-gray-600 text-white text-sm rounded-lg transition">
          Reset Counters
        </button>
      </div>

      <%!-- Recent notifications --%>
      <div class="bg-gray-900 rounded-lg border border-gray-800">
        <div class="px-4 py-3 border-b border-gray-800">
          <h2 class="text-xs text-gray-500 uppercase tracking-widest">Recent Notifications</h2>
        </div>
        <div class="divide-y divide-gray-800">
          <%= if @recent == [] do %>
            <div class="px-4 py-6 text-gray-600 text-sm text-center">
              No notifications yet — inject some above
            </div>
          <% else %>
            <%= for n <- @recent do %>
              <div class="px-4 py-3 flex items-center justify-between">
                <div class="flex items-center gap-3">
                  <span class={priority_badge(n.type)}>
                    <%= n.type %>
                  </span>
                  <span class="text-gray-300 text-sm"><%= n.message %></span>
                </div>
                <span class="text-gray-600 text-xs font-mono"><%= n.id %></span>
              </div>
            <% end %>
          <% end %>
        </div>
      </div>

      <%!-- Last updated --%>
      <div class="mt-4 text-right text-gray-700 text-xs">
        Last updated: <%= @metrics.timestamp %>
      </div>

    </div>
  </body>
  </html>
  """
end

  ## ── Components ────────────────────────────────────────────────────────────

  attr :label, :string, required: true
  attr :value, :integer, required: true
  attr :color, :string, required: true

  def stat_card(assigns) do
    ~H"""
    <div class="bg-gray-900 rounded-lg p-4 border border-gray-800 text-center">
      <div class={"text-2xl font-bold " <> @color}><%= @value %></div>
      <div class="text-gray-500 text-xs mt-1 uppercase tracking-wider"><%= @label %></div>
    </div>
    """
  end

  ## ── Helpers ───────────────────────────────────────────────────────────────

  defp priority_badge(:overdue),  do: "px-2 py-0.5 text-xs rounded bg-red-900 text-red-300"
  defp priority_badge(:system),   do: "px-2 py-0.5 text-xs rounded bg-yellow-900 text-yellow-300"
  defp priority_badge(:upcoming), do: "px-2 py-0.5 text-xs rounded bg-blue-900 text-blue-300"
  defp priority_badge(_),         do: "px-2 py-0.5 text-xs rounded bg-gray-800 text-gray-400"
end
