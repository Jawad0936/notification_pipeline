defmodule Pipeline.BroadwayProducer do
  @moduledoc """
  Broadway producer adapter — implements Broadway.Producer behaviour.

  Broadway starts and supervises this process internally.
  Messages are injected via Pipeline.BroadwayPipeline.notify/1
  which routes to the correct producer pid via Broadway's registry.
  """

  use GenStage
  @behaviour Broadway.Producer

  require Logger

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
    # Note: no name: — Broadway manages this process's identity
  end

  ## ── Broadway.Producer callbacks ───────────────────────────────────────────

  @impl Broadway.Producer
  def prepare_for_start(_module, broadway_opts) do
    {[], broadway_opts}
  end

  @impl Broadway.Producer
  def prepare_for_draining(state) do
    {:noreply, [], state}
  end

  ## ── GenStage callbacks ────────────────────────────────────────────────────

  @impl GenStage
  def init(opts) do
    Logger.info("[BroadwayProducer] started pid=#{inspect(self())}")
    {:producer, %{queue: :queue.new(), pending_demand: 0, broadway: opts}}
  end

  @impl GenStage
  def handle_demand(incoming_demand, state) do
    Logger.debug("[BroadwayProducer] demand=#{incoming_demand}")
    total = state.pending_demand + incoming_demand
    {events, new_queue, remaining} = drain(state.queue, total, [])
    {:noreply, wrap(events), %{state | queue: new_queue, pending_demand: remaining}}
  end

  @impl GenStage
  def handle_info({:notify, notification}, state) do
    # Receives messages forwarded by BroadwayPipeline.notify/1
    new_queue = :queue.in(notification, state.queue)

    if state.pending_demand > 0 do
      {events, final_queue, remaining} = drain(new_queue, state.pending_demand, [])
      {:noreply, wrap(events), %{state | queue: final_queue, pending_demand: remaining}}
    else
      {:noreply, [], %{state | queue: new_queue}}
    end
  end

  ## ── Private ───────────────────────────────────────────────────────────────

  defp wrap(events) do
    Enum.map(events, fn notification ->
      %Broadway.Message{
        data: notification,
        acknowledger: {Broadway.NoopAcknowledger, nil, nil}
      }
    end)
  end

  defp drain(queue, 0, acc), do: {Enum.reverse(acc), queue, 0}
  defp drain(queue, demand, acc) do
    case :queue.out(queue) do
      {{:value, event}, new_queue} -> drain(new_queue, demand - 1, [event | acc])
      {:empty, queue}              -> {Enum.reverse(acc), queue, demand}
    end
  end
end
