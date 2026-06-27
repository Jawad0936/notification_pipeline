defmodule Pipeline.ConsumerSupervisor do
  @moduledoc """
  DynamicSupervisor for the Consumer pool.

  Starts `pool_size` consumer workers on boot. Workers can be added
  or removed at runtime via add_consumer/0 and remove_consumer/1.
  """

  use DynamicSupervisor

  require Logger

  @default_pool_size 3

  ## ── Public API ────────────────────────────────────────────────────────────

  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Add a new consumer worker to the pool at runtime."
  def add_consumer do
    id = System.unique_integer([:positive])
    DynamicSupervisor.start_child(__MODULE__, {Pipeline.Consumer, [id: id]})
  end

  @doc "Remove a specific consumer by pid."
  def remove_consumer(pid) do
    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end

  @doc "Return the current pool size."
  def pool_size do
    DynamicSupervisor.count_children(__MODULE__).active
  end

  ## ── DynamicSupervisor callbacks ───────────────────────────────────────────

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
