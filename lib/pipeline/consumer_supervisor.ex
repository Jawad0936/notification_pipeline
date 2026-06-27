defmodule Pipeline.ConsumerSupervisor do
  use DynamicSupervisor

  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def add_consumer do
    id = System.unique_integer([:positive])
    DynamicSupervisor.start_child(__MODULE__, {Pipeline.Consumer, [id: id]})
  end

  def remove_consumer(pid) do
    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end

  def pool_size do
    DynamicSupervisor.count_children(__MODULE__).active
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
