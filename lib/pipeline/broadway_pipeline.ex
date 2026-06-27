defmodule Pipeline.BroadwayPipeline do
  use Broadway

  require Logger

  alias Broadway.Message

  @pubsub NotificationPipeline.PubSub
  @topic  "notifications:broadway"

  def start_link(_opts \\ []) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {Pipeline.BroadwayProducer, []},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 2]
      ],
      batchers: [
        overdue:  [concurrency: 2, batch_size: 5,  batch_timeout: 200],
        system:   [concurrency: 1, batch_size: 10, batch_timeout: 200],
        upcoming: [concurrency: 1, batch_size: 20, batch_timeout: 200]
      ]
    )
  end

  @doc "Inject a notification into the Broadway pipeline."
  def notify(notification) do
    # Broadway names its producer processes predictably — index 0 is the first
    producer = Broadway.producer_names(__MODULE__) |> List.first()
    send(producer, {:notify, notification})
  end

  @impl true
def handle_message(_processor, message, _context) do
  notification = message.data

  Logger.info(
    "[Broadway] handle_message id=#{notification.id} " <>
    "type=#{inspect(notification.type)}"
  )

  # Register in ETS so mark_delivered has something to update
  Pipeline.Store.put(notification)

  case validate(notification) do
    :ok ->
      Message.put_batcher(message, notification.type)

    {:error, reason} ->
      Logger.warning("[Broadway] invalid id=#{notification.id} reason=#{reason}")
      Message.failed(message, reason)
  end
end
  @impl true
  def handle_batch(batcher, messages, _batch_info, _context) do
    Logger.info("[Broadway] handle_batch batcher=#{batcher} size=#{length(messages)}")

    Enum.map(messages, fn message ->
      case deliver(message.data) do
        :ok              -> message
        {:error, reason} -> Message.failed(message, reason)
      end
    end)
  end

  @impl true
  def handle_failed(messages, _context) do
    Enum.each(messages, fn message ->
      notification = message.data
      Logger.error("[Broadway] failed id=#{notification.id} reason=#{inspect(message.status)}")
      Pipeline.Store.mark_failed(notification.id)
    end)
    messages
  end

  defp deliver(notification) do
    Logger.info("[Broadway] delivering id=#{notification.id}")
    Phoenix.PubSub.broadcast(@pubsub, @topic, {:notification, notification})
    Pipeline.Store.mark_delivered(notification.id)
    :ok
  rescue
    e -> {:error, Exception.message(e)}
  end

  defp validate(%{id: id, type: type})
       when is_binary(id) and type in [:overdue, :system, :upcoming],
       do: :ok
  defp validate(notification) do
    Logger.warning("[Broadway] validation failed: #{inspect(notification)}")
    {:error, "missing or invalid id/type"}
  end
end
