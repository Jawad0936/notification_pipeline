defmodule NotificationPipelineTest do
  use ExUnit.Case
  doctest NotificationPipeline

  test "greets the world" do
    assert NotificationPipeline.hello() == :world
  end
end
