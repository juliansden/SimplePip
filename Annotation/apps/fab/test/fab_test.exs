defmodule FabTest do
  use ExUnit.Case
  doctest Fab

  test "greets the world" do
    assert Fab.hello() == :world
  end
end
