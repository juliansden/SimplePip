defmodule PipTest do
  use ExUnit.Case
  doctest Pip

  test "greets the world" do
    assert Pip.hello() == :world
  end
end
