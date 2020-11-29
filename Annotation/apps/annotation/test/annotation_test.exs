defmodule AnnotationTest do
  use ExUnit.Case
  doctest Annotation

  test "greets the world" do
    assert Annotation.hello() == :world
  end
end
