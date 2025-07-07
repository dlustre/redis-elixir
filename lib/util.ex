defmodule Util do
  use ExUnit.Case

  def assert_equals(actual, expected), do: assert(actual == expected)
end
