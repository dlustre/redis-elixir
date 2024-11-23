defmodule Util do
  require(ExUnit.Assertions)

  def assert_equals(actual, expected), do: ExUnit.Assertions.assert(actual == expected)
end
