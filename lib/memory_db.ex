defmodule MemoryDb do
  use GenServer

  def init(init_arg), do: {:ok, init_arg}
  def start_link(init_db), do: GenServer.start_link(__MODULE__, init_db, name: __MODULE__)
  def set(kv_pair), do: GenServer.call(__MODULE__, {:set, kv_pair})
  def get(key), do: GenServer.call(__MODULE__, {:get, key})
  def keys(), do: GenServer.call(__MODULE__, {:keys})

  def handle_call({:set, kv_pair}, _, db) do
    IO.inspect(kv_pair, label: "setting to db")
    IO.inspect(db, label: "db state before op")
    {:reply, :ok, Map.put(db, elem(kv_pair, 0), kv_pair)}
  end

  def handle_call({:get, key}, _, db) do
    IO.inspect(key, label: "getting key")
    IO.inspect(db, label: "db state before op")
    {:reply, Map.fetch!(db, key), db}
  end

  def handle_call({:keys}, _, db), do: {:reply, Map.keys(db), db}
end
