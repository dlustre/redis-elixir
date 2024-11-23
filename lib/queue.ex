defmodule Queue do
  use GenServer

  def init(init_arg), do: {:ok, init_arg}

  def start_link(ctx) do
    GenServer.start_link(__MODULE__, %{ctx: ctx, queue: [], processing: false}, name: __MODULE__)
  end

  def enqueue(msg) do
    GenServer.cast(__MODULE__, {:enqueue, msg})
  end

  def handle_cast({:enqueue, msg}, %{processing: false} = state) do
    IO.inspect(state.queue ++ [msg], label: "not busy. resulting queue")

    {:noreply, %{state | queue: state.queue ++ [msg], processing: true},
     {:continue, :process_next_msg}}
  end

  def handle_cast({:enqueue, msg}, %{processing: true} = state) do
    IO.inspect(state.queue ++ [msg], label: "busy. resulting queue")
    {:noreply, %{state | queue: state.queue ++ [msg], processing: true}}
  end

  def handle_continue(:process_next_msg, %{queue: []} = state) do
    IO.inspect("queue empty, processing stopped.")
    {:noreply, %{state | processing: false}}
  end

  def handle_continue(:process_next_msg, %{ctx: ctx, queue: [msg | tl]} = state) do
    IO.inspect("processing msg in queue")
    Server.exec(msg, ctx)
    {:noreply, %{state | queue: tl}, {:continue, :process_next_msg}}
  end
end
