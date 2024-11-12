defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  def handle("*1\r\n$4\r\nPING\r\n", socket), do: :gen_tcp.send(socket, "+PONG\r\n")

  def handle(_, _), do: raise("Huh")

  def serve(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} -> handle(data, socket)
      _ -> raise "Todo"
    end

    serve(socket)
  end

  def loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    Task.start_link(fn -> serve(client) end)
    loop_acceptor(socket)
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # ensures that we don't run into 'Address already in use' errors
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])

    loop_acceptor(socket)
  end
end
