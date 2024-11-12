defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  def handle("*1\r\n$4\r\nPING\r\n", client), do: :gen_tcp.send(client, "+PONG\r\n")

  def handle(_, _), do: raise("Huh")

  def serve(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} -> handle(data, client)
      _ -> raise "Todo"
    end

    serve(client)
  end

  def loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    serve(client)
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
