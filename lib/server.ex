defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  @array ?*
  @bulk_str ?$
  @sep "\r\n"

  use Application

  defmodule Array do
    defstruct elems: []
  end

  def start(_type, _args) do
    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  def command(<<"PING", _::binary>>, _, socket), do: :gen_tcp.send(socket, "+PONG\r\n")

  def command(<<"ECHO", @sep, msg::binary>>, _, socket), do: :gen_tcp.send(socket, msg)

  def serve(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok,
       <<@array, num_args::8-bitstring, @sep, @bulk_str, _command_length::8-bitstring, @sep,
         command::binary>>} ->
        command(command, String.to_integer(num_args) - 1, socket)

      msg ->
        IO.inspect(msg)
        raise "Unknown"
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
