defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  @array ?*
  @bulk_str ?$
  @sep "\r\n"
  @digit ?0..?9

  use Application

  defmodule Command do
    defstruct kind: "", args: []
  end

  def start(_type, _args) do
    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  def consume_digits(<<@sep, tl::binary>>, acc),
    do: {String.to_integer(acc) |> IO.inspect(label: "evaluated length num"), tl}

  def consume_digits(<<n, tl::binary>>, acc) when n in @digit do
    IO.puts("here")
    consume_digits(tl, <<acc::binary, n>>)
  end

  def encode(@bulk_str, str),
    do: <<@bulk_str>> <> (str |> String.length() |> Integer.to_string()) <> @sep <> str <> @sep

  def decode(<<@bulk_str, tl::binary>>) do
    {str_length, tl} = consume_digits(tl, <<>>)
    <<str::size(str_length)-unit(8)-binary, @sep, tl::binary>> = tl
    {str, tl}
  end

  def command(
        <<@array, args_length_str::8-bitstring, @sep, @bulk_str, kind_length_str::8-bitstring,
          @sep, tl::binary>> = _bin
      ) do
    kind_length = String.to_integer(kind_length_str) |> IO.inspect()
    <<kind::size(kind_length)-unit(8)-binary, @sep, tl::binary>> = tl
    IO.inspect(kind)
    args_length = (String.to_integer(args_length_str) - 1) |> IO.inspect()

    {args, _tl} =
      List.duplicate(0, args_length)
      |> Enum.reduce({[], tl}, fn _, {args, bin} ->
        {arg, tl} = decode(bin)
        {[arg | args], tl}
      end)
      |> IO.inspect()

    %Command{kind: kind, args: args} |> IO.inspect()
  end

  def exec(%Command{kind: "PING"}, socket), do: :gen_tcp.send(socket, "+PONG\r\n")

  def exec(%Command{kind: "ECHO", args: [msg]}, socket),
    do: :gen_tcp.send(socket, encode(@bulk_str, msg))

  def serve(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        data |> command() |> exec(socket)

      {:error, :closed} ->
        nil

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
