defmodule Server do
  @array ?*
  @simple_str ?+
  @bulk_str ?$

  @sep "\r\n"
  @digit ?0..?9
  @null_bulk_str "$-1\r\n"

  use Application

  defmodule Ctx do
    @enforce_keys [:config]
    defstruct [:config, socket: nil, client: nil]
  end

  defmodule Command do
    @enforce_keys [:kind, :args]
    defstruct [:kind, :args]
  end

  def start(_type, _args) do
    {args, _} =
      OptionParser.parse!(System.argv(),
        allow_nonexistent_atoms: true,
        switches: [dir: :string, dbfilename: :string]
      )

    ctx = %Ctx{config: Enum.into(args, %{})} |> IO.inspect()

    Supervisor.start_link([{Task, fn -> Server.listen(ctx) end}], strategy: :one_for_one)
  end

  def consume_digits(<<@sep, tl::binary>>, acc), do: {String.to_integer(acc), tl}

  def consume_digits(<<n, tl::binary>>, acc) when n in @digit,
    do: consume_digits(tl, <<acc::binary, n>>)

  def encode(str, @simple_str),
    do: <<@simple_str>> <> str <> @sep

  def encode(str, @bulk_str),
    do: <<@bulk_str>> <> "#{String.length(str)}" <> @sep <> str <> @sep

  def encode(array, @array), do: <<@array>> <> "#{Enum.count(array)}" <> @sep <> Enum.join(array)

  def decode(<<@bulk_str, tl::binary>>) do
    {str_length, tl} = consume_digits(tl, <<>>)
    <<str::size(str_length)-unit(8)-binary, @sep, tl::binary>> = tl
    {str, tl}
  end

  def command(
        <<@array, args_length_str::8-bitstring, @sep, @bulk_str, kind_length_str::8-bitstring,
          @sep, tl::binary>> = _bin
      ) do
    kind_length = String.to_integer(kind_length_str)
    <<kind::size(kind_length)-unit(8)-binary, @sep, tl::binary>> = tl
    args_length = String.to_integer(args_length_str) - 1

    {args, _tl} =
      List.duplicate(0, args_length)
      |> Enum.reduce({[], tl}, fn _, {args, bin} ->
        {arg, tl} = decode(bin)
        {[arg | args], tl}
      end)

    %Command{kind: kind, args: Enum.reverse(args)} |> IO.inspect()
  end

  def exec(%Command{kind: "PING"}, ctx), do: :gen_tcp.send(ctx.client, "+PONG\r\n")

  def exec(%Command{kind: "ECHO", args: [msg]}, ctx),
    do: :gen_tcp.send(ctx.client, encode(msg, @bulk_str))

  def exec(%Command{kind: "SET", args: [key, value, "px", expiry_ms]}, ctx) do
    expiration = :os.system_time(:millisecond) + String.to_integer(expiry_ms)
    true = :ets.insert(:redis, {key, value, expiration})
    :gen_tcp.send(ctx.client, encode("OK", @simple_str))
  end

  def exec(%Command{kind: "SET", args: [key, value]}, ctx) do
    true = :ets.insert(:redis, {key, value})
    :gen_tcp.send(ctx.client, encode("OK", @simple_str))
  end

  def exec(%Command{kind: "GET", args: [key]}, ctx) do
    value =
      case :ets.lookup(:redis, key) |> IO.inspect() do
        [{^key, value, expiration}] ->
          if expiration <= :os.system_time(:millisecond),
            do: @null_bulk_str,
            else: encode(value, @bulk_str)

        [{^key, value}] ->
          encode(value, @bulk_str)
      end

    :gen_tcp.send(ctx.client, value)
  end

  def exec(%Command{kind: "CONFIG", args: ["GET", name]}, ctx),
    do:
      :gen_tcp.send(
        ctx.client,
        [name, Map.fetch!(ctx.config, String.to_existing_atom(name))]
        |> Enum.map(&encode(&1, @bulk_str))
        |> encode(@array)
      )

  def exec(unknown_cmd, ctx) do
    IO.inspect(unknown_cmd)
    IO.inspect(ctx)
    raise "Unexpected command: " <> unknown_cmd.kind
  end

  def serve(ctx) do
    case :gen_tcp.recv(ctx.client, 0) do
      {:ok, data} ->
        data |> command() |> exec(ctx)

      {:error, :closed} ->
        IO.puts("Socket closed")
        nil

      msg ->
        IO.inspect(msg)
        raise "Unknown"
    end

    serve(ctx)
  end

  def loop_acceptor(ctx) do
    {:ok, client} = :gen_tcp.accept(ctx.socket)
    Task.start_link(fn -> serve(%Ctx{ctx | client: client}) end)
    loop_acceptor(ctx)
  end

  @doc """
  Listen for incoming connections
  """
  def listen(ctx) do
    # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # ensures that we don't run into 'Address already in use' errors
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])
    :ets.new(:redis, [:set, :public, :named_table])

    loop_acceptor(%Ctx{ctx | socket: socket})
  end
end
