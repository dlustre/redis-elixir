defmodule Util do
  require(ExUnit.Assertions)

  def assert_equals(actual, expected), do: ExUnit.Assertions.assert(actual == expected)
end

defmodule Rdb do
  @header "REDIS0011"
  @eof 0xFF
  @selectdb 0xFE
  @string 0x00
  @expiretime 0xFD
  @expiretime_ms 0xFC
  @resizedb 0xFB
  @aux 0xFA

  @next_six <<0b00::2>>
  @next_fourteen <<0b01::2>>
  @next_thirty_two <<0b10::2>>
  @special <<0b11::2>>

  defmodule Section do
    @enforce_keys [:kind, :data]
    defstruct [:kind, :data]
  end

  def empty_file,
    do:
      Base.decode64!(
        "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
      )

  def test_length_encoding() do
    consume(<<0x0A>>, :length_encoded) |> elem(0) |> Util.assert_equals(10)

    consume(<<0x42, 0xBC>>, :length_encoded)
    |> elem(0)
    |> Util.assert_equals(700)

    consume(<<0x80, 0x00, 0x00, 0x42, 0x68>>, :length_encoded)
    |> elem(0)
    |> Util.assert_equals(17000)

    consume(<<0b11::2>>, :length_encoded)
  end

  def test_str_encoding() do
    consume(
      <<0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21>>,
      :str_encoded
    )
    |> elem(0)
    |> Util.assert_equals("Hello, World!")

    consume(
      <<0xC0, 0x7B, 0xFF>>,
      :str_encoded
    )
    |> elem(0)
    |> Util.assert_equals(123)

    consume(
      <<0xC1, 0x39, 0x30>>,
      :str_encoded
    )
    |> elem(0)
    |> Util.assert_equals(12345)

    consume(
      <<0xC2, 0x87, 0xD6, 0x12, 0x00>>,
      :str_encoded
    )
    |> elem(0)
    |> Util.assert_equals(1_234_567)
  end

  def consume(<<@next_six, length::6, tl::binary>>, :length_encoded),
    do: {length, tl} |> IO.inspect(label: "length_encoded: next_six")

  def consume(<<@next_fourteen, length::14, tl::binary>>, :length_encoded),
    do: {length, tl} |> IO.inspect(label: "length_encoded: next_fourteen")

  def consume(<<@next_thirty_two, _::6, length::32, tl::binary>>, :length_encoded),
    do: {length, tl} |> IO.inspect(label: "length_encoded: next_thirty_two")

  def consume(<<@special, str_format::6, tl::binary>>, :length_encoded) do
    case str_format do
      0 -> {:eight, tl}
      1 -> {:sixteen, tl}
      2 -> {:thirtytwo, tl}
    end
  end

  def consume(bin, :str_encoded) do
    case consume(bin, :length_encoded) do
      {:eight, tl} ->
        <<int, tl::binary>> = tl
        {int, tl} |> IO.inspect(label: "str_encoded:eight")

      {:sixteen, tl} ->
        <<int::16-little, tl::binary>> = tl
        {int, tl} |> IO.inspect(label: "str_encoded:sixteen")

      {:thirtytwo, tl} ->
        <<int::32-little, tl::binary>> = tl
        {int, tl} |> IO.inspect(label: "str_encoded:thirtytwo")

      {length, tl} ->
        <<str::size(length)-binary, tl::binary>> = tl
        {str, tl} |> IO.inspect(label: "str_encoded")
    end
  end

  def parse(<<@eof, tl::binary>>) do
    IO.puts("eof")
    {%Section{kind: :eof, data: nil}, tl}
  end

  def parse(<<@selectdb, bin::binary>>) do
    IO.puts("selectdb")
    {db_index, tl} = consume(bin, :length_encoded) |> IO.inspect()
    <<@resizedb, tl::binary>> = tl
    {table_size, tl} = consume(tl, :length_encoded) |> IO.inspect()
    {expire_table_size, tl} = consume(tl, :length_encoded) |> IO.inspect()

    {%Section{
       kind: :selectdb,
       data: %{db_index: db_index, table_size: table_size, expire_table_size: expire_table_size}
     }, tl}
  end

  def parse(<<@string, bin::binary>>) do
    IO.puts("string no expiry")
    {key, tl} = consume(bin, :str_encoded) |> IO.inspect()
    {val, tl} = consume(tl, :str_encoded) |> IO.inspect()
    {%Section{kind: :kv_pair, data: %{key: key, val: val}}, tl}
  end

  def parse(<<@expiretime, time::32-little, bin::binary>>) do
    IO.puts("expiretime")
    {key, tl} = consume(bin, :str_encoded) |> IO.inspect()
    {val, tl} = consume(tl, :str_encoded) |> IO.inspect()
    {%Section{kind: :kv_pair, data: %{key: key, val: val, expiretime: {time, :second}}}, tl}
  end

  def parse(<<@expiretime_ms, time::64-little, @string, bin::binary>>) do
    IO.puts("expiretime_ms")
    {key, tl} = consume(bin, :str_encoded) |> IO.inspect()
    {val, tl} = consume(tl, :str_encoded) |> IO.inspect()
    {%Section{kind: :kv_pair, data: %{key: key, val: val, expiretime: {time, :millisecond}}}, tl}
  end

  def parse(<<@aux, bin::binary>>) do
    IO.puts("aux")
    {key, tl} = consume(bin, :str_encoded) |> IO.inspect()
    {val, tl} = consume(tl, :str_encoded) |> IO.inspect()
    {%Section{kind: :aux, data: %{key: key, val: val}}, tl}
  end

  def parse(<<@header, tl::binary>>) do
    IO.puts("header")
    {%Section{kind: :header, data: nil}, tl}
  end

  def parse_file(bin, acc) do
    case parse(bin) do
      {%Section{kind: :eof} = section, _tl} -> Enum.reverse([section | acc])
      {section, tl} -> parse_file(tl, [section |> IO.inspect() | acc])
    end
  end
end

default_config = %{
  port: 6379,
  master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
  master_repl_offset: 0
}

array = ?*
simple_str = ?+
bulk_str = ?$

defmodule Server do
  use Application

  # Encodings.
  @array array
  @simple_str simple_str
  @bulk_str bulk_str

  # Commands.
  @replconf "REPLCONF"
  @psync "PSYNC"

  # Utils.
  @sep "\r\n"
  @digit ?0..?9
  @null_bulk_str "$-1\r\n"
  @pong "+PONG\r\n"
  @default_config default_config

  defmodule Ctx do
    defstruct socket: nil, client: nil, config: default_config

    def config_get(%Ctx{config: config}, name) when is_atom(name), do: Map.fetch!(config, name)

    def config_get(%Ctx{config: config}, name),
      do: Map.fetch!(config, String.to_existing_atom(name))

    def filepath(ctx),
      do:
        [config_get(ctx, :dir), config_get(ctx, :dbfilename)]
        |> Path.join()
        |> IO.inspect(label: "Filepath")

    def file(ctx), do: ctx |> filepath() |> File.read!()
  end

  defmodule Command do
    @array array
    @bulk_str bulk_str

    @enforce_keys [:kind, :args]
    defstruct [:kind, :args]

    def encode(%Command{kind: kind, args: args}) do
      [kind | args]
      |> Enum.map(&Server.encode(&1, @bulk_str))
      |> Server.encode(@array)
      |> IO.inspect()
    end

    def propagate?(%Command{kind: kind}) when kind in ["SET"], do: true
    def propagate?(_), do: false
  end

  def start(_type, _args) do
    {args, _} =
      OptionParser.parse!(System.argv(),
        allow_nonexistent_atoms: true,
        switches: [port: :integer, dir: :string, dbfilename: :string]
      )

    ctx = %Ctx{config: Enum.into(args, @default_config)} |> IO.inspect()

    Supervisor.start_link([{Task, fn -> Server.listen(ctx) end}], strategy: :one_for_one)
  end

  def consume_digits(<<@sep, tl::binary>>, acc), do: {String.to_integer(acc), tl}

  def consume_digits(<<n, tl::binary>>, acc) when n in @digit,
    do: consume_digits(tl, <<acc::binary, n>>)

  def encode(str, @simple_str), do: <<@simple_str>> <> str <> @sep
  def encode(str, @bulk_str), do: <<@bulk_str>> <> "#{String.length(str)}" <> @sep <> str <> @sep
  def encode(array, @array), do: <<@array>> <> "#{Enum.count(array)}" <> @sep <> Enum.join(array)
  def encode_file(bin), do: <<@bulk_str>> <> "#{byte_size(bin)}" <> @sep <> bin

  def ok, do: encode("OK", @simple_str)

  # def consume_simple_str(@sep <> <<tl::binary>>, acc), do: {acc, tl}
  # def consume_simple_str(<<char::8-binary, tl::binary>>, acc), do: consume_simple_str(tl, acc <> "#{char}")

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

  def exec(%Command{kind: "PING"}, ctx), do: :gen_tcp.send(ctx.client, @pong)

  def exec(%Command{kind: "ECHO", args: [msg]}, ctx),
    do: :gen_tcp.send(ctx.client, encode(msg, @bulk_str))

  def exec(%Command{kind: "SET", args: [key, value, "px", expiry_ms]}, ctx) do
    expiration = :os.system_time(:millisecond) + String.to_integer(expiry_ms)
    true = :ets.insert(:redis, {key, value, expiration})
    :gen_tcp.send(ctx.client, ok())
  end

  def exec(%Command{kind: "SET", args: [key, value]}, ctx) do
    true = :ets.insert(:redis, {key, value})
    :gen_tcp.send(ctx.client, ok())
  end

  def exec(%Command{kind: "GET", args: [key]}, %Ctx{config: config, client: client})
      when config == @default_config do
    value =
      case :ets.lookup(:redis, key) |> IO.inspect() do
        [{^key, value, expiration}] ->
          if expiration <= :os.system_time(:millisecond),
            do: @null_bulk_str,
            else: encode(value, @bulk_str)

        [{^key, value}] ->
          encode(value, @bulk_str)
      end

    :gen_tcp.send(client, value)
  end

  def exec(%Command{kind: "GET", args: [key]}, ctx),
    do:
      :gen_tcp.send(
        ctx.client,
        Rdb.parse_file(Ctx.file(ctx), [])
        |> Enum.find_value(fn
          %Rdb.Section{
            kind: :kv_pair,
            data: %{key: candidate, val: val, expiretime: {expiretime, time_unit}}
          }
          when candidate == key ->
            if expiretime <= :os.system_time(time_unit),
              do: @null_bulk_str,
              else: encode(val, @bulk_str)

          %Rdb.Section{kind: :kv_pair, data: %{key: candidate, val: val}}
          when candidate == key ->
            encode(val, @bulk_str)

          _ ->
            nil
        end)
        |> IO.inspect(label: "value for: " <> key)
      )

  def exec(%Command{kind: "CONFIG", args: ["GET", name]}, ctx),
    do:
      :gen_tcp.send(
        ctx.client,
        [name, Ctx.config_get(ctx, name)]
        |> Enum.map(&encode(&1, @bulk_str))
        |> encode(@array)
      )

  def exec(%Command{kind: "KEYS", args: ["*"]}, ctx),
    do:
      :gen_tcp.send(
        ctx.client,
        Rdb.parse_file(Ctx.file(ctx), [])
        |> IO.inspect(label: "sections")
        |> Enum.filter(fn
          %Rdb.Section{kind: :kv_pair} -> true
          _ -> false
        end)
        |> IO.inspect(label: "kv_pairs")
        |> Enum.map(fn %Rdb.Section{data: %{key: key}} -> key end)
        |> IO.inspect(label: "keys")
        |> Enum.map(&encode(&1, @bulk_str))
        |> encode(@array)
      )

  def exec(%Command{kind: "INFO", args: _args}, %Ctx{
        config: %{replicaof: _} = config,
        client: client
      }) do
    replication =
      Map.take(config, [:master_replid, :master_repl_offset]) |> Map.merge(%{role: "slave"})

    res =
      Enum.map_join(replication, "\n", fn {key, val} -> "#{key}:#{val}" end)
      |> encode(@bulk_str)
      |> IO.inspect()

    :gen_tcp.send(client, res)
  end

  def exec(%Command{kind: "INFO", args: _args}, ctx) do
    replication =
      Map.take(ctx.config, [:master_replid, :master_repl_offset]) |> Map.merge(%{role: "master"})

    res =
      Enum.map_join(replication, "\n", fn {key, val} -> "#{key}:#{val}" end)
      |> encode(@bulk_str)
      |> IO.inspect()

    :gen_tcp.send(ctx.client, res)
  end

  def exec(%Command{kind: @replconf, args: ["capa" | _] = capabilities}, ctx) do
    IO.puts("capa: #{capabilities}")
    :gen_tcp.send(ctx.client, ok())
  end

  def exec(%Command{kind: @replconf, args: ["listening-port", listening_port]}, ctx) do
    IO.puts("listening-port: #{listening_port}")
    :gen_tcp.send(ctx.client, ok())
  end

  def exec(
        %Command{kind: @psync, args: [replid, repl_offset]},
        %Ctx{config: %{master_replid: master_replid}, client: client}
      ) do
    IO.puts("psync: #{replid}, #{repl_offset}")

    :ok = :gen_tcp.send(client, encode("FULLRESYNC #{master_replid} 0", @simple_str))
    :ok = :gen_tcp.send(client, Rdb.empty_file() |> encode_file())

    Agent.update(ReplicaSet, fn rs -> MapSet.put(rs, client) end)

    Agent.get(ReplicaSet, & &1)
    |> IO.inspect(label: "psync successful, adding client to replicaset")
  end

  def exec(unknown_cmd, ctx) do
    IO.inspect(unknown_cmd)
    IO.inspect(ctx)
    raise "Unexpected command: " <> unknown_cmd.kind
  end

  def serve(ctx) do
    case :gen_tcp.recv(ctx.client, 0) do
      {:ok, data} ->
        command = command(data)

        # TODO: make concurrent
        if Command.propagate?(command),
          do:
            Agent.get(ReplicaSet, & &1)
            |> IO.inspect(label: "propagating to these replicas")
            |> Enum.map(&:gen_tcp.send(&1, Command.encode(command)))
            |> IO.inspect(label: "propagation result")

        exec(command, ctx)

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

  def send_handshake(%Ctx{config: %{port: port, replicaof: replicaof}}) do
    [master_base, master_port] = String.split(replicaof)

    {:ok, master_socket} =
      :gen_tcp.connect(~c"#{master_base}", String.to_integer(master_port), [
        :binary,
        active: false
      ])
      |> IO.inspect(label: "connected to #{replicaof}")

    :gen_tcp.send(master_socket, [encode("PING", @bulk_str)] |> encode(@array) |> IO.inspect())

    {:ok, @pong} =
      :gen_tcp.recv(master_socket, 0) |> IO.inspect(label: "recv pong for ping")

    ok_res = ok()

    :gen_tcp.send(
      master_socket,
      Command.encode(%Command{kind: @replconf, args: ["listening-port", "#{port}"]})
    )

    {:ok, ^ok_res} = :gen_tcp.recv(master_socket, 0) |> IO.inspect(label: "listening-port res")

    :gen_tcp.send(
      master_socket,
      Command.encode(%Command{kind: @replconf, args: ["capa", "eof", "capa", "psync2"]})
    )

    {:ok, ^ok_res} = :gen_tcp.recv(master_socket, 0) |> IO.inspect(label: "recv ok for capa")

    :gen_tcp.send(master_socket, Command.encode(%Command{kind: @psync, args: ["?", "-1"]}))

    {:ok, _} =
      :gen_tcp.recv(master_socket, 0) |> IO.inspect(label: "psync and file res")
  end

  @doc """
  Listen for incoming connections
  """
  def listen(%Ctx{config: %{replicaof: _}} = ctx) do
    send_handshake(ctx)
    do_listen(ctx)
  end

  def listen(ctx), do: do_listen(ctx)

  def do_listen(ctx) do
    # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # ensures that we don't run into 'Address already in use' errors
    {:ok, socket} =
      :gen_tcp.listen(ctx.config.port, [:binary, active: false, reuseaddr: true])
      |> IO.inspect(label: "Listening to port: #{ctx.config.port}")

    :ets.new(:redis, [:set, :public, :named_table])
    {:ok, _} = Agent.start_link(&MapSet.new/0, name: ReplicaSet)
    loop_acceptor(%Ctx{ctx | socket: socket})
  end
end
