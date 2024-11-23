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
  @null_bulk_str "$-1\r\n"
  @pong "+PONG\r\n"
  @default_config default_config

  defmodule Resp do
    @array array
    @simple_str simple_str
    @bulk_str bulk_str
    @sep "\r\n"

    def consume_digits(<<@sep, tl::binary>>, acc), do: {String.to_integer(acc), tl}

    def consume_digits(<<n, tl::binary>>, acc) when n in ?0..?9,
      do: consume_digits(tl, <<acc::binary, n>>)

    def consume_simple_str(@sep <> <<tl::binary>>, acc),
      do: {acc, tl} |> IO.inspect(label: "simple string consumed")

    def consume_simple_str(<<char::1-binary, tl::binary>>, acc),
      do: consume_simple_str(tl, acc <> "#{char}")

    def consume_simple_str(<<@simple_str, tl::binary>>), do: consume_simple_str(tl, "")

    def encode(str, @simple_str), do: <<@simple_str>> <> str <> @sep

    def encode(str, @bulk_str),
      do: <<@bulk_str>> <> "#{String.length(str)}" <> @sep <> str <> @sep

    def encode(array, @array),
      do: <<@array>> <> "#{Enum.count(array)}" <> @sep <> Enum.join(array)

    def encode_file(bin), do: <<@bulk_str>> <> "#{byte_size(bin)}" <> @sep <> bin

    def decode(<<@bulk_str, tl::binary>>) do
      {str_length, tl} = consume_digits(tl, <<>>)
      <<str::size(str_length)-unit(8)-binary, @sep, tl::binary>> = tl
      {str, tl}
    end

    def decode_file(<<@bulk_str, tl::binary>>) do
      {file_length, tl} = consume_digits(tl, <<>>) |> IO.inspect()
      <<file::size(file_length)-unit(8)-binary, tl::binary>> = tl
      {file, tl}
    end
  end

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
      |> Enum.map(&Resp.encode(&1, @bulk_str))
      |> Resp.encode(@array)
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

  # def consume_commands(<<>>, acc), do: Enum.reverse(acc)

  # def consume_commands(bin, acc) do
  #   {command, tl} = Server.command(bin)
  #   consume_commands(tl, [command | acc])
  # end

  def handle_commands(<<>>, _), do: IO.inspect("finished queueing commands")

  def handle_commands(bin, func) do
    {command, tl} = command(bin)
    func.(command)
    handle_commands(tl, func)
  end

  def ok, do: Resp.encode("OK", @simple_str)

  def command(
        <<@array, args_length_str::8-bitstring, @sep, @bulk_str, kind_length_str::8-bitstring,
          @sep, tl::binary>> = _bin
      ) do
    kind_length = String.to_integer(kind_length_str)
    <<kind::size(kind_length)-unit(8)-binary, @sep, tl::binary>> = tl
    args_length = String.to_integer(args_length_str) - 1

    {args, tl} =
      List.duplicate(0, args_length)
      |> Enum.reduce({[], tl}, fn _, {args, bin} ->
        {arg, tl} = Resp.decode(bin)
        {[arg | args], tl}
      end)

    {%Command{kind: kind, args: Enum.reverse(args)}, tl} |> IO.inspect(label: "Parsed command")
  end

  def set(kv_pair) do
    true = :ets.insert(:redis, kv_pair)
    :ets.tab2list(:redis) |> IO.inspect(label: "result of set")
  end

  def exec(%Command{kind: "PING"}, ctx), do: :gen_tcp.send(ctx.client, @pong)

  def exec(%Command{kind: "ECHO", args: [msg]}, ctx),
    do: :gen_tcp.send(ctx.client, Resp.encode(msg, @bulk_str))

  def exec(%Command{kind: "SET", args: [key, value, "px", expiry_ms]}, ctx) do
    expiration = :os.system_time(:millisecond) + String.to_integer(expiry_ms)
    set({key, value, expiration})
    if !is_map_key(ctx.config, :replicaof), do: :gen_tcp.send(ctx.client, ok())
  end

  def exec(%Command{kind: "SET", args: [key, value]}, ctx) do
    set({key, value})
    if !is_map_key(ctx.config, :replicaof), do: :gen_tcp.send(ctx.client, ok())
  end

  def exec(
        %Command{kind: "GET", args: [key]},
        %Ctx{
          config: %{dir: _, dbfilename: _},
          client: client
        } = ctx
      ),
      do:
        :gen_tcp.send(
          client,
          Rdb.parse_file(Ctx.file(ctx), [])
          |> elem(0)
          |> Enum.find_value(fn
            %Rdb.Section{
              kind: :kv_pair,
              data: %{key: candidate, val: val, expiretime: {expiretime, time_unit}}
            }
            when candidate == key ->
              if expiretime <= :os.system_time(time_unit),
                do: @null_bulk_str,
                else: Resp.encode(val, @bulk_str)

            %Rdb.Section{kind: :kv_pair, data: %{key: candidate, val: val}}
            when candidate == key ->
              Resp.encode(val, @bulk_str)

            _ ->
              nil
          end)
          |> IO.inspect(label: "value for: " <> key)
        )

  def exec(%Command{kind: "GET", args: [key]}, ctx) do
    value =
      case :ets.lookup(:redis, key) |> IO.inspect(label: key <> " when looked up:") do
        [] ->
          raise "'#{key}' not found in ets"

        [{^key, value, expiration}] ->
          if expiration <= :os.system_time(:millisecond),
            do: @null_bulk_str,
            else: Resp.encode(value, @bulk_str)

        [{^key, value}] ->
          Resp.encode(value, @bulk_str)
      end

    :gen_tcp.send(ctx.client, value)
  end

  def exec(%Command{kind: "CONFIG", args: ["GET", name]}, ctx),
    do:
      :gen_tcp.send(
        ctx.client,
        [name, Ctx.config_get(ctx, name)]
        |> Enum.map(&Resp.encode(&1, @bulk_str))
        |> Resp.encode(@array)
      )

  def exec(%Command{kind: "KEYS", args: ["*"]}, ctx),
    do:
      :gen_tcp.send(
        ctx.client,
        Rdb.parse_file(Ctx.file(ctx), [])
        |> elem(0)
        |> IO.inspect(label: "sections")
        |> Enum.filter(fn
          %Rdb.Section{kind: :kv_pair} -> true
          _ -> false
        end)
        |> IO.inspect(label: "kv_pairs")
        |> Enum.map(fn %Rdb.Section{data: %{key: key}} -> key end)
        |> IO.inspect(label: "keys")
        |> Enum.map(&Resp.encode(&1, @bulk_str))
        |> Resp.encode(@array)
      )

  def exec(%Command{kind: "INFO", args: _args}, %Ctx{
        config: %{replicaof: _} = config,
        client: client
      }) do
    replication =
      Map.take(config, [:master_replid, :master_repl_offset]) |> Map.merge(%{role: "slave"})

    res =
      Enum.map_join(replication, "\n", fn {key, val} -> "#{key}:#{val}" end)
      |> Resp.encode(@bulk_str)
      |> IO.inspect()

    :gen_tcp.send(client, res)
  end

  def exec(%Command{kind: "INFO", args: _args}, ctx) do
    replication =
      Map.take(ctx.config, [:master_replid, :master_repl_offset]) |> Map.merge(%{role: "master"})

    res =
      Enum.map_join(replication, "\n", fn {key, val} -> "#{key}:#{val}" end)
      |> Resp.encode(@bulk_str)
      |> IO.inspect()

    :gen_tcp.send(ctx.client, res)
  end

  def exec(%Command{kind: @replconf, args: ["capa" | _] = capabilities}, ctx) do
    IO.puts("capa: #{capabilities}")
    :gen_tcp.send(ctx.client, ok())
  end

  def exec(%Command{kind: @replconf, args: ["listening-port", _]}, ctx),
    do: :gen_tcp.send(ctx.client, ok())

  def exec(%Command{kind: @replconf, args: ["GETACK", "*"]}, ctx) do
    IO.puts("getack: *")

    :gen_tcp.send(
      ctx.client,
      [@replconf, "ACK", "0"]
      |> Enum.map(&Resp.encode(&1, @bulk_str))
      |> Resp.encode(@array)
    )
  end

  def exec(
        %Command{kind: @psync, args: [replid, repl_offset]},
        %Ctx{config: %{master_replid: master_replid}, client: client}
      ) do
    IO.puts("psync: #{replid}, #{repl_offset}")

    :ok = :gen_tcp.send(client, Resp.encode("FULLRESYNC #{master_replid} 0", @simple_str))
    :ok = :gen_tcp.send(client, Rdb.empty_file() |> Resp.encode_file())

    Agent.update(ReplicaSet, fn rs -> MapSet.put(rs, client) end)

    Agent.get(ReplicaSet, & &1)
    |> IO.inspect(label: "psync successful, adding client to replicaset")
  end

  def exec(unknown_cmd, ctx) do
    IO.inspect(unknown_cmd)
    IO.inspect(ctx)
    raise "Unexpected command: " <> unknown_cmd.kind
  end

  def serve(%Ctx{config: %{replicaof: _}} = ctx) do
    case :gen_tcp.recv(ctx.client, 0) do
      {:ok, data} ->
        IO.inspect("recvd as replica")
        handle_commands(data, &exec(&1, ctx))
        serve(ctx)

      {:error, :closed} ->
        IO.puts("Socket closed")

      msg ->
        IO.inspect(msg)
        raise "Unknown"
    end
  end

  def serve(%Ctx{config: config} = ctx) when not is_map_key(config, :replicaof) do
    case :gen_tcp.recv(ctx.client, 0) do
      {:ok, data} ->
        IO.inspect("recvd as master")
        {command, _tl} = command(data)

        # TODO: make concurrent
        if Command.propagate?(command),
          do:
            Agent.get(ReplicaSet, & &1)
            |> IO.inspect(label: "propagating to these replicas")
            |> Enum.map(&:gen_tcp.send(&1, Command.encode(command)))
            |> IO.inspect(label: "propagation result")

        exec(command, ctx)
        serve(ctx)

      {:error, :closed} ->
        IO.puts("Socket closed")

      msg ->
        IO.inspect(msg)
        raise "Unknown"
    end
  end

  def loop_acceptor(ctx) do
    {:ok, client} = :gen_tcp.accept(ctx.socket)

    Task.start_link(fn ->
      Queue.start_link(%Ctx{ctx | client: client})
      serve(%Ctx{ctx | client: client})
    end)

    loop_acceptor(ctx)
  end

  def send_handshake(%Ctx{config: %{port: port, replicaof: replicaof}} = ctx) do
    [master_base, master_port] = String.split(replicaof)

    {:ok, master_socket} =
      :gen_tcp.connect(~c"#{master_base}", String.to_integer(master_port), [
        :binary,
        active: false
      ])
      |> IO.inspect(label: "connected to #{replicaof}")

    :gen_tcp.send(
      master_socket,
      [Resp.encode("PING", @bulk_str)] |> Resp.encode(@array) |> IO.inspect()
    )

    {:ok, @pong} =
      :gen_tcp.recv(master_socket, 0) |> IO.inspect(label: "recv pong for ping")

    ok_res = ok()

    :gen_tcp.send(
      master_socket,
      Command.encode(%Command{kind: @replconf, args: ["listening-port", "#{port}"]})
    )

    {:ok, ^ok_res} = :gen_tcp.recv(master_socket, 0)

    :gen_tcp.send(
      master_socket,
      Command.encode(%Command{kind: @replconf, args: ["capa", "eof", "capa", "psync2"]})
    )

    {:ok, ^ok_res} = :gen_tcp.recv(master_socket, 0)

    :gen_tcp.send(master_socket, Command.encode(%Command{kind: @psync, args: ["?", "-1"]}))

    {:ok, res} = :gen_tcp.recv(master_socket, 0)

    encoded_file =
      case Resp.consume_simple_str(res) do
        {_, ""} ->
          IO.puts("need to recv file")
          {:ok, file} = :gen_tcp.recv(master_socket, 0)
          file

        {_, file} ->
          IO.puts("file already sent")
          file
      end

    {file, propagated_commands} = Resp.decode_file(encoded_file)
    {_, <<>>} = Rdb.parse_file(file, [])

    handle_commands(propagated_commands, &exec(&1, %Ctx{ctx | client: master_socket}))

    Task.start_link(fn -> serve(%Ctx{ctx | client: master_socket}) end)
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
