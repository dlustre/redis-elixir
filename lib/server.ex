default_config = %{
  port: 6379,
  master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
  master_repl_offset: 0
}

array = ?*
simple_str = ?+
bulk_str = ?$
integer = ?:

defmodule Server do
  use Application

  # Encodings.
  @array array
  @simple_str simple_str
  @bulk_str bulk_str
  @integer integer

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
    @integer integer
    @sep "\r\n"

    def consume_digits(<<@sep, tl::binary>>, acc), do: {String.to_integer(acc), tl}

    def consume_digits(<<n, tl::binary>>, acc) when n in ?0..?9,
      do: consume_digits(tl, <<acc::binary, n>>)

    def consume_simple_str(@sep <> <<tl::binary>>, acc),
      do: {acc, tl} |> IO.inspect(label: "simple string consumed")

    def consume_simple_str(<<char::1-binary, tl::binary>>, acc),
      do: consume_simple_str(tl, acc <> "#{char}")

    def consume_simple_str(<<@simple_str, tl::binary>>), do: consume_simple_str(tl, "")

    def encode(int, @integer), do: <<@integer>> <> Integer.to_string(int) <> @sep
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
    defstruct socket: nil, client: nil
  end

  def config_fetch!(key), do: Agent.get(Config, &Map.fetch!(&1, key))

  def filepath(), do: [config_fetch!(:dir), config_fetch!(:dbfilename)] |> Path.join()
  def file_exists?(), do: filepath() |> File.exists?()
  def file(), do: filepath() |> File.read!()
  def is_config_key?(key), do: Agent.get(Config, &Map.has_key?(&1, key))
  def is_replica?(), do: is_config_key?(:replicaof)

  def get_synced_replicas(),
    do: Agent.get(ReplicaMap, &Enum.count(&1, fn {_, %{in_sync: in_sync}} -> in_sync end))

  defmodule Command do
    @array array
    @bulk_str bulk_str

    @enforce_keys [:kind, :args]
    defstruct [:kind, :args, :resp_bytes]

    def encode(%Command{kind: kind, args: args}) do
      [kind | args]
      |> Enum.map(&Resp.encode(&1, @bulk_str))
      |> Resp.encode(@array)
      |> IO.inspect(label: "encoded command")
    end

    def propagate?(%Command{kind: kind}) when kind in ["SET"], do: true
    def propagate?(_), do: false

    def blocking?(command), do: propagate?(command)
  end

  def start(_type, _args) do
    {args, _} =
      OptionParser.parse!(System.argv(),
        allow_nonexistent_atoms: true,
        switches: [port: :integer, dir: :string, dbfilename: :string]
      )

    {:ok, _} =
      Agent.start_link(fn -> Enum.into(args, @default_config) end, name: Config)
      |> IO.inspect(label: "started config agent")

    Supervisor.start_link([{Task, fn -> Server.listen(%Ctx{}) end}], strategy: :one_for_one)
  end

  def handle_commands(<<>>, _), do: IO.inspect("finished commands")

  def handle_commands(bin, func) do
    {command, tl} = command(bin)
    func.(command)

    Agent.update(Config, fn config ->
      Map.update!(config, :master_repl_offset, &Kernel.+(&1, command.resp_bytes))
    end)

    handle_commands(tl, func)
  end

  def ok, do: Resp.encode("OK", @simple_str)

  def command(
        <<@array, args_length_str::8-bitstring, @sep, @bulk_str, kind_length_str::8-bitstring,
          @sep, tl::binary>> = bin
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

    {%Command{kind: kind, args: Enum.reverse(args), resp_bytes: byte_size(bin) - byte_size(tl)},
     tl}
    |> IO.inspect(label: "Parsed command")
  end

  def exec(command, ctx) do
    IO.inspect({command, ctx}, label: "executing command")
    do_exec(command, ctx)
  end

  def do_exec(%Command{kind: "PING"}, ctx),
    do: if(not is_replica?(), do: :gen_tcp.send(ctx.client, @pong))

  def do_exec(%Command{kind: "ECHO", args: [msg]}, ctx),
    do: :gen_tcp.send(ctx.client, Resp.encode(msg, @bulk_str))

  def do_exec(%Command{kind: "SET", args: [key, value, "px", expiry_ms]}, ctx) do
    expiration = :os.system_time(:millisecond) + String.to_integer(expiry_ms)
    MemoryDb.set({key, value, expiration})
    if not is_replica?(), do: :gen_tcp.send(ctx.client, ok())
  end

  def do_exec(%Command{kind: "SET", args: [key, value]}, ctx) do
    MemoryDb.set({key, value})
    if not is_replica?(), do: :gen_tcp.send(ctx.client, ok())
  end

  def do_exec(%Command{kind: "TYPE", args: [key]}, ctx) do
    if key in MemoryDb.keys() do
      :gen_tcp.send(ctx.client, Resp.encode("string", @simple_str))
    else
      :gen_tcp.send(ctx.client, Resp.encode("none", @simple_str))
    end
  end

  def do_exec(%Command{kind: "GET", args: [key]}, ctx),
    do:
      :gen_tcp.send(
        ctx.client,
        case MemoryDb.get(key) do
          {^key, value, expiration} ->
            if expiration <= :os.system_time(:millisecond),
              do: @null_bulk_str,
              else: Resp.encode(value, @bulk_str)

          {^key, value} ->
            Resp.encode(value, @bulk_str)
        end
      )

  def do_exec(%Command{kind: "CONFIG", args: ["GET", name]}, ctx),
    do:
      :gen_tcp.send(
        ctx.client,
        [name, config_fetch!(String.to_existing_atom(name))]
        |> Enum.map(&Resp.encode(&1, @bulk_str))
        |> Resp.encode(@array)
      )

  def do_exec(%Command{kind: "KEYS", args: ["*"]}, ctx),
    do:
      :gen_tcp.send(
        ctx.client,
        MemoryDb.keys()
        |> Enum.map(&Resp.encode(&1, @bulk_str))
        |> Resp.encode(@array)
      )

  def do_exec(%Command{kind: "INFO", args: _args}, ctx) do
    replication =
      Agent.get(Config, &Map.take(&1, [:master_replid, :master_repl_offset]))
      |> Map.merge(%{role: if(is_replica?(), do: "slave", else: "master")})

    res =
      Enum.map_join(replication, "\n", fn {key, val} -> "#{key}:#{val}" end)
      |> Resp.encode(@bulk_str)
      |> IO.inspect()

    :gen_tcp.send(ctx.client, res)
  end

  def do_exec(%Command{kind: @replconf, args: ["capa" | _]}, ctx),
    do: :gen_tcp.send(ctx.client, ok())

  def do_exec(%Command{kind: @replconf, args: ["listening-port", _]}, ctx),
    do: :gen_tcp.send(ctx.client, ok())

  def do_exec(%Command{kind: @replconf, args: ["GETACK", "*"]}, ctx),
    do:
      :gen_tcp.send(
        ctx.client,
        [@replconf, "ACK", config_fetch!(:master_repl_offset) |> Integer.to_string()]
        |> Enum.map(&Resp.encode(&1, @bulk_str))
        |> Resp.encode(@array)
      )

  def do_exec(%Command{kind: @replconf, args: ["ACK", _offset_at_ack]}, ctx) do
    Agent.update(ReplicaMap, &Map.update!(&1, ctx.client, fn _ -> %{in_sync: true} end))
    |> IO.inspect(label: "got ack from client, setting as in_sync")
  end

  def do_exec(%Command{kind: @psync, args: [_replid, _repl_offset]}, ctx) do
    :ok =
      :gen_tcp.send(
        ctx.client,
        Resp.encode("FULLRESYNC #{config_fetch!(:master_replid)} 0", @simple_str)
      )

    :ok = :gen_tcp.send(ctx.client, Rdb.empty_file() |> Resp.encode_file())

    Agent.update(ReplicaMap, &Map.put(&1, ctx.client, %{in_sync: true}))

    Agent.get(ReplicaMap, & &1)
    |> IO.inspect(label: "psync successful, adding client to ReplicaMap")
  end

  def do_exec(%Command{kind: "WAIT", args: args}, ctx) do
    synced_replicas =
      get_synced_replicas() |> IO.inspect(label: "synced replicas at time of wait cmd")

    [acks_required, timeout_ms] =
      args |> Enum.map(&String.to_integer/1) |> IO.inspect(label: "parsed args")

    if synced_replicas >= acks_required do
      :ok = :gen_tcp.send(ctx.client, Resp.encode(synced_replicas, @integer))
    else
      Agent.get(ReplicaMap, &Map.keys(&1))
      |> IO.inspect(label: "sending getack to these replicas")
      |> Enum.map(
        &:gen_tcp.send(&1, Command.encode(%Command{kind: @replconf, args: ["GETACK", "*"]}))
      )
      |> IO.inspect(label: "getack stream result")

      task = Task.async(fn -> await_acks(acks_required, timeout_ms) end)
      :ok = :gen_tcp.send(ctx.client, Resp.encode(Task.await(task), @integer))
    end
  end

  def do_exec(unknown_cmd, ctx) do
    IO.inspect(unknown_cmd)
    IO.inspect(ctx)
    raise "Unexpected command: " <> unknown_cmd.kind
  end

  def await_acks(_, timeout_ms) when timeout_ms <= 0, do: get_synced_replicas()

  def await_acks(acks_required, timeout_ms) do
    IO.inspect({acks_required, timeout_ms, get_synced_replicas()}, label: "await_acks")
    Process.sleep(100)

    if get_synced_replicas() >= acks_required do
      get_synced_replicas()
    else
      await_acks(acks_required, timeout_ms - 100)
    end
  end

  def serve(ctx) do
    case :gen_tcp.recv(ctx.client, 0, 5000) do
      {:ok, data} ->
        MsgHandler.handle_commands(data, fn command ->
          exec(command, ctx)
          # TODO: make concurrent
          if not is_replica?() and Command.propagate?(command),
            do:
              Agent.get_and_update(ReplicaMap, fn map ->
                IO.inspect(map, label: "desyncing replicas")
                {Map.keys(map), Map.new(map, fn {k, v} -> {k, %{v | in_sync: false}} end)}
              end)
              |> IO.inspect(label: "propagating to these replicas")
              |> Enum.map(&:gen_tcp.send(&1, Command.encode(command)))
              |> IO.inspect(label: "propagation result")
        end)

        serve(ctx)

      {:error, :closed} ->
        IO.inspect("Socket closed")

      {:error, :timeout} ->
        IO.inspect("Socket timed out")

      msg ->
        IO.inspect(msg, label: "unexpected tcp recv result")
        raise "unexpected tcp recv result"
    end
  end

  def loop_acceptor(ctx) do
    {:ok, client} = :gen_tcp.accept(ctx.socket)

    Task.start_link(fn -> serve(%Ctx{ctx | client: client}) end)

    loop_acceptor(ctx)
  end

  def send_handshake(ctx) do
    port = config_fetch!(:port)
    replicaof = config_fetch!(:replicaof)
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
    {sections, <<>>} = Rdb.parse_file(file, [])

    sections
    |> Enum.filter(fn %Rdb.Section{kind: kind} -> kind == :kv_pair end)
    |> Enum.map(fn %Rdb.Section{data: data} ->
      data |> Map.to_list() |> Enum.map(&elem(&1, 1)) |> MemoryDb.set()
    end)

    ctx = %Ctx{ctx | client: master_socket}

    handle_commands(propagated_commands, &exec(&1, ctx))
    |> IO.inspect(label: "handled commands appended to handshake")

    Task.start_link(fn -> serve(ctx) end)
  end

  @doc """
  Listen for incoming connections
  """
  def listen(ctx) do
    if is_replica?(), do: send_handshake(ctx)
    do_listen(ctx)
  end

  def do_listen(ctx) do
    port = config_fetch!(:port)
    # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # ensures that we don't run into 'Address already in use' errors
    {:ok, socket} =
      :gen_tcp.listen(port, [:binary, active: false, reuseaddr: true])
      |> IO.inspect(label: "Listening to port: #{port}")

    {:ok, _} = MemoryDb.start_link(Map.new())
    {:ok, _} = MsgHandler.start_link() |> IO.inspect(label: "started msghandler")

    if is_config_key?(:dir) and is_config_key?(:dbfilename) and file_exists?(),
      do:
        Rdb.parse_file(file(), [])
        |> elem(0)
        |> Enum.filter(fn %Rdb.Section{kind: kind} -> kind == :kv_pair end)
        |> Enum.map(fn %Rdb.Section{data: data} ->
          data |> Map.values() |> List.to_tuple() |> MemoryDb.set()
        end)

    {:ok, _} =
      Agent.start_link(&Map.new/0, name: ReplicaMap)
      |> IO.inspect(label: "started ReplicaMap agent")

    loop_acceptor(%Ctx{ctx | socket: socket})
  end
end
