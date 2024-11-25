defmodule MsgHandler do
  use GenServer

  def init(init_arg), do: {:ok, init_arg}
  def start_link(), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def handle_commands(commands, func),
    do: GenServer.cast(__MODULE__, {:handle_commands, {commands, func}})

  def handle_cast({:handle_commands, {commands, func}}, state) do
    IO.inspect("received handle_commands")
    do_handle_commands(commands, func)
    {:noreply, state}
  end

  def do_handle_commands(<<>>, _), do: IO.inspect("finished commands")

  def do_handle_commands(bin, func) do
    {command, tl} = Server.command(bin)

    do_func = fn ->
      func.(command)

      Agent.update(Config, fn config ->
        Map.update!(config, :master_repl_offset, &Kernel.+(&1, command.resp_bytes))
      end)
    end

    if Server.Command.blocking?(command), do: do_func.(), else: Task.start_link(do_func)
    do_handle_commands(tl, func)
  end
end
