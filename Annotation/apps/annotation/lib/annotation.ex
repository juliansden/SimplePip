defmodule Annotation do
  use GenServer
  @moduledoc """
  Documentation for `Annotation`.
  """

  require Poison

  @impl true
  def init({pid, file}) do
    {:ok, %{pid: pid, id: nil, logs: [], file: file}}
  end

  @impl true
  def handle_call({:inspect_logs}, _from, state) do
    {:reply, Map.get(state, :logs), state}
  end

  @impl true
  def handle_cast({:set_path_id, id}, state) do
    {:noreply, Map.replace!(state, :id, id)}
  end

  @impl true
  def handle_cast({:start_task, name}, state) do
    new_log = %{id: state.id, type: :start_task, name: name}
    IO.binwrite(state.file, Poison.encode!(new_log))
    IO.binwrite(state.file, "\n")
    {:noreply, Map.update!(state, :logs, &([new_log | &1]))}
  end

  @impl true
  def handle_cast({:end_task, name}, state) do
    new_log = %{id: state.id, type: :end_task, name: name}
    IO.binwrite(state.file, Poison.encode!(new_log))
    IO.binwrite(state.file, "\n")
    {:noreply, Map.update!(state, :logs, &([new_log | &1]))}
  end

  @impl true
  def handle_cast({:send, receiver}, state) do
    new_log = %{id: state.id, type: :send, receiver: receiver}
    IO.binwrite(state.file, Poison.encode!(new_log))
    IO.binwrite(state.file, "\n")
    {:noreply, Map.update!(state, :logs, &([new_log | &1]))}
  end

  @impl true
  def handle_cast({:receive, sender}, state) do
    new_log = %{id: state.id, type: :receive, sender: sender}
    IO.binwrite(state.file, Poison.encode!(new_log))
    IO.binwrite(state.file, "\n")
    {:noreply, Map.update!(state, :logs, &([new_log | &1]))}
  end

  @impl true
  def handle_cast({:notice, string}, state) do
    new_log = %{id: state.id, type: :notice, detail: string}
    IO.binwrite(state.file, Poison.encode!(new_log))
    IO.binwrite(state.file, "\n")
    {:noreply, Map.update!(state, :logs, &([new_log | &1]))}
  end

  @doc """
  """
  def start_link(pid) do
    {:ok, file} = File.open(Atom.to_string((pid)), [:write])
    GenServer.start_link(__MODULE__, {pid, file})
  end

  # @spec set_path_id(atom() | pid(), {atom() | pid(), non_neg_integer()})
  def set_path_id(server, id) do
    GenServer.cast(server, {:set_path_id, id})
  end

  def start_task(server, name) do
    GenServer.cast(server, {:start_task, name})
  end

  def end_task(server, name) do
    GenServer.cast(server, {:end_task, name})
  end

  def send(server, receiver) do
    GenServer.cast(server, {:send, receiver})
  end

  def receive(server, sender) do
    GenServer.cast(server, {:receive, sender})
  end

  def notice(server, string) do
    GenServer.cast(server, {:notice, string})
  end

end
