defmodule Raft do
  @moduledoc """
  An implementation of the Raft consensus protocol.
  """
  # Shouldn't need to spawn anything from this module, but if you do
  # you should add spawn to the imports.
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  # This allows you to use Elixir's loggers
  # for messages. See
  # https://timber.io/blog/the-ultimate-guide-to-logging-in-elixir/
  # if you are interested in this. Note we currently purge all logs
  # below Info
  require Logger

  # This structure contains all the process state
  # required by the Raft protocol.
  defstruct(
    # The list of current proceses.
    view: nil,
    # Current leader.
    current_leader: nil,
    # Time before starting an election.
    min_election_timeout: nil,
    max_election_timeout: nil,
    election_timer: nil,
    # Time between heartbeats from the leader.
    heartbeat_timeout: nil,
    heartbeat_timer: nil,
    # Persistent state on all servers.
    current_term: nil,
    voted_for: nil,
    # A short note on log structure: The functions that follow
    # (e.g., get_last_log_index, commit_log_index, etc.) all
    # assume that the log is a list with later entries (i.e.,
    # entries with higher index numbers) appearing closer to
    # the head of the list, and that index numbers start with 1.
    # For example if the log contains 3 entries committe in term
    # 2, 2, and 1 we would expect:
    #
    # `[{index: 3, term: 2, ..}, {index: 2, term: 2, ..},
    #     {index: 1, term: 1}]`
    #
    # If you change this structure, you will need to change
    # those functions.
    #
    # Finally, it might help to know that two lists can be
    # concatenated using `l1 ++ l2`
    log: nil,
    # Volatile state on all servers
    commit_index: nil,
    last_applied: nil,
    # Volatile state on leader
    is_leader: nil,
    next_index: nil,
    match_index: nil,
    # The queue we are building using this RSM.
    queue: nil
  )

  @doc """
  Create state for an initial Raft cluster. Each
  process should get an appropriately updated version
  of this state.
  """
  @spec new_configuration(
          [atom()],
          atom(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: %Raft{}
  def new_configuration(
        view,
        leader,
        min_election_timeout,
        max_election_timeout,
        heartbeat_timeout
      ) do
    %Raft{
      view: view,
      current_leader: leader,
      min_election_timeout: min_election_timeout,
      max_election_timeout: max_election_timeout,
      heartbeat_timeout: heartbeat_timeout,
      # Start from term 1
      current_term: 1,
      voted_for: nil,
      log: [],
      commit_index: 0,
      last_applied: 0,
      is_leader: false,
      next_index: nil,
      match_index: nil,
      queue: :queue.new()
    }
  end

  # Enqueue an item, this **modifies** the state
  # machine, and should only be called when a log
  # entry is committed.
  @spec enqueue(%Raft{}, any()) :: %Raft{}
  defp enqueue(state, item) do
    %{state | queue: :queue.in(item, state.queue)}
  end

  # Dequeue an item, modifying the state machine.
  # This function should only be called once a
  # log entry has been committed.
  @spec dequeue(%Raft{}) :: {:empty | {:value, any()}, %Raft{}}
  defp dequeue(state) do
    {ret, queue} = :queue.out(state.queue)
    {ret, %{state | queue: queue}}
  end

  @doc """
  Commit a log entry, advancing the state machine. This
  function returns a tuple:
  * The first element is {requester, return value}. Your
    implementation should ensure that the leader who committed
    the log entry sends the return value to the requester.
  * The second element is the updated state.
  """
  @spec commit_log_entry(%Raft{}, %Raft.LogEntry{}) ::
          {{atom() | pid(), :ok | :empty | {:value, any()}}, %Raft{}}
  def commit_log_entry(state, entry) do
    case entry do
      %Raft.LogEntry{operation: :nop, requester: r, index: i} ->
        {{r, :ok}, %{state | last_applied: i}}

      %Raft.LogEntry{operation: :enq, requester: r, argument: e, index: i} ->
        {{r, :ok}, %{enqueue(state, e) | last_applied: i}}

      %Raft.LogEntry{operation: :deq, requester: r, index: i} ->
        {ret, state} = dequeue(state)
        {{r, ret}, %{state | last_applied: i}}

      %Raft.LogEntry{} ->
        raise "Log entry with an unknown operation: maybe an empty entry?"

      _ ->
        raise "Attempted to commit something that is not a log entry."
    end
  end

  @doc """
  Commit log at index `index`. This index, which one should read from
  the log entry is assumed to start at 1. This function **does not**
  ensure that commits are processed in order.
  """
  @spec commit_log_index(%Raft{}, non_neg_integer()) ::
          {:noentry | {atom(), :ok | :empty | {:value, any()}}, %Raft{}}
  def commit_log_index(state, index) do
    if length(state.log) < index do
      {:noentry, state}
    else
      # Note that entry indexes are all 1, which in
      # turn means that we expect commit indexes to
      # be 1 indexed. Now a list is a reversed log,
      # so what we can do here is simple: 
      # Given 0-indexed index i, length(log) - 1 - i
      # is the ith list element. => length(log) - (i +1),
      # and hence length(log) - index is what we want.
      correct_idx = length(state.log) - index
      commit_log_entry(state, Enum.at(state.log, correct_idx))
    end
  end

  @doc """
  Find the correct commitIndex for the leader
  applying the last rule of leaders
  """
  @spec update_commit_index(%Raft{is_leader: true}) :: %Raft{is_leader: true}
  def update_commit_index(state) do
    possible_index = state.commit_index + 1
    replicated_num = 
      state.view
      |>  Enum.filter(fn pid -> pid != state.current_leader and Map.get(state.match_index, pid) >= possible_index end)
      |>  length()
    # replicated_num + 1(the leader) is the total number of servers that already replicated the possible_index log
    if 2 * replicated_num + 2 > length(state.view) do
      update_commit_index(%{state | commit_index: possible_index})
    else
      state
    end
  end

  @doc """
  Apply rule number 1 to all servers:
  If commit_index > last_applied, increment last_applied, apply that log to state machine
  """
  @spec apply_to_rsm(%Raft{}) :: %Raft{}
  def apply_to_rsm(state) do
    if state.commit_index > state.last_applied do
      case commit_log_index(state, state.last_applied + 1) do
        {{requester, value}, state} ->
          if state.is_leader and get_log_entry(state, state.last_applied).term == state.current_term do
            send(requester, value)
          end
          apply_to_rsm(state)
        {:noentry, state} ->
          raise "#{state.current_term}: #{whoami()} tried to commit #{state.commit_index} but it only has #{get_last_log_index(state)} entries"
      end
    else
      state
    end
  end

  # The next few functions are public so we can test them, see
  # log_test.exs.
  @doc """
  Get index for the last log entry.
  """
  @spec get_last_log_index(%Raft{}) :: non_neg_integer()
  def get_last_log_index(state) do
    Enum.at(state.log, 0, Raft.LogEntry.empty()).index
  end

  @doc """
  Get term for the last log entry.
  """
  @spec get_last_log_term(%Raft{}) :: non_neg_integer()
  def get_last_log_term(state) do
    Enum.at(state.log, 0, Raft.LogEntry.empty()).term
  end

  @doc """
  Check if log entry at index exists.
  """
  @spec logged?(%Raft{}, non_neg_integer()) :: boolean()
  def logged?(state, index) do
    index > 0 && length(state.log) >= index
  end

  @doc """
  Get log entry at `index`.
  """
  @spec get_log_entry(%Raft{}, non_neg_integer()) ::
          :no_entry | %Raft.LogEntry{}
  def get_log_entry(state, index) do
    if index <= 0 || length(state.log) < index do
      :noentry
    else
      # Note that entry indexes are all 1, which in
      # turn means that we expect commit indexes to
      # be 1 indexed. Now a list is a reversed log,
      # so what we can do here is simple:
      # Given 0-indexed index i, length(log) - 1 - i
      # is the ith list element. => length(log) - (i +1),
      # and hence length(log) - index is what we want.
      correct_idx = length(state.log) - index
      Enum.at(state.log, correct_idx)
    end
  end

  @doc """
  Get log entries starting at index.
  """
  @spec get_log_suffix(%Raft{}, non_neg_integer()) :: [%Raft.LogEntry{}]
  def get_log_suffix(state, index) do
    if length(state.log) < index do
      []
    else
      correct_idx = length(state.log) - index
      Enum.take(state.log, correct_idx + 1)
    end
  end

  @doc """
  Truncate log entry at `index`. This removes log entry
  with index `index` and larger.
  """
  @spec truncate_log_at_index(%Raft{}, non_neg_integer()) :: %Raft{}
  def truncate_log_at_index(state, index) do
    if length(state.log) < index do
      # Nothing to do
      state
    else
      to_drop = length(state.log) - index + 1
      %{state | log: Enum.drop(state.log, to_drop)}
    end
  end

  @doc """
  Add log entries to the log. This adds entries to the beginning
  of the log, we assume that entries are already correctly ordered
  (see structural note about log above.).
  """
  @spec add_log_entries(%Raft{}, [%Raft.LogEntry{}]) :: %Raft{}
  def add_log_entries(state, entries) do
    %{state | log: entries ++ state.log}
  end

  @doc """
  make_leader changes process state for a process that
  has just been elected leader.
  """
  @spec make_leader(%Raft{}) :: %Raft{
          is_leader: true,
          next_index: map(),
          match_index: map()
        }
  def make_leader(state) do
    log_index = get_last_log_index(state)

    # next_index needs to be reinitialized after each
    # election.
    # WARNING TO MYSELF: CHANGED to log_index + 1
    # PROBABLY WRONG
    next_index =
      state.view
      |> Enum.map(fn v -> {v, log_index + 1} end)
      |> Map.new()

    # match_index needs to be reinitialized after each
    # election.
    match_index =
      state.view
      |> Enum.map(fn v -> {v, 0} end)
      |> Map.new()

    %{
      state
      | is_leader: true,
        next_index: next_index,
        match_index: match_index,
        current_leader: whoami(),
    }
  end

  @doc """
  make_follower changes process state for a process
  to mark it as a follower.
  """
  @spec make_follower(%Raft{}) :: %Raft{
          is_leader: false
        }
  def make_follower(state) do
    %{
      state 
      | is_leader: false, 
        next_index: nil,
        match_index: nil
    }
  end

  # update_leader: update the process state with the
  # current leader.
  @spec update_leader(%Raft{}, atom()) :: %Raft{current_leader: atom()}
  defp update_leader(state, who) do
    %{state | current_leader: who}
  end

  # Compute a random election timeout between
  # state.min_election_timeout and state.max_election_timeout.
  # See the paper to understand the reasoning behind having
  # a randomized election timeout.
  @spec get_election_time(%Raft{}) :: non_neg_integer()
  defp get_election_time(state) do
    state.min_election_timeout +
      :rand.uniform(
        state.max_election_timeout -
          state.min_election_timeout
      )
  end

  # Save a handle to the election timer.
  @spec save_election_timer(%Raft{}, reference()) :: %Raft{}
  defp save_election_timer(state, timer) do
    %{state | election_timer: timer}
  end

  # Save a handle to the hearbeat timer.
  @spec save_heartbeat_timer(%Raft{}, reference()) :: %Raft{}
  defp save_heartbeat_timer(state, timer) do
    %{state | heartbeat_timer: timer}
  end

  # Utility function to send a message to all
  # processes other than the caller. Should only be used by leader.
  @spec broadcast_to_others(%Raft{is_leader: true}, any()) :: [boolean()]
  defp broadcast_to_others(state, message) do
    me = whoami()
    state.view
    |> Enum.filter(fn pid -> pid != me end)
    |> Enum.map(fn pid -> send(pid, message) end)
  end

  @doc """
  Send all the necessary entries to a certain server.
  Use after update next_index
  """
  @spec send_entries(%Raft{is_leader: true}, atom() | pid()) :: boolean()
  def send_entries(state, pid) do
    if state.is_leader == false or pid == state.current_leader or not Enum.member?(state.view, pid) do
      raise "Invalid call of send_entries: not leader calling or destination is not valid"
    end
    last_index = get_last_log_index(state)
    if last_index >= Map.get(state.next_index, pid) do
      prev_index = Map.get(state.next_index, pid) - 1
      prev_term = case get_log_entry(state, prev_index) do
        :noentry -> 0
        entry -> entry.term
      end
      message = 
        Raft.AppendEntryRequest.new(
          state.current_term,
          state.current_leader,
          prev_index,
          prev_term,
          get_log_suffix(state, prev_index + 1),
          state.commit_index
        )
      send(pid, message)
    else
        true
    end
  end

  @spec broadcast_entries_to_others(%Raft{is_leader: true}) :: [boolean()]
  defp broadcast_entries_to_others(state) do
    me = whoami()
    last_index = get_last_log_index(state)

    state.view
    |> Enum.filter(fn pid -> pid != me end)
    |> Enum.map(fn pid -> 
      send_entries(state, pid)
    end)
  end

  # END OF UTILITY FUNCTIONS. You should not need to (but are allowed to)
  # change any of the code above this line, but will definitely need to
  # change the code that follows.

  # This function should cancel the current
  # election timer, and set  a new one. You can use
  # `get_election_time` defined above to get a
  # randomized election timeout. You might need
  # to call this function from within your code.
  @spec reset_election_timer(%Raft{}) :: %Raft{}
  defp reset_election_timer(state) do
    # TODO: Set a new election timer
    state = remove_timer(state)
    save_election_timer(state, Emulation.timer(get_election_time(state)))
  end

  # This function should cancel the current
  # hearbeat timer, and set a new one. You can
  # get heartbeat timeout from `state.heartbeat_timeout`.
  # You might need to call this from your code.
  @spec reset_heartbeat_timer(%Raft{}) :: %Raft{}
  defp reset_heartbeat_timer(state) do
    # TODO: Set a new heartbeat timer.
    state = remove_timer(state)
    save_heartbeat_timer(state, Emulation.timer(state.heartbeat_timeout))
  end

  # cancel all timer
  defp remove_timer(state) do
    if state.heartbeat_timer != nil do
      case Emulation.cancel_timer(state.heartbeat_timer) do
        false -> 
          receive do
            :timer -> nil
          end
        _ -> nil
      end
    end
    if state.election_timer != nil do
      case Emulation.cancel_timer(state.election_timer) do
        false -> 
          receive do
            :timer -> nil
          end
        _ -> nil
      end
    end
    %{state | heartbeat_timer: nil, election_timer: nil}
  end

  @doc """
  This function transitions a process so it is
  a follower.
  """
  @spec become_follower(%Raft{}) :: no_return()
  def become_follower(state) do
    # TODO: Do anything you need to when a process
    # transitions to a follower.
    IO.puts("[#{state.current_term}: #{whoami()}]: becoming follower")
    state = reset_election_timer(state)
    follower(make_follower(state), nil)
  end

  @doc """
  This function implements the state machine for a process
  that is currently a follower.

  `extra_state` can be used to hod anything that you find convenient
  when building your implementation.
  """
  @spec follower(%Raft{is_leader: false}, any()) :: no_return()
  def follower(state, extra_state) do
    state = apply_to_rsm(state)
    receive do
      :timer ->
        # IO.puts("[#{state.current_term}: #{whoami()}]: Follower election timer off")
        # need to start a election!
        become_candidate(state)

      # Messages that are a part of Raft.
      {sender,
       %Raft.AppendEntryRequest{
         term: term,
         leader_id: leader_id,
         prev_log_index: prev_log_index,
         prev_log_term: prev_log_term,
         entries: [],
         leader_commit_index: leader_commit_index
       }} ->
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Follower received empty message for term #{term} with leader #{leader_id} " <>
            "(#{leader_commit_index})"
        )
        if term < state.current_term do
          follower(state, extra_state)
        else
          state = %{state | current_term: term, current_leader: leader_id, voted_for: nil}
          state = reset_election_timer(state)
          follower(state, extra_state)
        end

      {sender,
       %Raft.AppendEntryRequest{
         term: term,
         leader_id: leader_id,
         prev_log_index: prev_log_index,
         prev_log_term: prev_log_term,
         entries: entries,
         leader_commit_index: leader_commit_index
       }} ->
        # TODO: Handle an AppendEntryRequest received by a
        # follower
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Follower received append entry for term #{term} with leader #{leader_id} " <>
            "(#{leader_commit_index})"
        )
        
        # step 1: if term < state.current_term
        if term < state.current_term do
          message = 
            Raft.AppendEntryResponse.new(
              state.current_term,
              # not sure what to return here
              get_last_log_index(state),
              false
            )
          send(sender, message)
          # not current leader, don't update election timer
          follower(state, extra_state)
        else
          # upate current_term no matter what
          max_term = max(term, state.current_term)
          state = %{state | current_term: max_term, current_leader: leader_id}
          # step 2: if log DOES contain an entry at prevLogIndex whose term matches prevLogTerm
          if prev_log_index == 0 or logged?(state, prev_log_index) and get_log_entry(state, prev_log_index).term == prev_log_term do
            # Starting from here, I trusted this leader already.

            state = truncate_log_at_index(state, prev_log_index + 1)
            state = add_log_entries(state, entries)
            # step 5
            state = %{state | commit_index: max(state.commit_index, leader_commit_index)}
            message = 
              Raft.AppendEntryResponse.new(
                state.current_term,
                get_last_log_index(state),
                true
              )
            send(sender, message)
            state = reset_election_timer(state)
            follower(state, extra_state)
          # step 2: if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
          else
            message = 
              Raft.AppendEntryResponse.new(
                state.current_term,
                get_last_log_index(state),
                false
              )
            send(sender, message)
            state = reset_election_timer(state)
            follower(state, extra_state)
          end
        end

      {sender,
       %Raft.AppendEntryResponse{
         term: term,
         log_index: index,
         success: succ
       }} ->
        # TODO: Handle an AppendEntryResponse received by
        # a follower.
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Follower received append entry response from #{sender} #{term}," <>
            " index #{index}, succcess #{inspect(succ)}"
        )

        # raise "Not yet implemented"
        follower(state, extra_state)

      {sender,
       %Raft.RequestVote{
         term: term,
         candidate_id: candidate,
         last_log_index: last_log_index,
         last_log_term: last_log_term
       }} ->
        # TODO: Handle a RequestVote call received by a
        # follower.
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Follower received RequestVote " <>
            "term = #{term}, candidate = #{candidate}"
        )
        # voted_for denote who the follower voted in current term
        # Thus, if term is updated, voted_for should be update
        state = if term > state.current_term do
          %{state | voted_for: nil}
        else
          state
        end
        granted? = 
          cond do
            state.current_term > term -> false
            state.voted_for != nil and state.voted_for != sender -> false
            get_last_log_index(state) > last_log_index -> false
            get_last_log_index(state) == last_log_index and get_last_log_term(state) > last_log_term -> false
            true -> true
          end
        message =
          Raft.RequestVoteResponse.new(
            state.current_term,
            granted?
          )
        send(sender, message)
        if granted? do
          state = %{state | voted_for: candidate, current_leader: candidate, current_term: term}
          state = reset_election_timer(state)
          follower(state, extra_state)
        else
          state = %{state | current_term: max(term, state.current_term)}
          follower(state, extra_state)
        end

      {sender,
       %Raft.RequestVoteResponse{
         term: term,
         granted: granted
       }} ->
        # TODO: Handle a RequestVoteResponse.
        IO.puts(
          "[#{state.current_term}: #{whoami}]: Follower received RequestVoteResponse " <>
            "term = #{term}, granted = #{inspect(granted)}"
        )
        follower(state, extra_state)


      # Messages from external clients. In each case we
      # tell the client that it should go talk to the
      # leader.
      {sender, :nop} ->
        # IO.puts("[#{state.current_term}: #{whoami()}]: Follower receives an nop from client")
        send(sender, {:redirect, state.current_leader})
        follower(state, extra_state)

      {sender, {:enq, item}} ->
        # IO.puts("[#{state.current_term}: #{whoami()}]: Follower receives an enq from client")
        send(sender, {:redirect, state.current_leader})
        follower(state, extra_state)

      {sender, :deq} ->
        # IO.puts("[#{state.current_term}: #{whoami()}]: Follower receives an deq from client")
        send(sender, {:redirect, state.current_leader})
        follower(state, extra_state)

      # Messages for debugging [Do not modify existing ones,
      # but feel free to add new ones.]
      {sender, :send_state} ->
        send(sender, state.queue)
        follower(state, extra_state)

      {sender, :send_log} ->
        send(sender, state.log)
        follower(state, extra_state)

      {sender, :whois_leader} ->
        send(sender, {state.current_leader, state.current_term})
        follower(state, extra_state)

      {sender, :current_process_type} ->
        send(sender, :follower)
        follower(state, extra_state)

      {sender, {:set_election_timeout, min, max}} ->
        state = %{state | min_election_timeout: min, max_election_timeout: max}
        state = reset_election_timer(state)
        send(sender, :ok)
        follower(state, extra_state)

      {sender, {:set_heartbeat_timeout, timeout}} ->
        send(sender, :ok)
        follower(%{state | heartbeat_timeout: timeout}, extra_state)
    end
  end

  @doc """
  This function transitions a process that is not currently
  the leader so it is a leader.
  """
  @spec become_leader(%Raft{is_leader: false}) :: no_return()
  def become_leader(state) do
    # TODO: Send out any one time messages that need to be sent,
    # you might need to update the call to leader too.
    IO.puts("[#{state.current_term}: #{whoami()}]: becoming leader for term #{state.current_term}")
    state = %{state | voted_for: nil, current_leader: whoami()}
    message = 
      Raft.AppendEntryRequest.new(
        state.current_term,
        state.current_leader,
        get_last_log_index(state),
        get_last_log_term(state),
        [],
        state.commit_index
      )
    broadcast_to_others(state, message)
    state = reset_heartbeat_timer(state)
    leader(make_leader(state), %{})
  end

  # tell the uncommitted clients to resend their message
  @spec tell_clients_resend(%Raft{is_leader: true}) :: no_return()
  defp tell_clients_resend(state) do
    # if there's some entries can be commited, commit them and reply to the client
    IO.puts("[#{state.current_term}: #{whoami()}]: Leader trying to tell clients to resend the outstanding requests")
    uncommitted = get_log_suffix(state, state.commit_index + 1)
    Enum.map(Enum.reverse(uncommitted), fn entry ->
      send(entry.requester, {:redirect, state.current_leader})
    end)
  end

  @doc """
  This function implements the state machine for a process
  that is currently the leader.

  `extra_state` can be used to hold any additional information.
  HINT: It might be useful to track the number of responses
  received for each AppendEntry request.
  """
  @spec leader(%Raft{is_leader: true}, any()) :: no_return()
  def leader(state, extra_state) do
    state = apply_to_rsm(state)
    receive do
      # Messages that are a part of Raft.

      :timer ->
        message = 
          Raft.AppendEntryRequest.new(
            state.current_term,
            state.current_leader,
            get_last_log_index(state),
            get_last_log_term(state),
            [],
            state.commit_index
          )
        broadcast_to_others(state, message)
        state = save_heartbeat_timer(state, Emulation.timer(state.heartbeat_timeout))
        leader(state, extra_state)

      {sender,
       %Raft.AppendEntryRequest{
         term: term,
         leader_id: leader_id,
         prev_log_index: prev_log_index,
         prev_log_term: prev_log_term,
         entries: entries,
         leader_commit_index: leader_commit_index
       }} ->
        # TODO: Handle an AppendEntryRequest seen by the leader.
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Leader received append entry for term #{term} with leader #{
            leader_id
          } " <>
            "(#{leader_commit_index})"
        )
        # if a new leader is elected, it definitely broadcast an empty message
        # because we assume the message is received in order, the first one we received is an empty one
        cond do
          term < state.current_term ->
            IO.puts("[#{state.current_term}: #{whoami()}]: Leader receive a message from former leader #{sender}")
            # I suppose I have already send empty message to everyone, so I'm not gonna send it again for this previous leader
            leader(state, extra_state)
          term == state.current_term ->
            raise "Each term should only have one leader"
          true ->
            if length(entries) != 0 do
              raise "A new leader send me a non-empty message before broadcast an empty message?"
            end
            state = %{state | current_term: term, current_leader: leader_id}
            tell_clients_resend(state)
            become_follower(state)
        end

      {sender,
       %Raft.AppendEntryResponse{
         term: term,
         log_index: index,
         success: succ
       }} ->
        # TODO: Handle an AppendEntryResposne received by the leader.
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Leader received append entry response #{term} from #{sender}, " <>
            " index #{index}, succcess #{succ}"
        )

        if term < state.current_term do
          raise "What? Why don't you update your current term???"
        end

        # record client in extra_state, and send client if the log is committed
        if succ do
          if term > state.current_term do
            raise "I don't think this is possible. If term is larger than leader's term, it will return false!"
          end
          # For a specific follower, The message is received by order.
          # Thus, I assume this max is actually unnecessary
          state = %{state | 
            match_index: Map.update!(state.match_index, sender, &(max(&1, index))),
            next_index: Map.update!(state.next_index, sender, &(max(&1, index + 1)))} 
          # The leader has to calculate the number of successful respond
          if Map.has_key?(extra_state, index) do
            current_poll = List.delete(Map.get(extra_state, index), sender)
            # length(state.view) - length(current_poll) = num_of_replicated
            # num_of_replicated > length(current_poll) means majority of the servers has this entry
            if length(state.view) > 2 * length(current_poll) do
              extra_state = Map.drop(extra_state, [index])
              # only if we finish a poll, we need to update the commit index
              state = update_commit_index(state)
              leader(state, extra_state)
            else
              # This entry is not replicated to majority yet
              leader(state, Map.replace!(extra_state, index, current_poll))
            end
          else
            # the poll is finished, nothing changes
            leader(state, extra_state)
          end
        else
          # either term is not right
          if term > state.current_term do
            state = %{state | current_term: term, current_leader: sender}
            tell_clients_resend(state)
            become_follower(state)
          # or the follower can't match this one
          else
            # if index is much smaller than next_index.sender - 1, this min() here can reduce times of resending
            # max() is to make sure next_index.sender >= 1
            next_index = Map.update!(state.next_index, sender, &(max(min(&1 - 1, index), 1)))
            state = %{state | next_index: next_index}
            send_entries(state, sender)
            leader(state, extra_state)
          end
        end


      {sender,
       %Raft.RequestVote{
         term: term,
         candidate_id: candidate,
         last_log_index: last_log_index,
         last_log_term: last_log_term
       }} ->
        # TODO: Handle a RequestVote call at the leader.
        granted? = 
          cond do
            state.current_term >= term -> false
            state.voted_for != nil and state.voted_for != sender -> false
            get_last_log_index(state) > last_log_index -> false
            get_last_log_index(state) == last_log_index and get_last_log_term(state) > last_log_term -> false
            true -> true
          end
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Leader received RequestVote " <>
            "term = #{term}, candidate = #{candidate}, granted = #{granted?}"
        )
        message =
          Raft.RequestVoteResponse.new(
            state.current_term,
            granted?
          )
        send(sender, message)
        if term > state.current_term do
          state = if granted? do %{state | voted_for: sender} else state end
          state = %{state | current_leader: candidate, current_term: term}
          tell_clients_resend(state)
          become_follower(state)
        else
          leader(state, extra_state)
        end

      {sender,
       %Raft.RequestVoteResponse{
         term: term,
         granted: granted
       }} ->
        # TODO: Handle RequestVoteResponse at a leader.         
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Leader received RequestVoteResponse " <>
            "term = #{term}, granted = #{inspect(granted)}"
        )
        # maybe just ignore these message?
        leader(state, extra_state)

      # Messages from external clients. For all of what follows
      # you should send the `sender` an :ok (see `Raft.Client`
      # below) only after the request has completed, i.e., after
      # the log entry corresponding to the request has been **committed**.
      {sender, :nop} ->
        # TODO: entry is the log entry that you need to
        # append.
        # IO.puts("[#{state.current_term}: #{whoami()}]: Leader receives an nop from client")
        entry =
          Raft.LogEntry.nop(
            get_last_log_index(state) + 1,
            state.current_term,
            sender
          )
        state = add_log_entries(state, [entry])
        new_poll = 
          state.view
            |> Enum.filter(fn pid -> pid != whoami() end)
        broadcast_entries_to_others(state) 
        state = reset_heartbeat_timer(state)
        leader(state, Map.put(extra_state, entry.index, new_poll))

      {sender, {:enq, item}} ->
        # TODO: entry is the log entry that you need to
        # append.
        # IO.puts("[#{state.current_term}: #{whoami()}]: Leader receives an enq from client")
        entry =
          Raft.LogEntry.enqueue(
            get_last_log_index(state) + 1,
            state.current_term,
            sender,
            item
          )
        state = add_log_entries(state, [entry])
        new_poll = 
          state.view
            |> Enum.filter(fn pid -> pid != whoami() end)
        broadcast_entries_to_others(state) 
        state = reset_heartbeat_timer(state)
        leader(state, Map.put(extra_state, entry.index, new_poll))

      {sender, :deq} ->
        # TODO: entry is the log entry that you need to
        # append.
        # IO.puts("[#{state.current_term}: #{whoami()}]: Leader receives an deq from client")
        entry =
          Raft.LogEntry.dequeue(
            get_last_log_index(state) + 1,
            state.current_term,
            sender
          )
        state = add_log_entries(state, [entry])
        new_poll = 
          state.view
            |> Enum.filter(fn pid -> pid != whoami() end)
        broadcast_entries_to_others(state) 
        state = reset_heartbeat_timer(state)
        leader(state, Map.put(extra_state, entry.index, new_poll))


      # Messages for debugging [Do not modify existing ones,
      # but feel free to add new ones.]
      {sender, :send_state} ->
        send(sender, state.queue)
        leader(state, extra_state)

      {sender, :send_log} ->
        send(sender, state.log)
        leader(state, extra_state)

      {sender, :whois_leader} ->
        send(sender, {whoami(), state.current_term})
        leader(state, extra_state)

      {sender, :current_process_type} ->
        send(sender, :leader)
        leader(state, extra_state)

      {sender, {:set_election_timeout, min, max}} ->
        send(sender, :ok)

        leader(
          %{state | min_election_timeout: min, max_election_timeout: max},
          extra_state
        )

      {sender, {:set_heartbeat_timeout, timeout}} ->
        state = %{state | heartbeat_timeout: timeout}
        state = reset_heartbeat_timer(state)
        send(sender, :ok)
        leader(state, extra_state)
    end
  end

  @doc """
  This function transitions a process to candidate.
  """
  @spec become_candidate(%Raft{is_leader: false}) :: no_return()
  def become_candidate(state) do
    # TODO:   Send out any messages that need to be sent out
    # you might need to update the call to candidate below.
    me = whoami()
    # a server become candidate because timesout, so I have to set election timer to nil here
    state = %{state | election_timer: nil}
    state = %{state | is_leader: true, current_leader: me, current_term: state.current_term + 1, voted_for: me}
    IO.puts("[#{state.current_term}: #{me}]: becoming a candidate, term #{state.current_term}")
    message = 
      Raft.RequestVote.new(
        state.current_term,
        me,
        get_last_log_index(state),
        get_last_log_term(state)
      )
    broadcast_to_others(state, message)
    state = reset_election_timer(state)
    poll = 
      state.view
        |> Enum.filter(fn pid -> pid != whoami() end)
    candidate(state, poll)
  end

  @doc """
  This function implements the state machine for a process
  that is currently a candidate.

  `extra_state` can be used to store any additional information
  required, e.g., to count the number of votes received.
  """
  @spec candidate(%Raft{is_leader: false}, any()) :: no_return()
  def candidate(state, extra_state) do
    receive do
      :timer ->
        # start new election
        IO.puts("[#{state.current_term}: #{whoami()}]: Candidate timer off, start new election")
        become_candidate(state)
      {sender,
       %Raft.AppendEntryRequest{
         term: term,
         leader_id: leader_id,
         prev_log_index: prev_log_index,
         prev_log_term: prev_log_term,
         entries: [],
         leader_commit_index: leader_commit_index
       }} ->
        # new leader is probably elected
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Candidate received empty message for term #{term} with leader #{leader_id} " <>
            "(#{leader_commit_index})"
        )
        if term < state.current_term do
          candidate(state, extra_state)
        else
          state = %{state | voted_for: nil, current_term: term, current_leader: leader_id}
          become_follower(state)
        end

      {sender,
       %Raft.AppendEntryRequest{
         term: term,
         leader_id: leader_id,
         prev_log_index: prev_log_index,
         prev_log_term: prev_log_term,
         entries: entries,
         leader_commit_index: leader_commit_index
       }} ->
        # TODO: Handle an AppendEntryRequest as a candidate
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Candidate received append entry for term #{term} " <>
            "with leader #{leader_id} " <>
            "(#{leader_commit_index})"
        )
        if term < state.current_term do
          candidate(state, extra_state)
        else
          raise "Why this leader #{sender} doesn't broadcast empty message to all after elected?"
        end
        # raise "Not yet implemented"

      {sender,
       %Raft.AppendEntryResponse{
         term: term,
         log_index: index,
         success: succ
       }} ->
        # TODO: Handle an append entry response as a candidate
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Candidate received append entry response #{term}," <>
            " index #{index}, succcess #{succ}"
        )

        candidate(state, extra_state)

      {sender,
       %Raft.RequestVote{
         term: term,
         candidate_id: candidate,
         last_log_index: last_log_index,
         last_log_term: last_log_term
       }} ->
        # TODO: Handle a RequestVote response as a candidate.
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: received RequestVote " <>
            "term = #{term}, candidate = #{candidate}"
        )

        granted? = 
          cond do
            # difference between follower: if term == current_term, can't vote
            state.current_term >= term -> false
            get_last_log_index(state) > last_log_index -> false
            get_last_log_index(state) == last_log_index and get_last_log_term(state) > last_log_term -> false
            true -> true
          end
        # state = if granted? do %{state | voted_for: sender} else state end
        message =
          Raft.RequestVoteResponse.new(
            state.current_term,
            granted?
          )
        send(sender, message)
        if granted? do
          # term must be greater than current_term
          state = %{state | voted_for: candidate, current_term: term}
          become_follower(state)
        else
          candidate(state, extra_state)
        end

      {sender,
       %Raft.RequestVoteResponse{
         term: term,
         granted: granted
       }} ->
        # TODO: Handle a RequestVoteResposne as a candidate.
        IO.puts(
          "[#{state.current_term}: #{whoami()}]: Candidate received RequestVoteResponse from #{sender} " <>
            "term = #{term}, granted = #{inspect(granted)}"
        )
        state = reset_election_timer(state)
        if granted do
          extra_state = List.delete(extra_state, sender)
          if length(state.view) > 2 * length(extra_state) do
            IO.puts("[#{state.current_term}: #{whoami}]: elected as new leader for term #{state.current_term}")
            # majority have voted
            become_leader(state)
          else
            candidate(state, extra_state)
          end
        else
          candidate(state, extra_state)
        end

      # Messages from external clients.
      {sender, :nop} ->
        # Redirect in hopes that the current process
        # eventually gets elected leader.
        # IO.puts("[#{state.current_term}: #{whoami()}]: Candidate receives an nop from client")
        send(sender, {:redirect, whoami()})
        candidate(state, extra_state)

      {sender, {:enq, item}} ->
        # Redirect in hopes that the current process
        # eventually gets elected leader.
        # IO.puts("[#{state.current_term}: #{whoami()}]: Candidate receives an enq from client")
        send(sender, {:redirect, whoami()})
        candidate(state, extra_state)

      {sender, :deq} ->
        # Redirect in hopes that the current process
        # eventually gets elected leader.
        # IO.puts("[#{state.current_term}: #{whoami()}]: Candidate receives an deq from client")
        send(sender, {:redirect, whoami()})
        candidate(state, extra_state)

      # Messages for debugging [Do not modify existing ones,
      # but feel free to add new ones.]
      {sender, :send_state} ->
        send(sender, state.queue)
        candidate(state, extra_state)

      {sender, :send_log} ->
        send(sender, state.log)
        candidate(state, extra_state)

      {sender, :whois_leader} ->
        send(sender, {:candidate, state.current_term})
        candidate(state, extra_state)

      {sender, :current_process_type} ->
        send(sender, :candidate)
        candidate(state, extra_state)

      {sender, {:set_election_timeout, min, max}} ->
        state = %{state | min_election_timeout: min, max_election_timeout: max}
        state = reset_election_timer(state)
        send(sender, :ok)
        candidate(state, extra_state)

      {sender, {:set_heartbeat_timeout, timeout}} ->
        send(sender, :ok)
        candidate(%{state | heartbeat_timeout: timeout}, extra_state)
    end
  end
end

defmodule Raft.Client do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @moduledoc """
  A client that can be used to connect and send
  requests to the RSM.
  """
  alias __MODULE__
  @enforce_keys [:leader]
  defstruct(leader: nil)

  @doc """
  Construct a new Raft Client. This takes an ID of
  any process that is in the RSM. We rely on
  redirect messages to find the correct leader.
  """
  @spec new_client(atom()) :: %Client{leader: atom()}
  def new_client(member) do
    %Client{leader: member}
  end

  @doc """
  Send a nop request to the RSM.
  """
  @spec nop(%Client{}) :: {:ok, %Client{}}
  def nop(client) do
    leader = client.leader
    send(leader, :nop)

    receive do
      {_, {:redirect, new_leader}} ->
        nop(%{client | leader: new_leader})

      {_, :ok} ->
        {:ok, client}
    end
  end

  @doc """
  Send a dequeue request to the RSM.
  """
  @spec deq(%Client{}) :: {:empty | {:value, any()}, %Client{}}
  def deq(client) do
    leader = client.leader
    send(leader, :deq)

    receive do
      {_, {:redirect, new_leader}} ->
        deq(%{client | leader: new_leader})

      {_, v} ->
        {v, client}
    end
  end

  @doc """
  Send an enqueue request to the RSM.
  """
  @spec enq(%Client{}, any()) :: {:ok, %Client{}}
  def enq(client, item) do
    leader = client.leader
    send(leader, {:enq, item})

    receive do
      {_, :ok} ->
        {:ok, client}

      {_, {:redirect, new_leader}} ->
        enq(%{client | leader: new_leader}, item)
    end
  end
end
