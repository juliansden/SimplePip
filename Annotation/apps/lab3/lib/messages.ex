defmodule Raft.LogEntry do
  @moduledoc """
  Log entry for Raft implementation.
  """
  alias __MODULE__
  @enforce_keys [:index, :term]
  defstruct(
    index: nil,
    term: nil,
    operation: nil,
    requester: nil,
    argument: nil,
    id: nil
  )

  @doc """
  Return an empty log entry, this is mostly
  used for convenience.
  """
  @spec empty() :: %LogEntry{index: 0, term: 0}
  def empty do
    %LogEntry{index: 0, term: 0}
  end

  @doc """
  Return a nop entry for the given index.
  """
  @spec nop(non_neg_integer(), non_neg_integer(), atom(), {atom() | pid(), non_neg_integer()}) :: %LogEntry{
          index: non_neg_integer(),
          term: non_neg_integer(),
          requester: atom() | pid(),
          operation: :nop,
          argument: none(),
          id: {atom() | pid(), non_neg_integer()}
        }
  def nop(index, term, requester, id) do
    %LogEntry{
      index: index,
      term: term,
      requester: requester,
      operation: :nop,
      argument: nil,
      id: id
    }
  end

  @doc """
  Return a log entry for an `enqueue` operation.
  """
  @spec enqueue(non_neg_integer(), non_neg_integer(), atom(), any(), {atom() | pid(), non_neg_integer()}) ::
          %LogEntry{
            index: non_neg_integer(),
            term: non_neg_integer(),
            requester: atom() | pid(),
            operation: :enq,
            argument: any(),
            id: {atom() | pid(), non_neg_integer()}
          }
  def enqueue(index, term, requester, item, id) do
    %LogEntry{
      index: index,
      term: term,
      requester: requester,
      operation: :enq,
      argument: item,
      id: id
    }
  end

  @doc """
  Return a log entry for a `dequeue` operation.
  """
  @spec dequeue(non_neg_integer(), non_neg_integer(), atom(), {atom() | pid(), non_neg_integer()}) :: %LogEntry{
          index: non_neg_integer(),
          term: non_neg_integer(),
          requester: atom() | pid(),
          operation: :enq,
          argument: none(),
          id: {atom() | pid(), non_neg_integer()}
        }
  def dequeue(index, term, requester, id) do
    %LogEntry{
      index: index,
      term: term,
      requester: requester,
      operation: :deq,
      argument: nil,
      id: id
    }
  end
end

defmodule Raft.AppendEntryRequest do
  @moduledoc """
  AppendEntries RPC request.
  """
  alias __MODULE__

  # Require that any AppendEntryRequest contains
  # a :term, :leader_id, :prev_log_index, and :leader_commit_index.
  @enforce_keys [
    :term,
    :leader_id,
    :prev_log_index,
    :prev_log_term,
    :leader_commit_index
  ]
  defstruct(
    term: nil,
    leader_id: nil,
    prev_log_index: nil,
    prev_log_term: nil,
    entries: nil,
    leader_commit_index: nil,
    id: nil
  )

  @doc """
  Create a new AppendEntryRequest
  """

  @spec new(
          non_neg_integer(),
          atom(),
          non_neg_integer(),
          non_neg_integer(),
          list(any()),
          non_neg_integer(),
          {atom() | pid(), non_neg_integer()}
        ) ::
          %AppendEntryRequest{
            term: non_neg_integer(),
            leader_id: atom(),
            prev_log_index: non_neg_integer(),
            prev_log_term: non_neg_integer(),
            entries: list(any()),
            leader_commit_index: non_neg_integer(),
            id: {atom() | pid(), non_neg_integer()}
          }
  def new(
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit_index,
        id
      ) do
    %AppendEntryRequest{
      term: term,
      leader_id: leader_id,
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      entries: entries,
      leader_commit_index: leader_commit_index,
      id: id
    }
  end
end

defmodule Raft.AppendEntryResponse do
  @moduledoc """
  Response for the AppendEntryRequest
  """
  alias __MODULE__
  @enforce_keys [:term, :log_index, :success]
  defstruct(
    term: nil,
    # used to relate request with response.
    log_index: nil,
    success: nil,
    id: nil
  )

  @doc """
  Create a new AppendEntryResponse.
  """
  @spec new(non_neg_integer(), non_neg_integer(), boolean(), {atom() | pid(), non_neg_integer()}) ::
          %AppendEntryResponse{
            term: non_neg_integer(),
            log_index: non_neg_integer(),
            success: boolean(),
            id: {atom() | pid(), non_neg_integer()}
          }
  def new(term, prevIndex, success, id) do
    %AppendEntryResponse{
      term: term,
      log_index: prevIndex,
      success: success,
      id: id
    }
  end
end

defmodule Raft.RequestVote do
  @moduledoc """
  Arguments when requestion votes.
  """
  alias __MODULE__
  @enforce_keys [:term, :candidate_id, :last_log_index, :last_log_term]
  defstruct(
    term: nil,
    candidate_id: nil,
    last_log_index: nil,
    last_log_term: nil,
    id: nil
  )

  @doc """
  Create a new RequestVote request.
  """
  @spec new(non_neg_integer(), atom(), non_neg_integer(), non_neg_integer(), {atom() | pid(), non_neg_integer()}) ::
          %RequestVote{
            term: non_neg_integer(),
            candidate_id: atom(),
            last_log_index: non_neg_integer(),
            last_log_term: non_neg_integer(),
            id: {atom() | pid(), non_neg_integer()}
          }
  def new(term, pid, last_log_index, last_log_term, id) do
    %RequestVote{
      term: term,
      candidate_id: pid,
      last_log_index: last_log_index,
      last_log_term: last_log_term,
      id: id
    }
  end
end

defmodule Raft.RequestVoteResponse do
  @moduledoc """
  Response for RequestVote requests.
  """
  alias __MODULE__
  @enforce_keys [:term, :granted]
  defstruct(
    term: nil,
    granted: nil,
    id: nil
  )

  @doc """
  Create a new RequestVoteResponse.
  """
  @spec new(non_neg_integer(), boolean(), {atom() | pid(), non_neg_integer()}) ::
          %RequestVoteResponse{
            term: non_neg_integer(),
            granted: boolean(),
            id: {atom() | pid(), non_neg_integer()}
          }
  def new(term, granted, id) do
    %RequestVoteResponse{term: term, granted: granted, id: id}
  end
end
