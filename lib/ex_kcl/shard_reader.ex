defmodule ExKcl.ShardReader do
  use GenServer
  require Logger

  defmodule State do
    defstruct [
      adapter: nil,
      handler: nil,
      stream_name: nil,
      lease: nil,
      repo: nil,
      iterator: nil,
      pending: nil,
      worker_sup: nil,
      worker_id: nil,
      supervisor_registry: nil,
      task_supervisor: nil
    ]
  end

  alias ExKcl.{
    Lease,
    LeaseRepo,
  }

  def start_link(opts, lease) do
    GenServer.start_link(__MODULE__, {opts, lease}, name: via_tuple(opts, lease))
  end

  def via_tuple(opts, %Lease{shard_id: shard_id}) do
    {:via, Registry, {opts[:producer_registry], shard_id}}
  end

  def init({opts, lease}) do
    state = %State{
      lease:       lease,
      adapter:     opts[:adapter],
      handler:     opts[:handler],
      repo:        opts[:repo],
      stream_name: opts[:stream_name],
      worker_sup:  opts[:worker_sup],
      worker_id:   opts[:worker_id],
      task_supervisor: opts[:task_supervisor],
      supervisor_registry: opts[:supervisor_registry],
    }

    Process.send(self(), :fetch_records, [])
    {:ok, state}
  end

  def handle_info(:fetch_records, state) do
    state
    |> checkpoint()
    |> fetch_records()
  end

  defp fetch_records(%State{adapter: adapter, stream_name: stream_name, lease: lease, iterator: iterator, worker_id: worker_id} = state) when is_nil(iterator) do
    case adapter.get_shard_iterator(stream_name, lease) do
      {:ok, %{"ShardIterator" => iterator}} ->
        Logger.info "#{worker_id} has started #{lease.shard_id} at #{lease.checkpoint}"
        %State{state | iterator: iterator}
        |> fetch_records()

      {:error, {"ResourceNotFoundException", "Requested resource not found: Shard does not exist"}} ->
        %State{state | pending: "SHARD_END"}
        |> checkpoint()
    end
  end

  defp fetch_records(%State{lease: lease} = state) when is_nil(lease) do
    {:noreply, state}
  end

  defp fetch_records(%State{lease: %Lease{checkpoint: "SHARD_END"}} = state) do
    :ok = stop_supervisor(state)
    {:noreply, state}
  end

  defp fetch_records(%State{adapter: adapter, iterator: iterator} = state) do
    adapter.get_records(iterator)
    |> dispatch_records(state)
  end

  defp checkpoint(%State{pending: pending} = state) when is_nil(pending) do
    state
  end

  defp checkpoint(%State{pending: pending, lease: lease, repo: repo} = state) do
    case LeaseRepo.checkpoint(repo, lease, pending) do
      {:error, :stolen} ->
        :ok = stop_supervisor(state)
        %State{state | lease: nil}

      {:ok, %Lease{checkpoint: "SHARD_END"} = lease} ->
        :ok = stop_supervisor(state)
        %State{state | lease: lease}

      {:ok, lease} ->
        %State{state | lease: lease}
    end
  end

  defp run(records, state) do
    ExKcl.RecordHandler.handle_records(state, state.handler, records)
  end

  defp dispatch_records(%{"Records" => [], "NextShardIterator" => iterator}, %State{lease: %Lease{checkpoint: "TRIM_HORIZON"}} = state) do
    state =
      %State{state | iterator: iterator, pending: "SHARD_END"}
      |> checkpoint()

    {:noreply, state}
  end

  defp dispatch_records(%{"Records" => [], "NextShardIterator" => iterator}, state) do
    Process.send_after(self(), :fetch_records, 2000) # TODO - config timeout
    {:noreply,  %State{state | iterator: iterator, pending: state.lease.checkpoint}}
  end

  defp dispatch_records(%{"Records" => records, "NextShardIterator" => iterator}, state) do
    checkpoint = records |> List.last() |> state.adapter.checkpoint_for_record()

    state = %State{state | iterator: iterator,  pending: checkpoint}
    :ok = run(records, state)

    Process.send_after(self(), :fetch_records, 0) # TODO - config timeout
    {:noreply, state}
  end

  defp dispatch_records(%{"Records" => records}, state) do
    :ok = run(records, state)

    state =
      %State{state | pending: "SHARD_END"}
      |> checkpoint()

    {:noreply, state}
  end

  defp stop_supervisor(%State{worker_sup: worker_sup, supervisor_registry: supervisor_registry, lease: lease}) do
    :ok = ExKcl.ShardWorkerSupervisor.stop_worker(worker_sup, supervisor_registry, lease)
  end
end
