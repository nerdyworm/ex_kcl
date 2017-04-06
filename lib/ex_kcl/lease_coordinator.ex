defmodule ExKcl.LeaseCoordinator do
  @moduledoc """
  Coordinates a LeaseTaker and LeaseRenewer.  It handles the scheduling
  of these two servers.
  """

  use GenServer

  alias ExKcl.{
    LeaseRepo,
    LeaseTaker
  }

  require Logger

  defmodule State do
    defstruct [
      worker_id: nil,
      repo: nil,
      taker: nil,
      worker_sup: nil,
      workers: %{},
      timeout: 0,
    ]
  end

  def start_link(worker_id, opts) do
    GenServer.start_link(__MODULE__, {worker_id, opts}, name: opts[:coordinator])
  end

  def init({worker_id, opts}) do
    Process.flag(:trap_exit, true)

    state = %State{
      worker_id:  worker_id,
      repo:       opts[:repo],
      taker:      opts[:taker],
      worker_sup: opts[:worker_sup],
      timeout:    opts[:coordinator_sync_interval]
    }

    {:ok, state, state.timeout}
  end

  def handle_info(:timeout, %State{worker_id: worker_id, taker: taker, worker_sup: worker_sup, workers: workers} = state) do
    {:ok, leases} = LeaseTaker.take(taker)

    workers =
      Enum.reduce(leases, workers, fn(lease, workers) ->
        case ExKcl.ShardWorkerSupervisor.start_worker(worker_sup, lease) do
          {:ok, pid} ->
            Process.monitor(pid)
            Map.put(workers, pid, lease.shard_id)
          {:error, {:already_started, _}} ->
            workers

          {:error, {:shutdown, reason}} ->
            Logger.error "failed to start worker: #{inspect reason}"
            workers
        end
      end)

    {:noreply, %State{state | workers: workers}, state.timeout}
  end

  def handle_info({:DOWN, _ref, :process, pid, :shutdown}, %State{worker_id: worker_id, workers: workers, repo: repo} = state) do
    shard_id = Map.get(workers, pid)
    Logger.info "[ex_kcl] #{worker_id} #{shard_id} shutdown"

    :ok = LeaseRepo.release(repo, shard_id, worker_id)
    workers = Map.delete(workers, pid)
    {:noreply, %State{state | workers: workers}, 0}
  end

  def terminate(_, _) do
    :normal
  end
end
