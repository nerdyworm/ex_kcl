defmodule ExKcl.LeaseTaker do
  @moduledoc """
  Takes leases that need to be worked
  """
  use GenServer


  alias ExKcl.{
    Lease,
    LeaseRepo,
  }

  defmodule State do
    defstruct [
      worker_id: nil,
      repo: nil,
      leases: [],
      times: %{},
      max_leases_per_worker: 1,
      lease_stale_after: 10_0000
    ]
  end

  def start_link(worker_id, opts) do
    GenServer.start_link(__MODULE__, {worker_id, opts}, name: opts[:taker])
  end

  def init({worker_id, opts}) do
    {:ok, %State{worker_id: worker_id, repo: opts[:repo], max_leases_per_worker: opts[:max_leases_per_worker], lease_stale_after: opts[:lease_stale_after]}}
  end

  def take(taker) do
    GenServer.call(taker, :take)
  end

  defp get_expired_leases(leases, new_times, start_time, lease_stale_after) do
    Enum.filter(leases, fn(%Lease{shard_id: shard_id}) ->
      last_scanned = Map.get(new_times, shard_id)
      start_time - last_scanned > lease_stale_after
    end)
  end

  def handle_call(:take, _, %State{
    repo: repo,
    worker_id: worker_id,
    leases: old_leases,
    times: times,
    max_leases_per_worker: max_leases_for_worker,
    lease_stale_after: lease_stale_after,
  } = state) do
    start_time = :os.system_time(:milli_seconds)

    {:ok, leases} = LeaseRepo.list(repo)

    # TODO - should probalby just delete the finished leases
    # and prevent them from being recreated
    leases = Enum.filter(leases, fn(lease) ->
      !Lease.finished?(lease)
    end)

    # calculate the last time the leases were updated
    new_times =
      Enum.reduce(leases, times, fn(new_lease, new_times) ->
        shard_id = new_lease.shard_id
        old_lease = Enum.find(old_leases, &(&1.shard_id == shard_id))
        if old_lease do
          if old_lease.counter == new_lease.counter do
            new_times
          else
            Map.put(new_times, shard_id, start_time)
          end
        else
          if new_lease.owner == nil do
            Map.put(new_times, shard_id, 0)
          else
            Map.put(new_times, shard_id, start_time)
          end
        end
      end)

    expired = get_expired_leases(leases, new_times, start_time, lease_stale_after)

    counts = compute_lease_counts(state, leases, expired)
    num_leases = length(leases)
    num_workers = length(Map.keys(counts))
    # if leases == 0 early return here

    # TODO -  handle spill over
    target = compute_target(num_leases, num_workers, max_leases_for_worker)

    my_count = Map.get(counts, state.worker_id)
    leases_to_reach_target = target - my_count

    #IO.inspect counts
    #IO.puts "target: #{target}"
    #IO.puts "my_count: #{my_count}"
    #IO.puts "expired leases: `#{Enum.map(expired, &(&1.shard_id)) |> Enum.join(", ")}`"

    # if zero then early return

    # shuffle all the leases so that workers don't contend for the same lease

    # if we have expired leases we should take those
    # if we do not, then we need to steal a lease from another worker to balance the cluster
    taken = expired |> Enum.shuffle |> Enum.take(leases_to_reach_target)


    #IO.puts "leases to reach target: #{leases_to_reach_target}, target: #{target}"
    #IO.puts "TAKING: #{Enum.map(taken, &(&1.shard_id))}"

    taken = Enum.map(taken, fn(lease) ->
      {:ok, lease} = LeaseRepo.take(repo, lease, worker_id)
      lease
    end)

    state = %State{state | leases: leases, times: new_times}
    {:reply, {:ok, taken}, state}
  end

  defp compute_lease_counts(state, leases, expired) do
    Enum.reduce(leases, %{}, fn(%Lease{shard_id: shard_id} = lease, acc) ->
      if lease.owner == nil do
        acc
      else
        if Enum.find(expired, &(&1.shard_id == shard_id)) do
          acc
        else
          Map.update(acc, lease.owner, 1, &(&1 + 1))
        end
      end
    end)
    |> Map.update(state.worker_id, 0, &(&1)) # update my worker count just incase we do not have any leases
  end

  def compute_target(num_leases, num_workers, max_leases_per_worker) do
    target =
      if num_workers >= num_leases do
        1
      else
        overflow =
          if rem(num_leases, num_workers) == 0 do
            0
          else
            1
          end

        Integer.floor_div(num_leases, num_workers) + overflow
      end

    if target > max_leases_per_worker do
      max_leases_per_worker
    else
      target
    end
  end
end

