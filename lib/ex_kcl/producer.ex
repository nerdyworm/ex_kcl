defmodule ExKcl.Producer do
  use GenStage
  require Logger

  alias ExKcl.{
    LeaseRepo,
  }

  def start_link(lease, options) do
    #GenStage.start_link(__MODULE__, {lestream, options}, name: name(stream))
    GenStage.start_link(__MODULE__, {lease, options})
  end

  def init({lease, options}) do
    state = %{
      options: options,
      adapter: options[:adapter],
      stream_name: options[:stream_name],
      repo: options[:repo],
      demand: 0,
      timer: nil,
      lease: lease,
      iterator: nil,
    }

    producer_options = [dispatcher: GenStage.BroadcastDispatcher]
    {:producer, state, producer_options}
  end

  def handle_info(:pop, state) do
    dispatch(state)
  end

  def handle_demand(incoming_demand, %{demand: demand} = state) do
    new_demand = incoming_demand + demand
    IO.puts "handle_demand: #{incoming_demand} + #{demand} = #{new_demand}"

    %{state | demand: new_demand}
    |> dispatch()
  end

  def dispatch(%{
    iterator: iterator,
    adapter: adapter,
    stream_name: stream_name,
    lease: lease,
  } = state) when is_nil(iterator) do
    case adapter.get_shard_iterator(stream_name, lease) do
      {:ok, %{"ShardIterator" => iterator}} ->
        Logger.info "[ex_kcl] #{lease.shard_id} started at #{lease.checkpoint}"

        %{state | iterator: iterator}
        |> dispatch()

      #{:error, {"ResourceNotFoundException", "Requested resource not found: Shard does not exist"}} ->
        #%State{state | pending: "SHARD_END"}
        #|> checkpoint()
    end
  end


  def handle_call({:checkpoint, record}, from, %{adapter: adapter, lease: lease, repo: repo} = state) do
    checkpoint =
      record
      |> adapter.checkpoint_for_record()

    state =
      case LeaseRepo.checkpoint(repo, lease, checkpoint) do
        {:error, :stolen} ->
          #:ok = stop_supervisor(state)
          %{state | lease: nil}

        #{:ok, %Lease{checkpoint: "SHARD_END"} = lease} ->
          #:ok = stop_supervisor(state)
          #%{state | lease: lease}

        {:ok, lease} ->
          %{state | lease: lease}
      end

    {:noreply, [], state}
  end

  def dispatch(%{demand: demand, timer: timer, adapter: adapter, iterator: iterator} = state) do
    if timer do
      :erlang.cancel_timer(timer)
    end

    results = adapter.get_records(iterator)
    # todo - we will transition from
    # nil -> iterator -> nil... need to shutdown after
     # last nil
    events = Map.get(results, "Records")
    iterator = Map.get(results, "NextShardIterator")
    #checkpoint = events |> List.last() |> adapter.checkpoint_for_record()
    #IO.inspect checkpoint

    new_events = length(events)
    demand = demand - new_events
    state = %{state | demand: demand, iterator: iterator}

    state =
      if demand > 0 || new_events == 0 do
        timer = Process.send_after(self(), :pop, 500)
        %{state | timer: timer}
      else
        state
      end

    {:noreply, events, state}
  end
end
