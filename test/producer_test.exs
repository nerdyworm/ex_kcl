defmodule Forwarder do
  use GenStage

  defstruct [:listener]

  def start_link(options) do
    GenStage.start_link(__MODULE__, options)
  end

  def init([listener: listener]) do
    {:consumer, %__MODULE__{listener: listener}}
  end

  def handle_events(events, {pid, _ref}, state) do
    #Enum.each(events, fn(event) ->
      #IO.inspect event["dynamodb"]["Keys"]
    #end)

    send(state.listener, events)
    :ok = GenStage.call(pid, {:checkpoint, events |> List.last()})
    {:noreply, [], state}
  end
end

defmodule ExKcl.ProducerTest do
  use ExUnit.Case

  alias ExKcl.{
    Lease,
    LeaseRepo,
    Shard,
    Adapters.Dynamodb
  }

  def config(repo) do
    [
      adapter: Dynamodb,
      stream_name: "arn:aws:dynamodb:us-east-1:907015576586:table/SubZero.Events/stream/2017-01-22T01:45:50.219",
      repo: repo
    ]
  end

  setup do
    opts = [repo: :testing, lease_table_name: "leases_test"]
    {:ok, _} = LeaseRepo.start_link(opts)
    LeaseRepo.clear(opts[:repo])
    {:ok, opts}
  end

  @tag timeout: 60_000 * 60
  test "polls from a shard", %{repo: repo} do
    shard = %Shard{shard_id: "shardId-00000001491865608579-fc9175b5"}
    assert :ok = LeaseRepo.create(repo, shard)
    assert {:ok, lease} = LeaseRepo.get(repo, shard.shard_id)

    {:ok, producer} = ExKcl.Producer.start_link(lease, config(repo))
    {:ok, forwarder} = Forwarder.start_link(listener: self())
    {:ok, _} = GenStage.sync_subscribe(forwarder, to: producer)

    assert_receive events, 5000
    assert length(events) > 0

    {:ok, lease} = LeaseRepo.get(repo, shard.shard_id)
    refute lease.checkpoint == "TRIM_HORIZON"
  end
end
