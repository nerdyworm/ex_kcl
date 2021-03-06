defmodule ExKcl.ShardSyncer do
  @moduledoc """
  Polls for new shards and creates leases for them
  """
  use GenServer
  require Logger

  alias ExKcl.{
    LeaseRepo,
    LeaseCoordinator,
  }

  defmodule State do
    defstruct [
      stream_name: nil,
      adapter: nil,
      repo: nil,
      sync_interval: nil
    ]
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    state = %State{
      adapter:       opts[:adapter],
      repo:          opts[:repo],
      stream_name:   opts[:stream_name],
      sync_interval: opts[:shard_syncer_sync_interval]
    }

    {:ok, state, opts[:shard_syncer_start_timeout]}
  end

  def sync(syncer) do
    GenServer.call(syncer, :sync)
  end

  def handle_info(:timeout,  %State{sync_interval: sync_interval} = state) do
    :ok = sync_shards(state)
    {:noreply, state, sync_interval}
  end

  def handle_call(:sync, _, state) do
    :ok = sync_shards(state)
    {:reply, :ok, state}
  end

  defp sync_shards(%State{stream_name: stream_name, adapter: adapter, repo: repo}) do
    {:ok, shards} = adapter.get_shards(stream_name)

    # gen currently listed leases
    {:ok, leases} = LeaseRepo.list(repo)

    # shards with no leases
    Enum.each(shards, fn(shard) ->
      case Enum.find(leases, &(&1.shard_id == shard.shard_id)) do
        nil ->
          LeaseRepo.create(repo, shard)

        lease ->
          lease
      end
    end)

    # delete leases with no shards
    Enum.each(leases, fn(lease) ->
      case Enum.find(shards, &(&1.shard_id == lease.shard_id)) do
        nil ->
          LeaseRepo.delete(repo, lease)

        shard ->
          shard
      end
    end)

    :ok
  end
end
