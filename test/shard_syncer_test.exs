defmodule ShardSyncerTest do
  use ExUnit.Case

  alias ExKcl. {
    LeaseRepo,
    Shard,
    ShardSyncer,
  }

  setup do
    opts = [ repo: :testing, lease_table_name: "leases_test" ]
    {:ok, _} = LeaseRepo.start_link(opts)
    :ok = LeaseRepo.clear(opts[:repo])

    {:ok, opts}
  end

  test "sync a single shard", %{repo: repo} do
    {:ok, pid} = ShardSyncer.start_link([repo: repo, adapter: ExKcl.Adapters.Test, shard_syncer_sync_interval: 1000, shard_syncer_start_timeout: 1000])
    assert :ok = ShardSyncer.sync(pid)

    {:ok, leases} = LeaseRepo.list(repo)
    assert length(leases) == 1
  end

  test "it deletes lease with no shards", %{repo: repo} do
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0001"})
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0002"})
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0003"})
    {:ok, leases} = LeaseRepo.list(repo)
    assert length(leases) == 3

    {:ok, pid} = ShardSyncer.start_link([repo: repo, adapter: ExKcl.Adapters.Test, shard_syncer_sync_interval: 1000, shard_syncer_start_timeout: 1000])
    assert :ok = ShardSyncer.sync(pid)

    {:ok, leases} = LeaseRepo.list(repo)
    assert length(leases) == 1
  end
end
