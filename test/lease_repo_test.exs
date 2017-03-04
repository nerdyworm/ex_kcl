defmodule LeaseRepoTest do
  use ExUnit.Case

  alias ExKcl. {
    Lease,
    LeaseRepo,
    Shard,
  }

  setup do
    opts = [repo: :testing, lease_table_name: "leases_test"]
    {:ok, _} = LeaseRepo.start_link(opts)
    LeaseRepo.clear(opts[:repo])
    {:ok, opts}
  end

  test "list no leases", %{repo: repo} do
    assert {:ok, []} = LeaseRepo.list(repo)
  end

  test "creates one lease per shard", %{repo: repo} do
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0001"})
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0001"})
    assert {:ok, [%Lease{} = lease]} = LeaseRepo.list(repo)
    assert lease.shard_id == "shard-0001"
    assert lease.checkpoint == "TRIM_HORIZON"
  end

  test "get lease by id", %{repo: repo} do
    :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0001"})
    {:ok, lease} = LeaseRepo.get(repo, "shard-0001")
    assert lease.shard_id == "shard-0001"
    assert lease.checkpoint == "TRIM_HORIZON"
    assert lease.counter == 0
    assert lease.owner == nil
  end

  test "renew lease", %{repo: repo} do
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0001"})
    {:ok, lease} = LeaseRepo.get(repo, "shard-0001")

    assert {:ok, lease} = LeaseRepo.renew(repo, lease)
    assert lease.counter == 1
  end

  test "take unowned lease", %{repo: repo} do
    :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0001"})
    {:ok, lease} = LeaseRepo.get(repo, "shard-0001")
    {:ok, lease} = LeaseRepo.take(repo, lease, "owner")
    assert lease.owner == "owner"
    assert lease.counter == 1
  end

  test "take owned lease", %{repo: repo} do
    :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0001"})
    {:ok, lease} = LeaseRepo.get(repo, "shard-0001")
    {:ok, lease} = LeaseRepo.take(repo, lease, "owner")
    {:ok, lease} = LeaseRepo.take(repo, lease, "new_owner")
    assert lease.owner == "new_owner"
    assert lease.counter == 2
  end
end
