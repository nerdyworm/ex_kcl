defmodule LeaseTakerTest do
  use ExUnit.Case

  alias ExKcl. {
    LeaseRepo,
    LeaseTaker,
    Shard,
  }

  setup do
    opts = [
      repo: :testing,
      lease_table_name: "leases_test",
      taker: :test_taker,
      max_leases_per_worker: 3,
      lease_stale_after: 5000
    ]
    {:ok, _} = LeaseRepo.start_link(opts)
    :ok = LeaseRepo.clear(opts[:repo])

    {:ok, _} = LeaseTaker.start_link("some worker uuid", opts)
    {:ok, opts}
  end

  test "no leases to take", %{taker: taker} do
    assert {:ok, []} = LeaseTaker.take(taker)
  end

  test "compute_target" do
    assert 1 == LeaseTaker.compute_target(0, 1, 3)
    assert 1 == LeaseTaker.compute_target(1, 1, 3)
    assert 2 == LeaseTaker.compute_target(2, 1, 3)
    assert 3 == LeaseTaker.compute_target(5, 2, 3)
  end

  test "take a single unowned lease", %{repo: repo, taker: taker} do
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0001"})

    assert {:ok, [lease]} = LeaseTaker.take(taker)
    assert lease.shard_id == "shard-0001"
    assert lease.owner == "some worker uuid"
  end

  test "take should return nothing if nothing to take", %{repo: repo, taker: taker} do
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0001"})
    assert {:ok, [_]} = LeaseTaker.take(taker)
    assert {:ok, []} = LeaseTaker.take(taker)
  end

  test "take serveral leaese", %{repo: repo, taker: taker} do
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0001"})
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0002"})
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0003"})
    assert :ok = LeaseRepo.create(repo, %Shard{shard_id: "shard-0004"})

    assert {:ok, taken} = LeaseTaker.take(taker)
    assert length(taken) == 3

    # TODO - figure out how to simulate a lease going stale
    assert {:ok, taken} = LeaseTaker.take(taker)
    assert length(taken) == 0
  end
end
