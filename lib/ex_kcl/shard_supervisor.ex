defmodule ExKcl.ShardSupervisor do
  use Supervisor

  def start_link(opts, lease) do
    Supervisor.start_link(__MODULE__, {opts, lease}, name: via_tuple(opts, lease))
  end

  def init({opts, lease}) do
    children = [
      worker(ExKcl.ShardReader, [opts, lease])
    ]

    supervise(children, strategy: :one_for_one)
  end

  def via_tuple(opts, %ExKcl.Lease{shard_id: shard_id}) do
    {:via, Registry, {opts[:supervisor_registry], shard_id}}
  end
end

