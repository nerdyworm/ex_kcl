defmodule ExKcl.ShardSupervisor do
  use Supervisor

  def start_link(opts, lease) do
    Supervisor.start_link(__MODULE__, {opts, lease}, name: via_tuple(opts, lease))
  end

  def init({opts, lease}) do
    consumers = opts[:handler].handlers

    tuple = ExKcl.ShardProducer.via_tuple(opts, lease)

    children = [
      worker(ExKcl.ShardProducer, [opts, lease]),
    ] ++ Enum.map(consumers, fn(module) ->
      worker(module, [tuple])
    end)

    supervise(children, strategy: :one_for_one)
  end

  def via_tuple(opts, %ExKcl.Lease{shard_id: shard_id}) do
    {:via, Registry, {opts[:supervisor_registry], shard_id}}
  end
end

