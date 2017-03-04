defmodule ExKcl.ShardWorkerSupervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: opts[:worker_sup])
  end

  def start_worker(supervisor, lease) do
    Supervisor.start_child(supervisor, [lease])
  end

  def stop_worker(supervisor, registry, lease) do
    case Registry.lookup(registry, lease.shard_id) do
      [{pid, _}] ->
        :ok = Supervisor.terminate_child(supervisor, pid)

      [] ->
        :ok
    end
  end

  def init(opts) do
    children = [
      supervisor(ExKcl.ShardSupervisor, [opts], restart: :temporary)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end
end

