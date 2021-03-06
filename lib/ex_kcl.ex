defmodule ExKcl do
  import Supervisor.Spec

  def start_link(name, opts) do
    worker_id = Keyword.get(opts, :worker_id, UUID.uuid4)

    defaults = [
      idle_ms: 2000
    ]

    opts = [
      handler:      name,
      worker_id:    worker_id,
      repo:         Module.concat(name, LeaseRepo),
      taker:        Module.concat(name, LeaseTaker),
      coordinator:  Module.concat(name, LeaseCoordinator),
      worker_sup:   Module.concat(name, ShardWorkerSupervisor),
      producer:     Module.concat(name, Producer),
      task_supervisor:     Module.concat(name, TaskSupervisor),
      supervisor_registry: Module.concat(name, SupervisorRegistry),
      producer_registry:   Module.concat(name, ProducerRegistry),
    ] ++ opts

    opts = Keyword.merge(defaults, opts)

    children = [
      worker(ExKcl.LeaseRepo,        [opts]),
      worker(ExKcl.LeaseTaker,       [worker_id, opts]),
      worker(ExKcl.LeaseCoordinator, [worker_id, opts]),
      worker(ExKcl.ShardSyncer,      [opts]),
      supervisor(Registry, [:unique, opts[:supervisor_registry]], id: 1),
      supervisor(Registry, [:unique, opts[:producer_registry]],   id: 2),
      supervisor(ExKcl.ShardWorkerSupervisor, [opts]),
      supervisor(Task.Supervisor, [[name: opts[:task_supervisor]]])
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
