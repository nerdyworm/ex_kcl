defmodule ExKcl.IntegrationTest do
  use ExUnit.Case

  alias ExKcl.{
    Stream,
    Adapters.Dynamodb
  }

  defmodule Consumer do
    def handle_record(record, state) do
      IO.puts "handle_record: #{inspect record}"
      IO.puts "\n"
      :ok
    end
  end

  def config do
    [adapter: Dynamodb,
     stream_name: "arn:aws:dynamodb:us-east-1:907015576586:table/SubZero.Events/stream/2017-01-22T01:45:50.219",
     lease_table_name: "leases",
     shard_syncer_start_timeout: 1000,
     shard_syncer_sync_interval: 1000,
     max_leases_per_worker:      5,
     lease_stale_after:          5000,
     coordinator_sync_interval:  5000,
   ]
  end

  def handle_record(record, state) do
    tasks =
      handlers()
      |> Enum.map(fn(handler) ->
         Task.Supervisor.async_nolink(state.task_supervisor, handler, :handle_record, [record, state])
      end)
      |> Task.yield_many(10_000)


    results = Enum.map(tasks, fn {task, res} ->
      res || Task.shutdown(task, :brutal_kill)
    end)

    for result <- results do
      case result do
        {:ok, :ok} ->
          :ok

        {:exit, reason} ->
          message = Exception.format(:exit, reason, System.stacktrace)
          :ok = state.handler.nack_record(record, message)

        huh ->
          IO.puts "#{inspect huh}"
          :ok
      end
    end
    :ok
  end

  def nack_record(record, message) do
    IO.puts "nacking: #{inspect record}"
    :ok
  end

  def handlers() do
    [Consumer, Consumer, Consumer]
  end

  @tag timeout: 60_000 * 60
  test "can actually use this thing" do
    config = Keyword.merge(Stream.default_config, config())
    {:ok, pid} = ExKcl.start_link(__MODULE__, config)
    :timer.sleep(59_000 * 60)
  end
end
