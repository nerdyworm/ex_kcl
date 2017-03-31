defmodule ExKcl.RecordHandler do
  require Logger

  def handle_records(state, handler, records) do
    timeout = 10_000
    Enum.each(records, fn(record) ->
      task = Task.Supervisor.async_nolink(state.task_supervisor, __MODULE__, :handle_record, [record, handler, state])

      case Task.yield(task, timeout) || Task.shutdown(task) do
        {:ok, :ok} ->
          :ok

        {:exit, reason} ->
          message = Exception.format(:exit, reason, System.stacktrace)
          :ok = handler.nack_record(record, message)

        nil ->
          Logger.warn "Failed to get a result in #{timeout}ms"
          :ok
      end
    end)
  end

  def handle_record(record, handler, state) do
    IO.puts "handling: #{inspect record}"
    :ok
  end

end
