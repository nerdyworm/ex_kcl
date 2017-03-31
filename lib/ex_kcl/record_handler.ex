defmodule ExKcl.RecordHandler do
  require Logger

  def handle_records(state, handler, records) do
    timeout = 10_000
    Enum.each(records, fn(record) ->
      task = Task.Supervisor.async_nolink(
        state.task_supervisor,
        handler,
        :handle_record,
        [record])

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

    IO.puts "Proccessed #{length(records)} records"
    :ok
  end
end
