defmodule ExKcl.RecordHandler do
  require Logger

  def handle_records(state, handler, records) do
    timeout = 10_000
    task = Task.Supervisor.async_nolink(
      state.task_supervisor,
      handler,
      :handle_records,
      [records])

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, :ok} ->
        :ok

      {:exit, reason} ->
        message = Exception.format(:exit, reason, System.stacktrace)
        :ok = handler.nack_record(records, message)

      nil ->
        Logger.warn "[ex_kcl] Failed to get a result in #{timeout}ms"
        :ok

    end
  end
end
