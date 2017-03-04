defmodule ExKcl.LeaseRepo do
  @moduledoc """
  Defines the crud operations for leases using DynamoDB
  """
  use GenServer

  alias ExKcl.{
    Lease,
  }

  alias ExAws.Dynamo

  defmodule State do
    defstruct [
      table: nil
    ]
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:repo])
  end

  def init(opts) do
    {:ok, %State{table: opts[:lease_table_name]}}
  end

  def clear(repo) do
    GenServer.call(repo, :clear)
  end

  def list(repo) do
    GenServer.call(repo, :list)
  end

  def create(repo, shard) do
    GenServer.call(repo, {:create, shard})
  end

  def renew(repo, lease) do
    GenServer.call(repo, {:renew, lease})
  end

  def get(repo, shard_id) do
    GenServer.call(repo, {:get, shard_id})
  end

  def take(repo, lease, owner) do
    GenServer.call(repo, {:take, lease, owner})
  end

  def steal(repo, lease, owner) do
    GenServer.call(repo, {:steal, lease, owner})
  end

  def checkpoint(repo, lease, checkpoint) do
    GenServer.call(repo, {:checkpoint, lease, checkpoint})
  end

  def handle_call({:renew, %Lease{counter: counter} = lease}, _, %State{table: table} = state) do
    opts = [
      condition_expression: "#counter = :last",
      expression_attribute_names: %{"#counter" => "counter"},
      expression_attribute_values: [last: counter]]

    lease = %Lease{lease | counter: counter + 1}

    {:ok, _} = Dynamo.put_item(table, lease, opts) |> ExAws.request
    {:reply, {:ok, lease}, state}
  end

  def handle_call({:take, %Lease{counter: counter} = lease, owner}, _, %State{table: table} = state) do
    opts = [
      condition_expression: "#counter = :last",
      expression_attribute_names: %{"#counter" => "counter"},
      expression_attribute_values: [last: counter]]

    lease = %Lease{lease | owner: owner, counter: counter + 1}

    response = ExAws.Dynamo.put_item(table, lease, opts) |> ExAws.request
    case response do
      {:error, {"ConditionalCheckFailedException", "The conditional request failed"}} ->
        {:reply, :lost, state}

      {:ok, _} ->
        {:reply, {:ok, lease}, state}
    end
  end

  def handle_call({:steal, %Lease{counter: counter} = lease, owner}, _, %State{table: table} = state) do
    lease = %Lease{lease | owner: owner, counter: counter + 1}

    response = ExAws.Dynamo.put_item(table, lease) |> ExAws.request
    case response do
      {:ok, _} ->
        {:reply, {:ok, lease}, state}
    end
  end

  def handle_call({:get, shard_id}, _, %State{table: table} = state) do
    lease =
      Dynamo.get_item(table, %{shard_id: shard_id})
      |> ExAws.request!
      |> Dynamo.decode_item(as: Lease)

    {:reply, {:ok, lease}, state}
  end

  def handle_call(:clear, _, %State{table: table} = state) do
    leases = scan(table)

    Enum.each(leases, fn(lease) ->
      Dynamo.delete_item(table, %{shard_id: lease.shard_id})
      |> ExAws.request!
    end)

    {:reply, :ok, state}
  end

  def handle_call(:list, _, %State{table: table} = state) do
    {:reply, {:ok, scan(table)}, state}
  end

  def handle_call({:create, shard}, _, %State{table: table} = state) do
    lease = %Lease{
      checkpoint: "TRIM_HORIZON",
      shard_id: shard.shard_id,
      parent_id: shard.parent_shard_id,
      counter: 0,
    }

    opts = [condition_expression: "attribute_not_exists(shard_id)"]

    result =
      ExAws.Dynamo.put_item(table, lease, opts)
      |> ExAws.request

    case result do
      {:error, {"ConditionalCheckFailedException", "The conditional request failed"}} ->
        {:reply, :ok, state}

      {:ok, _} ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:checkpoint, %Lease{counter: counter} = lease, checkpoint}, _, %State{table: table} = state) do
    opts = [
      condition_expression: "#counter = :last",
      expression_attribute_names: %{"#counter" => "counter"},
      expression_attribute_values: [last: counter]]

    lease = %Lease{lease | counter: counter + 1, checkpoint: checkpoint}

    result =
      ExAws.Dynamo.put_item(table, lease, opts)
      |> ExAws.request

    case result do
      {:error, {"ConditionalCheckFailedException", "The conditional request failed"}} ->
        {:reply, {:error, :stolen}, state}

      {:ok, _} ->
        {:reply, {:ok, lease}, state}
    end
  end

  defp scan(table) do
    Dynamo.scan(table)
    |> ExAws.request!
    |> fn(%{"Items" => items}) -> items end.()
    |> Enum.map(fn(l) -> Dynamo.Decoder.decode(l, as: Lease) end)
  end
end
