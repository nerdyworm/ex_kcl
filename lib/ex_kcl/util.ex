defmodule ExKcl.Util do
  require Logger

  alias ExAws.{
    Kinesis,
    Dynamo,
  }

  def setup_kinesis(stream_name, shards \\ 2) do
    {:ok, response} =  Kinesis.list_streams |> ExAws.request
    %{"StreamNames" => names} = response

    if not Enum.member?(names, stream_name) do
      Logger.info "Creating stream #{stream_name} with #{shards} shards"
      Kinesis.create_stream(stream_name, shards) |> ExAws.request
      :timer.sleep(5000)
      setup_kinesis(stream_name, shards)
    end
  end

  def setup_lease_table(table_name, capacity \\ 5) do
    results =
      Dynamo.describe_table(table_name)
      |> ExAws.request

    case results do
      {:ok, %{"Table" => %{"TableStatus" => "ACTIVE"}}} ->
        :ok

      {:ok, %{"Table" => %{"TableStatus" => "CREATING"}}} ->
        :timer.sleep(5000)
        setup_lease_table(table_name, capacity)

      {:error, _} ->
        Logger.info "Creating leases table with name=#{table_name}"
        Dynamo.create_table(table_name, [shard_id: :hash], [shard_id: :string], capacity, capacity) |> ExAws.request!
        setup_lease_table(table_name, capacity)
    end
  end
end
