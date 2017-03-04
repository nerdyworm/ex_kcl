defmodule ExKcl.Adapters.Dynamodb do
  alias ExAws.DynamoStreams
  alias ExKcl.{
    Lease,
    Shard,
  }

  def get_shards(stream_name) do
    %{"StreamDescription" => %{"Shards" => shards}} = describe_stream(stream_name)
    {:ok, Enum.map(shards, &Shard.decode/1)}
  end

  def describe_stream(stream_name) do
    DynamoStreams.describe_stream(stream_name)
    |> ExAws.request!
  end

  def get_shard_iterator(stream_name, %Lease{shard_id: shard_id, checkpoint: checkpoint}) do
    if checkpoint == "TRIM_HORIZON" do
      DynamoStreams.get_shard_iterator(stream_name, shard_id, :trim_horizon)
    else
      DynamoStreams.get_shard_iterator(stream_name, shard_id, :after_sequence_number, [sequence_number: checkpoint])
    end
    |> ExAws.request!
  end

  def get_records(iterator) do
    DynamoStreams.get_records(iterator)
    |> ExAws.request!
  end

  def checkpoint_for_record(%{"dynamodb" => %{"SequenceNumber" => checkpoint}}) do
    checkpoint
  end
end
