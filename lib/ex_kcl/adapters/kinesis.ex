defmodule ExKcl.Adapters.Kinesis do
  alias ExAws.Kinesis
  alias ExKcl.{
    Lease,
    Shard,
  }

  def get_shards(stream_name) do
    %{"StreamDescription" => %{"Shards" => shards}} = describe_stream(stream_name)
    {:ok, Enum.map(shards, &Shard.decode/1)}
  end

  def describe_stream(stream_name) do
    Kinesis.describe_stream(stream_name) |> ExAws.request!
  end

  def get_shard_iterator(stream_name, %Lease{shard_id: shard_id, checkpoint: checkpoint}) do
    if checkpoint == "TRIM_HORIZON" do
      Kinesis.get_shard_iterator(stream_name, shard_id, :trim_horizon)
    else
      Kinesis.get_shard_iterator(stream_name, shard_id, :after_sequence_number, [starting_sequence_number: checkpoint])
    end
    |> ExAws.request!
  end

  def get_records(iterator) do
    Kinesis.get_records(iterator) |> ExAws.request!
  end

  def checkpoint_for_record(%{"SequenceNumber" => checkpoint}), do: checkpoint
end

