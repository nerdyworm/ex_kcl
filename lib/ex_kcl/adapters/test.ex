defmodule ExKcl.Adapters.Test do
  alias ExKcl.{
    Shard,
  }

  def get_shards(_) do
    {:ok, [%Shard{shard_id: "shard-0001"}]}
  end

  def describe_stream(_) do
    :todo
  end

  def get_shard_iterator(_, _) do
    :todo
  end

  def get_records(_) do
    :todo
  end

  def checkpoint_for_record(%{"SequenceNumber" => checkpoint}), do: checkpoint
end

