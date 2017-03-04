defmodule ExKcl.StreamConsumer do
  defmacro __using__(_) do
    quote bind_quoted: [] do
      use GenStage

      def start_link(stream) do
        GenStage.start_link(__MODULE__, stream)
      end

      def init(producer) do
        {:consumer, :ok, subscribe_to: [{producer, max_demand: 1}]}
      end
    end
  end
end
