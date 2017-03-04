# ExKcl

**WARNING: A work in progress**

* Dynamodb Streams
* Kinesis Streams

## Installation

```elixir
def deps do
  [{:ex_kcl, github: "nerdyworm/ex_kcl"}]
end
```

## Usage

```elixir
defmodule Stream do
  use ExKcl.Stream, otp_app: :example
end
```

```elixir
defmodule Consumer do
  use ExKcl.StreamConsumer

  def handle_events(events, _from, state) do
    IO.puts "consumer: #{length(events)}"
    {:noreply, [], state}
  end
end
```

```elixir
config :example, Stream, [
  stream_name: "stream_name",
  adapter: ExKcl.Adapters.Kinesis,
  handlers: [Consumer]
]
```
