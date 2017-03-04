defmodule ExKcl.Stream do
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      import ExKcl.Stream

      {:ok, config} = parse_config(__MODULE__,opts)
      @otp_app config[:otp_app]
      @config config

      def start_link do
        ExKcl.start_link(__MODULE__, @config)
      end

      def add_handler(handler) do
        config = Application.get_env(@otp_app, __MODULE__, [handlers: []])

        handlers = config[:handlers]
        handlers = [handler|handlers] |> Enum.uniq

        config = [handlers: handlers]
        Application.put_env(@otp_app, __MODULE__, config)
        :ok
      end

      def handlers do
        config = Application.get_env(@otp_app, __MODULE__)
        config[:handlers]
      end
    end
  end

  def parse_config(stream_module, options) do
    otp_app = Keyword.fetch!(options, :otp_app)
    config = Application.get_env(otp_app, stream_module, [])

    defaults = [
      shard_syncer_start_timeout: 1000,
      shard_syncer_sync_interval: 5000,
      max_leases_per_worker:      5,
      lease_stale_after:          10_000,
      coordinator_sync_interval:  5000,
    ]

    config = Keyword.merge(defaults, config)
    config = Keyword.merge(config, options)
    adapter = options[:adapter] || config[:adapter]

    unless adapter do
      raise ArgumentError, "missing :adapter configuration in " <>
      "config #{inspect otp_app}, #{inspect adapter}"
    end

    {:ok, config}
  end
end
