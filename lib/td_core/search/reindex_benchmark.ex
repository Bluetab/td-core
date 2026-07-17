defmodule TdCore.Search.ReindexBenchmark do
  @moduledoc """
  Generic harness for benchmarking `TdCore.Search.Indexer.reindex/2`.

  Services provide index atom, target (`:all` or ids), `doc_count`, and an
  optional `env_banner_fn` for service-specific knobs. Measurement (VM,
  scheduler, GC, optional profile sampler) and summary formatting live here.

  ## Options

  * `:doc_count` — document count used for throughput (required)
  * `:repeat` — number of runs (default `1`)
  * `:profile` — enable memory/run_queue sampling (default `false`)
  * `:profile_interval_ms` — sampler interval (default `500`)
  * `:env_banner_fn` — zero-arity callback returning a keyword list of
    extra banner lines (`[{label, value}, ...]`)
  * `:info_fn` — one-arity callback for output (default `IO.puts/1`)
  * `:reindex_fn` — two-arity `(index, target) -> result` (default
    `Indexer.reindex/2`, overridable in tests)
  """

  alias TdCore.Search.Cluster
  alias TdCore.Search.Indexer

  @memory_keys [:total, :processes, :system, :atom, :binary, :ets, :code]
  @default_profile_interval_ms 500

  @doc """
  Runs one or more timed `reindex` calls and prints banner + metrics.
  Returns the list of per-run metric maps.
  """
  def run(index, target, opts \\ []) when is_atom(index) do
    ensure_runtime_tools!()

    doc_count = Keyword.fetch!(opts, :doc_count)
    repeat = Keyword.get(opts, :repeat, 1)
    profile? = Keyword.get(opts, :profile, false)
    profile_interval_ms = Keyword.get(opts, :profile_interval_ms, @default_profile_interval_ms)
    info = Keyword.get(opts, :info_fn, &IO.puts/1)
    env_banner_fn = Keyword.get(opts, :env_banner_fn)
    reindex_fn = Keyword.get(opts, :reindex_fn, &Indexer.reindex/2)

    log_environment(index, target, doc_count, repeat, profile?, env_banner_fn, info)

    results =
      Enum.map(1..repeat, fn run ->
        info.("\n=== Run #{run}/#{repeat} ===")
        benchmark_run(index, target, doc_count, profile?, profile_interval_ms, reindex_fn, info)
      end)

    if repeat > 1, do: log_repeat_summary(results, info)

    results
  end

  defp ensure_runtime_tools! do
    {:ok, _} = Application.ensure_all_started(:runtime_tools)
  end

  defp benchmark_run(index, target, doc_count, profile?, profile_interval_ms, reindex_fn, info) do
    :erlang.system_flag(:scheduler_wall_time, true)

    before = vm_snapshot()
    scheduler_before = scheduler_wall_sample()
    gc_before = :erlang.statistics(:garbage_collection)

    sampler =
      if profile? do
        start_sampler(profile_interval_ms)
      end

    start_ms = System.monotonic_time(:millisecond)

    {micros, result} =
      :timer.tc(fn ->
        reindex_fn.(index, target)
      end)

    elapsed_ms = System.monotonic_time(:millisecond) - start_ms

    samples =
      if sampler do
        stop_sampler(sampler)
      else
        []
      end

    vm_after = vm_snapshot()
    scheduler_after = scheduler_wall_sample()
    gc_after = :erlang.statistics(:garbage_collection)

    metrics = %{
      micros: micros,
      elapsed_ms: elapsed_ms,
      doc_count: doc_count,
      result: result,
      before: before,
      vm_after: vm_after,
      memory_delta: memory_delta(before.memory, vm_after.memory),
      scheduler: scheduler_utilization(scheduler_before, scheduler_after),
      gc: gc_delta(gc_before, gc_after),
      samples: samples
    }

    log_run_metrics(metrics, profile_interval_ms, info)
    metrics
  end

  defp start_sampler(interval_ms) do
    parent = self()

    spawn(fn ->
      sampler_loop(parent, interval_ms, [])
    end)
  end

  defp sampler_loop(parent, interval_ms, acc) do
    receive do
      :stop ->
        send(parent, {:samples, Enum.reverse(acc)})
    after
      interval_ms ->
        sample = %{
          at_ms: System.monotonic_time(:millisecond),
          memory_total: :erlang.memory(:total),
          run_queue: run_queue_lengths()
        }

        sampler_loop(parent, interval_ms, [sample | acc])
    end
  end

  defp stop_sampler(pid) do
    send(pid, :stop)

    receive do
      {:samples, samples} -> samples
    after
      5_000 -> []
    end
  end

  defp vm_snapshot do
    %{
      memory: memory_map(),
      process_count: :erlang.system_info(:process_count),
      port_count: :erlang.system_info(:port_count),
      atom_count: :erlang.system_info(:atom_count),
      run_queue: run_queue_lengths()
    }
  end

  defp memory_map do
    Map.new(@memory_keys, fn key -> {key, :erlang.memory(key)} end)
  end

  defp memory_delta(before, vm_after) do
    Map.new(@memory_keys, fn key ->
      {key, Map.get(vm_after, key, 0) - Map.get(before, key, 0)}
    end)
  end

  defp run_queue_lengths do
    case :erlang.statistics(:run_queue) do
      {total, cpu, io} -> %{total: total, cpu: cpu, io: io}
      total when is_integer(total) -> %{total: total, cpu: total, io: 0}
    end
  end

  defp scheduler_wall_sample do
    :scheduler.get_sample()
  end

  defp scheduler_utilization(nil, _), do: empty_scheduler_metrics()
  defp scheduler_utilization(_, nil), do: empty_scheduler_metrics()

  defp scheduler_utilization(before, wall_after) do
    case :scheduler.utilization(before, wall_after) do
      results when is_list(results) ->
        %{
          percent: scheduler_util_value(results, :total),
          weighted_percent: scheduler_util_value(results, :weighted)
        }

      _ ->
        empty_scheduler_metrics()
    end
  end

  defp empty_scheduler_metrics do
    %{percent: nil, weighted_percent: nil}
  end

  defp scheduler_util_value(results, key) do
    case Enum.find(results, fn {tag, _, _} -> tag == key end) do
      {_, util, _} when is_float(util) -> Float.round(util * 100, 2)
      _ -> nil
    end
  end

  defp gc_delta(before, gc_after) do
    {count_b, words_b} = gc_stats_pair(before)
    {count_a, words_a} = gc_stats_pair(gc_after)

    %{
      collections: count_a - count_b,
      words_reclaimed: words_a - words_b
    }
  end

  defp gc_stats_pair({count, words}), do: {count, words}
  defp gc_stats_pair({count, words, _}), do: {count, words}

  defp log_run_metrics(%{micros: micros, doc_count: doc_count} = metrics, profile_interval_ms, info) do
    throughput = throughput_per_sec(doc_count, micros)

    info.("""
    Timing:
      wall_clock           #{format_micros(micros)} (#{micros} µs / #{div(micros, 1_000)} ms)
      throughput           #{throughput} docs/s
      reindex result       #{inspect(metrics.result)}

    Memory (BEAM, before → after, Δ):
      total                #{format_bytes(metrics.before.memory.total)} → #{format_bytes(metrics.vm_after.memory.total)} (#{format_delta(metrics.memory_delta.total)})
      processes            #{format_bytes(metrics.before.memory.processes)} → #{format_bytes(metrics.vm_after.memory.processes)} (#{format_delta(metrics.memory_delta.processes)})
      binary               #{format_bytes(metrics.before.memory.binary)} → #{format_bytes(metrics.vm_after.memory.binary)} (#{format_delta(metrics.memory_delta.binary)})
      ets                  #{format_bytes(metrics.before.memory.ets)} → #{format_bytes(metrics.vm_after.memory.ets)} (#{format_delta(metrics.memory_delta.ets)})
      system               #{format_bytes(metrics.before.memory.system)} → #{format_bytes(metrics.vm_after.memory.system)} (#{format_delta(metrics.memory_delta.system)})

    CPU (BEAM schedulers):
      utilization          #{format_percent(metrics.scheduler.percent)} (total, normal + dirty-cpu)
      weighted utilization #{format_percent(metrics.scheduler.weighted_percent)} (vs available CPU time)

    Scheduler run queues (after):
      total                #{metrics.vm_after.run_queue.total}
      cpu                  #{metrics.vm_after.run_queue.cpu}
      io                   #{metrics.vm_after.run_queue.io}

    GC:
      collections          #{metrics.gc.collections}
      words reclaimed      #{metrics.gc.words_reclaimed}

    VM (after):
      processes            #{metrics.vm_after.process_count}
      ports                #{format_count(metrics.vm_after.port_count)}
      atoms                #{format_count(metrics.vm_after.atom_count)}
    """)

    log_profile_samples(metrics.samples, profile_interval_ms, info)
  end

  defp log_profile_samples([], _profile_interval_ms, _info), do: :ok

  defp log_profile_samples(samples, profile_interval_ms, info) do
    memory_values = Enum.map(samples, & &1.memory_total)
    rq_total = Enum.map(samples, & &1.run_queue.total)

    info.("""
    Profile samples (#{length(samples)} every #{profile_interval_ms} ms):
      memory total         min #{format_bytes(Enum.min(memory_values))}  max #{format_bytes(Enum.max(memory_values))}  avg #{format_bytes(avg(memory_values))}
      run_queue total      min #{Enum.min(rq_total)}  max #{Enum.max(rq_total)}  avg #{Float.round(avg(rq_total), 1)}
    """)
  end

  defp log_repeat_summary(results, info) do
    micros_list = Enum.map(results, & &1.micros)
    percent_list = Enum.map(results, & &1.scheduler.percent) |> Enum.reject(&is_nil/1)

    scheduler_summary =
      if percent_list == [] do
        "      scheduler util       n/a"
      else
        "      scheduler util       min #{format_percent(Enum.min(percent_list))}  max #{format_percent(Enum.max(percent_list))}  avg #{format_percent(Float.round(avg(percent_list), 2))}"
      end

    info.("""

    === Repeat summary (#{length(results)} runs) ===
      wall_clock           min #{format_micros(Enum.min(micros_list))}  max #{format_micros(Enum.max(micros_list))}  avg #{format_micros(trunc(avg(micros_list)))}
    #{scheduler_summary}
    """)
  end

  defp log_environment(index, target, doc_count, repeat, profile?, env_banner_fn, info) do
    concurrency =
      :td_core
      |> Application.get_env(Cluster, [])
      |> Keyword.get(:reindex_concurrency, System.schedulers_online())

    bulk_page_size =
      :td_core
      |> Application.get_env(Cluster, [])
      |> get_in([:indexes, index, :bulk_page_size])

    target_label =
      case target do
        :all -> ":all"
        list when is_list(list) -> "#{length(list)} ids"
        other -> inspect(other)
      end

    baseline = vm_snapshot()

    extra_lines =
      case env_banner_fn do
        fun when is_function(fun, 0) ->
          fun.()
          |> List.wrap()
          |> Enum.map_join("\n", fn
            {label, value} ->
              label = to_string(label)
              pad = String.duplicate(" ", max(1, 21 - String.length(label)))
              "      #{label}#{pad}#{value}"

            line when is_binary(line) ->
              line
          end)
          |> case do
            "" -> ""
            lines -> "\n" <> lines
          end

        _ ->
          ""
      end

    info.("""
    Benchmark configuration:
      target               #{target_label}
      documents            #{doc_count}
      runs                 #{repeat}
      profile sampling     #{profile?}
      reindex_concurrency  #{concurrency}
      bulk_page_size       #{inspect(bulk_page_size)}#{extra_lines}
      schedulers_online    #{System.schedulers_online()}
      otp_release          #{System.otp_release()}
      elixir               #{System.version()}
      baseline memory      #{format_bytes(baseline.memory.total)} (processes=#{format_bytes(baseline.memory.processes)}, ets=#{format_bytes(baseline.memory.ets)})
    """)
  end

  defp throughput_per_sec(_doc_count, micros) when micros <= 0, do: "n/a"

  defp throughput_per_sec(doc_count, micros) do
    secs = micros / 1_000_000
    Float.round(doc_count / secs, 2)
  end

  defp format_micros(micros) when micros < 1_000_000 do
    "#{Float.round(micros / 1_000, 2)} ms"
  end

  defp format_micros(micros) do
    "#{Float.round(micros / 1_000_000, 2)} s"
  end

  defp format_bytes(bytes) when bytes < 1024, do: "#{bytes} B"
  defp format_bytes(bytes) when bytes < 1024 * 1024, do: "#{Float.round(bytes / 1024, 1)} KiB"
  defp format_bytes(bytes), do: "#{Float.round(bytes / (1024 * 1024), 2)} MiB"

  defp format_delta(bytes) when bytes >= 0, do: "+#{format_bytes(bytes)}"
  defp format_delta(bytes), do: format_bytes(bytes)

  defp format_percent(nil), do: "n/a"
  defp format_percent(percent), do: "#{percent}%"

  defp format_count(value) when is_integer(value), do: Integer.to_string(value)
  defp format_count(value), do: inspect(value)

  defp avg([]), do: 0

  defp avg(list) do
    Enum.sum(list) / length(list)
  end
end
