defmodule TdCore.Search.PhaseProfilerTest do
  use ExUnit.Case, async: false

  import TdCore.Search.PhaseProfiler

  defmodule Collector do
    @moduledoc false
    @agent __MODULE__

    def start do
      case Agent.start_link(fn -> [] end, name: @agent) do
        {:ok, pid} -> pid
        {:error, {:already_started, pid}} -> Agent.update(@agent, fn _ -> [] end) && pid
      end
    end

    def time(phase, fun) do
      Agent.update(@agent, &[phase | &1])
      fun.()
    end

    def phases, do: Agent.get(@agent, &Enum.reverse/1)
  end

  setup do
    on_exit(fn -> Application.delete_env(:td_core, :search_phase_profiler) end)
    :ok
  end

  describe "phase_time/2 without profiler configured" do
    test "runs the function directly and returns its result" do
      Application.delete_env(:td_core, :search_phase_profiler)

      assert phase_time(:encode, fn -> :encoded end) == :encoded
    end

    test "ignores unrelated env values" do
      Application.put_env(:td_core, :search_phase_profiler, :not_a_tuple)

      assert phase_time(:bulk_es, fn -> 7 end) == 7
    end
  end

  describe "phase_time/2 with a {module, function} profiler" do
    setup do
      Collector.start()
      Application.put_env(:td_core, :search_phase_profiler, {Collector, :time})
      :ok
    end

    test "delegates to the configured callback and returns the function result" do
      assert phase_time(:encode, fn -> :body end) == :body
      assert phase_time(:bulk_es, fn -> {:ok, :posted} end) == {:ok, :posted}
    end

    test "invokes the callback once per phase_time call preserving order" do
      phase_time(:encode, fn -> :a end)
      phase_time(:bulk_es, fn -> :b end)
      phase_time(:encode, fn -> :c end)

      assert Collector.phases() == [:encode, :bulk_es, :encode]
    end

    test "propagates the phase argument to the callback" do
      assert phase_time(:custom_phase, fn -> :ok end) == :ok
      assert :custom_phase in Collector.phases()
    end
  end
end
