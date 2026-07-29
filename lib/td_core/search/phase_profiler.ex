defmodule TdCore.Search.PhaseProfiler do
  @moduledoc """
  Optional phase timing hook for the search reindex pipeline.

  When `:td_core, :search_phase_profiler` is configured as `{module, function}`,
  `phase_time/2` delegates the measurement to `module.function(phase, fun)` and
  returns its result. When the env is unset (production default) it runs `fun`
  directly, adding no overhead and keeping the pipeline behaviour identical.
  """

  @type phase :: atom()

  @spec phase_time(phase(), (-> result)) :: result when result: term()
  def phase_time(phase, fun) when is_function(fun, 0) do
    case Application.get_env(:td_core, :search_phase_profiler) do
      {mod, name} -> apply(mod, name, [phase, fun])
      _ -> fun.()
    end
  end
end
