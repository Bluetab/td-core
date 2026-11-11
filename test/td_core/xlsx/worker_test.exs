defmodule TdCore.XLSX.WorkerTest do
  @moduledoc """
  Tests for the pure part of `TdCore.XLSX.Worker`. The `use` expansion needs Oban,
  which td-core does not depend on, so it is tested in the consumer applications.
  """
  use ExUnit.Case

  alias TdCore.XLSX.Worker

  setup do
    tmp = Path.join(System.tmp_dir!(), "td_core_worker_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)
    path = Path.join(tmp, "upload.xlsx")
    File.write!(path, "synthetic")
    on_exit(fn -> File.rm_rf(tmp) end)
    %{path: path}
  end

  test "timeout is 10 minutes" do
    assert Worker.timeout_ms() == 600_000
  end

  test "perform/2 runs the runner with the args and removes the file", %{path: path} do
    args = %{"path" => path, "job_id" => 1}
    parent = self()

    assert :done =
             Worker.perform(args, fn received ->
               send(parent, {:run, received})
               :done
             end)

    assert_received {:run, ^args}
    refute File.exists?(path)
  end

  test "perform/2 removes the file even when the runner raises", %{path: path} do
    assert_raise RuntimeError, "boom", fn ->
      Worker.perform(%{"path" => path}, fn _ -> raise "boom" end)
    end

    refute File.exists?(path)
  end

  test "perform/2 removes the file when the job process is killed (Oban timeout)", %{path: path} do
    parent = self()

    pid =
      spawn(fn ->
        Worker.perform(%{"path" => path}, fn _ ->
          send(parent, :running)
          Process.sleep(:infinity)
        end)
      end)

    assert_receive :running
    assert File.exists?(path)

    # Oban enforces its timeout with an exit signal, which skips `after` clauses.
    ref = Process.monitor(pid)
    Process.exit(pid, :timeout)
    assert_receive {:DOWN, ^ref, :process, ^pid, :timeout}

    assert wait_until(fn -> not File.exists?(path) end)
  end

  test "perform/2 does not trap exits, so the timeout still stops the job", %{path: path} do
    parent = self()

    pid =
      spawn(fn ->
        Worker.perform(%{"path" => path}, fn _ ->
          send(parent, {:trapping, Process.info(self(), :trap_exit)})
          Process.sleep(:infinity)
        end)
      end)

    assert_receive {:trapping, {:trap_exit, false}}
    ref = Process.monitor(pid)
    Process.exit(pid, :timeout)
    assert_receive {:DOWN, ^ref, :process, ^pid, :timeout}
  end

  defp wait_until(fun, attempts \\ 50) do
    cond do
      fun.() ->
        true

      attempts == 0 ->
        false

      true ->
        Process.sleep(20)
        wait_until(fun, attempts - 1)
    end
  end
end
