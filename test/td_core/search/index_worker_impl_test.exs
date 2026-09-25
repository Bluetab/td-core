defmodule TdCore.Search.IndexWorkerImplTest do
  use ExUnit.Case, async: false

  alias Elasticsearch.Cluster.Config
  alias TdCore.Search.Cluster
  alias TdCore.Search.IndexWorkerImpl

  @index :grants_buffer_test

  setup do
    previous_config = Config.get(Cluster)
    parent = self()

    Application.put_env(:td_core, :index_worker_runner, fn _index, op ->
      send(parent, {:ran, op, self()})

      receive do
        :continue -> :ok
      end
    end)

    aliases = Map.put(Map.get(previous_config, :aliases, %{}), @index, "grants_buffer_test")

    indexes =
      Map.put(Map.get(previous_config, :indexes, %{}), @index, %{
        buffer_partials_during_reindex: true,
        settings: %{}
      })

    :sys.replace_state(Cluster, fn _ ->
      previous_config
      |> Map.put(:aliases, aliases)
      |> Map.put(:indexes, indexes)
    end)

    GenServer.call(Cluster, :save_config)

    on_exit(fn ->
      Application.delete_env(:td_core, :index_worker_runner)

      if pid = Process.whereis(@index) do
        GenServer.stop(pid)
      end

      if pid = Process.whereis(:index_worker_plain) do
        GenServer.stop(pid)
      end

      :sys.replace_state(Cluster, fn _ -> previous_config end)
      GenServer.call(Cluster, :save_config)
    end)

    :ok
  end

  test "keeps the serial worker state when buffering is off" do
    {:ok, pid} = IndexWorkerImpl.start_link(:index_worker_plain)

    assert :sys.get_state(pid) == :index_worker_plain
  end

  test "replays coalesced partials and deletes after a full reindex" do
    {:ok, _pid} = IndexWorkerImpl.start_link(@index)

    GenServer.cast(@index, {:reindex, :all})
    assert_receive {:ran, {:reindex, :all}, task}

    GenServer.cast(@index, {:reindex, [1, 2]})
    GenServer.cast(@index, {:reindex, [2, 3]})
    GenServer.cast(@index, {:delete, [4]})
    _ = :sys.get_state(@index)

    send(task, :continue)

    assert_receive {:ran, {:reindex, [1, 2, 3]}, task}
    send(task, :continue)

    assert_receive {:ran, {:delete, [4]}, task}
    send(task, :continue)

    refute_receive {:ran, _, _}, 50
  end

  test "a later full reindex drops partials queued during the current one" do
    {:ok, _pid} = IndexWorkerImpl.start_link(@index)

    GenServer.cast(@index, {:reindex, :all})
    assert_receive {:ran, {:reindex, :all}, task}

    GenServer.cast(@index, {:reindex, [1]})
    GenServer.cast(@index, {:reindex, :all})
    _ = :sys.get_state(@index)

    send(task, :continue)

    assert_receive {:ran, {:reindex, :all}, task}
    send(task, :continue)

    refute_receive {:ran, {:reindex, [1]}, _}, 50
  end

  test "runs one partial at a time when no full reindex is in progress" do
    {:ok, _pid} = IndexWorkerImpl.start_link(@index)

    GenServer.cast(@index, {:reindex, [1]})
    assert_receive {:ran, {:reindex, [1]}, task}

    GenServer.cast(@index, {:reindex, [2]})
    _ = :sys.get_state(@index)
    refute_received {:ran, {:reindex, [2]}, _}

    send(task, :continue)

    assert_receive {:ran, {:reindex, [2]}, task}
    send(task, :continue)
  end
end
