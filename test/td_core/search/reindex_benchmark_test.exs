defmodule TdCore.Search.ReindexBenchmarkTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias TdCore.Search.ReindexBenchmark

  describe "run/3" do
    test "prints banner and summary and returns metrics without calling real Indexer" do
      reindex_fn = fn index, target ->
        send(self(), {:reindex_called, index, target})
        :ok
      end

      output =
        capture_io(fn ->
          results =
            ReindexBenchmark.run(:structures, [1, 2, 3],
              doc_count: 3,
              repeat: 1,
              profile: false,
              reindex_fn: reindex_fn,
              env_banner_fn: fn -> [enrich_concurrency: 2] end
            )

          assert length(results) == 1
          assert hd(results).result == :ok
          assert hd(results).doc_count == 3
          assert hd(results).micros >= 0
        end)

      assert_received {:reindex_called, :structures, [1, 2, 3]}
      assert output =~ "Benchmark configuration:"
      assert output =~ "target               3 ids"
      assert output =~ "documents            3"
      assert output =~ "enrich_concurrency   2"
      assert output =~ "Timing:"
      assert output =~ "wall_clock"
      assert output =~ "throughput"
      assert output =~ "reindex result       :ok"
    end

    test "supports :all target and repeat > 1 summary" do
      reindex_fn = fn _index, target ->
        send(self(), {:target, target})
        :ok
      end

      output =
        capture_io(fn ->
          results =
            ReindexBenchmark.run(:implementations, :all,
              doc_count: 10,
              repeat: 2,
              reindex_fn: reindex_fn
            )

          assert length(results) == 2
        end)

      assert_received {:target, :all}
      assert_received {:target, :all}
      assert output =~ "target               :all"
      assert output =~ "=== Repeat summary (2 runs) ==="
    end

    test "profile sampling includes profile section when enabled" do
      reindex_fn = fn _index, _target ->
        Process.sleep(60)
        :ok
      end

      output =
        capture_io(fn ->
          [metrics] =
            ReindexBenchmark.run(:structures, [1],
              doc_count: 1,
              profile: true,
              profile_interval_ms: 20,
              reindex_fn: reindex_fn
            )

          assert length(metrics.samples) >= 1
        end)

      assert output =~ "Profile samples"
      assert output =~ "memory total"
    end
  end
end
