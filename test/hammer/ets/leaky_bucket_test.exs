defmodule Hammer.ETS.LeakyBucketTest do
  use ExUnit.Case, async: true
  alias Hammer.ETS.LeakyBucket

  defmodule RateLimitLeakyBucket do
    use Hammer, backend: :ets, algorithm: :leaky_bucket
  end

  setup do
    table = :ets.new(:hammer_leaky_bucket_test, LeakyBucket.ets_opts())
    {:ok, table: table}
  end

  describe "hit/get" do
    test "returns {:allow, 1} tuple on first access", %{table: table} do
      key = "key"
      leak_rate = 10
      capacity = 10

      assert {:allow, 1} = LeakyBucket.hit(table, key, leak_rate, capacity, 1)
    end

    test "returns {:allow, 4} tuple on in-limit checks", %{table: table} do
      key = "key"
      leak_rate = 2
      capacity = 10

      assert {:allow, 1} = LeakyBucket.hit(table, key, leak_rate, capacity, 1)
      assert {:allow, 2} = LeakyBucket.hit(table, key, leak_rate, capacity, 1)
      assert {:allow, 3} = LeakyBucket.hit(table, key, leak_rate, capacity, 1)
      assert {:allow, 4} = LeakyBucket.hit(table, key, leak_rate, capacity, 1)
    end

    test "returns expected tuples on mix of in-limit and out-of-limit checks", %{table: table} do
      key = "key"
      leak_rate = 1
      capacity = 2

      assert {:allow, 1} = LeakyBucket.hit(table, key, leak_rate, capacity, 1)
      assert {:allow, 2} = LeakyBucket.hit(table, key, leak_rate, capacity, 1)

      assert {:deny, 1000} =
               LeakyBucket.hit(table, key, leak_rate, capacity, 1)

      assert {:deny, _retry_after} =
               LeakyBucket.hit(table, key, leak_rate, capacity, 1)
    end

    test "returns expected tuples after waiting for the next window", %{table: table} do
      key = "key"
      leak_rate = 1
      capacity = 2

      assert {:allow, 1} = LeakyBucket.hit(table, key, leak_rate, capacity, 1)
      assert {:allow, 2} = LeakyBucket.hit(table, key, leak_rate, capacity, 1)

      assert {:deny, retry_after} =
               LeakyBucket.hit(table, key, leak_rate, capacity, 1)

      :timer.sleep(retry_after)

      assert {:allow, 2} = LeakyBucket.hit(table, key, leak_rate, capacity, 1)

      assert {:deny, _retry_after} =
               LeakyBucket.hit(table, key, leak_rate, capacity, 1)
    end
  end

  describe "hit with concurrent cleanup" do
    test "does not crash when a concurrent deleter races with hit/5", %{table: table} do
      key = "concurrent_key"
      leak_rate = 10
      capacity = 10

      # Spawn a process that continuously deletes the key to provoke the race
      # between insert_new and lookup inside hit/5
      deleter =
        spawn_link(fn ->
          Stream.repeatedly(fn ->
            try do
              :ets.delete(table, key)
            rescue
              ArgumentError -> :ok
            end
          end)
          |> Stream.run()
        end)

      # Call hit/5 many times concurrently; none should raise MatchError
      results =
        Task.async_stream(
          1..200,
          fn _ -> LeakyBucket.hit(table, key, leak_rate, capacity, 1) end,
          max_concurrency: 10,
          timeout: 30_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      Process.unlink(deleter)
      Process.exit(deleter, :kill)

      # Every result must be a valid {:allow, _} or {:deny, _}
      for result <- results do
        assert {tag, _level} = result
        assert tag in [:allow, :deny]
      end
    end
  end

  describe "get" do
    test "get returns current bucket level", %{table: table} do
      key = "key"
      leak_rate = 1
      capacity = 10

      assert LeakyBucket.get(table, key) == 0

      assert {:allow, _} = LeakyBucket.hit(table, key, leak_rate, capacity, 4)
      assert LeakyBucket.get(table, key) == 4

      assert {:allow, _} = LeakyBucket.hit(table, key, leak_rate, capacity, 3)
      assert LeakyBucket.get(table, key) == 7
    end
  end
end
