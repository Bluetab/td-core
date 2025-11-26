defmodule TdCore.Search.FiltersTest do
  use ExUnit.Case

  alias TdCore.Search.Filters

  describe "build_filters/3" do
    test "creates filter clauses matching aggregations" do
      filters = %{
        "foo" => "foo",
        "bar" => ["bar1", "bar2"],
        "baz" => ["baz"],
        "table.col1" => ["val1", "val2"],
        "table.col2" => "val3"
      }

      aggs = %{
        "foo" => %{terms: %{field: "foo_field"}},
        "bar" => %{
          nested: %{path: "content.bar"},
          aggs: %{distinct_search: %{terms: %{field: "content.bar.xyzzy"}}}
        },
        "table" => %{
          meta: %{type: "dynamic_table"},
          aggs: %{
            "col1" => %{terms: %{size: 500, field: "content.dependent.col1.raw"}},
            "col2" => %{terms: %{size: 500, field: "content.dependent.col2.raw"}}
          },
          nested: %{path: "content.dependent"}
        }
      }

      assert filters
             |> Enum.sort()
             |> Filters.build_filters(aggs, %{must: %{wtf: %{}}}) == %{
               filter: [
                 %{
                   nested: %{
                     path: "content.dependent",
                     query: %{term: %{"content.dependent.col2.raw" => "val3"}}
                   }
                 },
                 %{
                   nested: %{
                     path: "content.dependent",
                     query: %{terms: %{"content.dependent.col1.raw" => ["val1", "val2"]}}
                   }
                 },
                 %{term: %{"foo_field" => "foo"}},
                 %{term: %{"baz" => "baz"}},
                 %{
                   nested: %{
                     path: "content.bar",
                     query: %{terms: %{"content.bar.xyzzy" => ["bar1", "bar2"]}}
                   }
                 }
               ],
               must: %{wtf: %{}}
             }
    end

    test "creates must_not filter with one term" do
      assert Filters.build_filters(%{"must_not" => %{"foo" => "bar"}}, %{}, %{}) ==
               %{
                 must_not: [
                   %{term: %{"foo" => "bar"}}
                 ]
               }
    end

    test "creates must_not filter with more of one terms" do
      assert Filters.build_filters(
               %{
                 "must_not" => %{
                   "foo" => "bar",
                   "bar" => "baz"
                 }
               },
               %{},
               %{}
             ) ==
               %{
                 must_not: [
                   %{term: %{"bar" => "baz"}},
                   %{term: %{"foo" => "bar"}}
                 ]
               }
    end

    test "handles updated_at, start_date and end_date as ranges" do
      for field <- ["updated_at", "inserted_at", "start_date", "end_date", "last_change_at"] do
        assert Filters.build_filters(%{field => %{"gte" => "now-1d/d"}}, %{}, %{}) ==
                 %{filter: %{range: %{field => %{"gte" => "now-1d/d"}}}}
      end
    end

    test "handles nested should filters" do
      filters = %{"id" => [1, 2], "parent_id" => [3, 4]}

      assert Filters.build_filters(%{"should" => filters}, %{}, %{}) == %{
               should: [
                 filter: %{terms: %{"id" => [1, 2]}},
                 filter: %{terms: %{"parent_id" => [3, 4]}}
               ]
             }
    end
  end
end
