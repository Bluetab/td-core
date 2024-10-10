defmodule TdCore.Search.QueryTest do
  use ExUnit.Case

  alias TdCore.Search.Query

  @match_all %{match_all: %{}}
  @match_none %{match_none: %{}}
  @aggs %{
    "status" => %{terms: %{field: "status.raw", size: 50}},
    "type" => %{terms: %{field: "type.raw", size: 50}}
  }

  describe "build_query/1" do
    test "returns a boolean query with a match_all filter by default" do
      assert Query.build_query(@match_all, %{}, @aggs) == %{bool: %{must: @match_all}}
    end

    test "returns a boolean query with user-defined filters" do
      params = %{"must" => %{"type" => ["foo"]}}

      assert Query.build_query(@match_all, params, @aggs) == %{
               bool: %{
                 must: %{term: %{"type.raw" => "foo"}}
               }
             }

      params = %{"must" => %{"type" => ["foo"], "status" => ["bar", "baz"]}}

      assert Query.build_query(@match_all, params, @aggs) == %{
               bool: %{
                 must: [
                   %{term: %{"type.raw" => "foo"}},
                   %{terms: %{"status.raw" => ["bar", "baz"]}}
                 ]
               }
             }
    end

    test "returns a simple_query_string for the search term" do
      params = %{"query" => "foo"}

      assert Query.build_query(@match_all, params, @aggs) == %{
               bool: %{
                 must: %{simple_query_string: %{query: "foo*"}},
                 should: %{multi_match: %{operator: "and", query: "foo*", type: "best_fields"}}
               }
             }
    end

    test "returns a boolean query with user-defined filters and simple_query_string" do
      params = %{
        "must" => %{"type" => ["foo"]},
        "query" => "foo"
      }

      assert Query.build_query(@match_all, params, @aggs) == %{
               bool: %{
                 must: [%{simple_query_string: %{query: "foo*"}}, %{term: %{"type.raw" => "foo"}}],
                 should: %{multi_match: %{operator: "and", query: "foo*", type: "best_fields"}}
               }
             }
    end

    test "with and without clauses" do
      params = %{
        "with" => ["foo", "bar"],
        "without" => "baz"
      }

      assert Query.build_query(@match_none, params) == %{
               bool: %{
                 must: [%{exists: %{field: "bar"}}, %{exists: %{field: "foo"}}, @match_none],
                 must_not: %{exists: %{field: "baz"}}
               }
             }
    end

    test "returns a query with must_not filters" do
      filters = %{
        must_not: [%{term: %{"foo" => "bar"}}]
      }

      params = %{}

      assert Query.build_query(filters, params) == %{
               bool: %{
                 must_not: %{term: %{"foo" => "bar"}}
               }
             }
    end

    test "returns a query with must_not params" do
      filters = %{}

      params = %{
        "must" => %{"must_not" => %{"foo" => ["bar"]}}
      }

      assert Query.build_query(filters, params) == %{
               bool: %{
                 must: %{},
                 must_not: %{term: %{"foo" => "bar"}}
               }
             }
    end

    test "returns a query with must_not params and filter" do
      filters = %{
        must_not: [%{term: %{"foo" => "bar"}}]
      }

      params = %{
        "must" => %{"must_not" => %{"baz" => ["xyz"]}}
      }

      assert Query.build_query(filters, params) == %{
               bool: %{
                 must_not: [
                   %{term: %{"baz" => "xyz"}},
                   %{term: %{"foo" => "bar"}}
                 ]
               }
             }
    end
  end

  describe "maybe_add_since/3" do
    test "return params when since in params" do
      params = %{
        "since" => "2024-01-01 00:00",
        "foo" => "bar"
      }

      %{"foo" => _, "must" => filter} = Query.maybe_add_since(params, "date_field")
      assert %{"date_field" => %{"gte" => "2024-01-01 00:00"}} = filter

      params = %{
        "must" => %{"field" => "value"},
        "since" => "2024-01-01 00:00",
        "foo" => "bar"
      }

      %{"foo" => _, "must" => filters} = Query.maybe_add_since(params, "date_field")

      assert [
               %{"date_field" => %{"gte" => "2024-01-01 00:00"}},
               %{"field" => "value"}
             ] = filters

      params = %{
        "must" => [%{"field" => "value"}, %{"field_2" => "value_2"}],
        "since" => "2024-01-01 00:00",
        "foo" => "bar"
      }

      %{"foo" => _, "must" => filters} = Query.maybe_add_since(params, "date_field")

      assert [
               %{"date_field" => %{"gte" => "2024-01-01 00:00"}},
               %{"field" => "value"},
               %{"field_2" => "value_2"}
             ] = filters
    end

    test "return same params when no since in params" do
      params = %{}
      assert params == Query.maybe_add_since(params, "date_field")

      params = %{
        "must" => %{"field" => "value"},
        "foo" => "bar"
      }

      assert params == Query.maybe_add_since(params, "date_field")

      params = %{
        "must" => [%{"field" => "value"}, %{"field_2" => "value_2"}],
        "foo" => "bar"
      }

      assert params == Query.maybe_add_since(params, "date_field")
    end
  end

  describe "maybe_add_min_id/3" do
    test "return params when min_id in params" do
      params = %{
        "min_id" => 100,
        "foo" => "bar"
      }

      %{"foo" => _, "must" => filter} = Query.maybe_add_min_id(params)
      assert %{"id" => %{"gte" => 100}} = filter

      params = %{
        "must" => %{"field" => "value"},
        "min_id" => 100,
        "foo" => "bar"
      }

      %{"foo" => _, "must" => filters} = Query.maybe_add_min_id(params)

      assert [
               %{"id" => %{"gte" => 100}},
               %{"field" => "value"}
             ] = filters

      params = %{
        "must" => [%{"field" => "value"}, %{"field_2" => "value_2"}],
        "min_id" => 100,
        "foo" => "bar"
      }

      %{"foo" => _, "must" => filters} = Query.maybe_add_min_id(params)

      assert [
               %{"id" => %{"gte" => 100}},
               %{"field" => "value"},
               %{"field_2" => "value_2"}
             ] = filters
    end

    test "return same params when no min_id in params" do
      params = %{}
      assert params == Query.maybe_add_min_id(params)

      params = %{
        "must" => %{"field" => "value"},
        "foo" => "bar"
      }

      assert params == Query.maybe_add_min_id(params)

      params = %{
        "must" => [%{"field" => "value"}, %{"field_2" => "value_2"}],
        "foo" => "bar"
      }

      assert params == Query.maybe_add_min_id(params)
    end
  end
end
