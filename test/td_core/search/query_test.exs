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
      assert Query.build_query(@match_all, %{}, %{aggs: @aggs}) == %{bool: %{filter: @match_all}}
    end

    test "returns a boolean query with user-defined filters" do
      params = %{"must" => %{"type" => ["foo"]}}

      assert Query.build_query(@match_all, params, aggs: @aggs) == %{
               bool: %{
                 filter: %{term: %{"type.raw" => "foo"}}
               }
             }

      params = %{"must" => %{"type" => ["foo"], "status" => ["bar", "baz"]}}

      assert Query.build_query(@match_all, params, aggs: @aggs) == %{
               bool: %{
                 filter: [
                   %{term: %{"type.raw" => "foo"}},
                   %{terms: %{"status.raw" => ["bar", "baz"]}}
                 ]
               }
             }
    end

    test "returns a simple_query_string for the search term" do
      params = %{"query" => "foo"}

      assert Query.build_query(@match_all, params, aggs: @aggs) == %{
               bool: %{
                 must: %{simple_query_string: %{query: "\"foo\""}},
                 filter: %{match_all: %{}}
               }
             }
    end

    test "returns a boolean query with user-defined filters and simple_query_string" do
      params = %{
        "must" => %{"type" => ["foo"]},
        "query" => "foo"
      }

      assert Query.build_query(@match_all, params, aggs: @aggs) == %{
               bool: %{
                 filter: %{term: %{"type.raw" => "foo"}},
                 must: %{simple_query_string: %{query: "\"foo\""}}
               }
             }
    end

    test "returns a multi_match query when clauses are provided" do
      params = %{
        "must" => %{"type" => ["foo"]},
        "query" => "foo"
      }

      multi_match_phrase_prefix = %{
        must: %{
          multi_match: %{
            type: "phrase_prefix",
            fields: ["name^3"],
            lenient: true,
            slop: 2
          }
        }
      }

      assert Query.build_query(@match_all, params,
               aggs: @aggs,
               clauses: multi_match_phrase_prefix
             ) == %{
               bool: %{
                 filter: %{term: %{"type.raw" => "foo"}},
                 must: %{
                   multi_match: %{
                     query: "foo",
                     type: "phrase_prefix",
                     fields: ["name^3"],
                     lenient: true,
                     slop: 2
                   }
                 }
               }
             }
    end

    test "returns a must and should query when clauses are provided" do
      params = %{
        "must" => %{"type" => ["foo"]},
        "query" => "foo"
      }

      clauses = %{
        must: %{
          multi_match: %{
            type: "bool_prefix",
            fields: ["ngram_name^3"],
            lenient: true,
            slop: 2
          }
        },
        should: %{
          multi_match: %{
            type: "phrase_prefix",
            fields: ["name^3"]
          }
        }
      }

      assert Query.build_query(@match_all, params,
               aggs: @aggs,
               clauses: clauses
             ) == %{
               bool: %{
                 filter: %{term: %{"type.raw" => "foo"}},
                 must: %{
                   multi_match: %{
                     type: "bool_prefix",
                     query: "foo",
                     fields: ["ngram_name^3"],
                     lenient: true,
                     slop: 2
                   }
                 },
                 should: %{
                   multi_match: %{
                     type: "phrase_prefix",
                     query: "foo",
                     fields: ["name^3"]
                   }
                 }
               }
             }
    end

    test "returns a query with minimum_should_match" do
      params = %{
        "query" => "foo",
        "minimum_should_match" => 1
      }

      clauses = %{
        should: [
          %{
            multi_match: %{
              type: "bool_prefix",
              fields: ["ngram_name^3"],
              lenient: true,
              slop: 2
            }
          },
          %{simple_query_string: %{fields: ["name^3"]}}
        ]
      }

      assert Query.build_query(@match_all, params, aggs: @aggs, clauses: clauses) ==
               %{
                 bool: %{
                   filter: %{match_all: %{}},
                   should: [
                     %{
                       multi_match: %{
                         type: "bool_prefix",
                         fields: ["ngram_name^3"],
                         query: "foo",
                         lenient: true,
                         slop: 2
                       }
                     },
                     %{simple_query_string: %{query: "foo", fields: ["name^3"]}}
                   ],
                   minimum_should_match: 1
                 }
               }
    end

    test "removes minimum_should_match when should clauses are not provided" do
      params = %{
        "query" => "foo",
        "minimum_should_match" => 1
      }

      assert Query.build_query(@match_all, params, aggs: @aggs) == %{
               bool: %{
                 filter: %{match_all: %{}},
                 must: %{simple_query_string: %{query: "\"foo\""}}
               }
             }
    end

    test "returns a term query when clauses are provided" do
      params = %{
        "must" => %{"type" => ["type"]},
        "query" => "foo"
      }

      clauses = %{
        must: %{
          multi_match: %{
            type: "bool_prefix",
            fields: ["ngram_name^3"],
            lenient: true,
            slop: 2
          }
        },
        should: [
          %{
            multi_match: %{
              type: "phrase_prefix",
              fields: ["name^3"]
            }
          },
          %{term: %{"name.exact" => %{"boost" => 2.0}}}
        ]
      }

      assert Query.build_query(@match_all, params, aggs: @aggs, clauses: clauses) == %{
               bool: %{
                 filter: %{term: %{"type.raw" => "type"}},
                 must: %{
                   multi_match: %{
                     type: "bool_prefix",
                     query: "foo",
                     fields: ["ngram_name^3"],
                     lenient: true,
                     slop: 2
                   }
                 },
                 should: [
                   %{
                     multi_match: %{
                       type: "phrase_prefix",
                       query: "foo",
                       fields: ["name^3"]
                     }
                   },
                   %{term: %{"name.exact" => %{"boost" => 2.0, "value" => "foo"}}}
                 ]
               }
             }
    end

    test "includes should filters when provided" do
      params = %{
        "must" => %{"type" => ["foo"]},
        "filters" => %{"should" => %{"id" => [1, 2], "parent_id" => [3, 4]}}
      }

      assert Query.build_query(@match_all, params, aggs: @aggs) ==
               %{
                 bool: %{
                   filter: [
                     %{term: %{"type.raw" => "foo"}},
                     %{
                       bool: %{
                         should: [
                           %{terms: %{"id" => [1, 2]}},
                           %{terms: %{"parent_id" => [3, 4]}}
                         ],
                         minimum_should_match: 1
                       }
                     }
                   ]
                 }
               }
    end

    test "returns a simple_query_string query when clauses are provided" do
      params = %{
        "must" => %{"type" => ["foo"]},
        "query" => "foo"
      }

      simple_query_string = %{
        must: %{simple_query_string: %{fields: ["name^3"]}}
      }

      assert Query.build_query(@match_all, params,
               aggs: @aggs,
               clauses: simple_query_string
             ) == %{
               bool: %{
                 filter: %{term: %{"type.raw" => "foo"}},
                 must: %{simple_query_string: %{query: "foo", fields: ["name^3"]}}
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
                 filter: [%{exists: %{field: "bar"}}, %{exists: %{field: "foo"}}, @match_none],
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
end
