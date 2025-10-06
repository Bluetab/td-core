defmodule TestCacheHelpers do
  @moduledoc """
  Support creation of templates in cache
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias TdCache.TemplateCache

  @template %{
    id: 123,
    label: "some name",
    name: "some_name",
    scope: "ts",
    subscope: nil,
    content: [
      %{
        "fields" => [
          %{
            "cardinality" => "?",
            "default" => %{"origin" => "default", "value" => ""},
            "group" => "",
            "label" => "field-text",
            "name" => "field-text",
            "type" => "string",
            "values" => nil,
            "widget" => "string"
          },
          %{
            "cardinality" => "?",
            "default" => %{"origin" => "default", "value" => ""},
            "group" => "",
            "label" => "single-user",
            "name" => "single-user",
            "subscribable" => false,
            "type" => "user",
            "values" => %{"role_users" => "Rol User"},
            "widget" => "dropdown"
          },
          %{
            "cardinality" => "?",
            "default" => %{"origin" => "default", "value" => ""},
            "group" => "",
            "label" => "single-group",
            "name" => "single-group",
            "subscribable" => false,
            "type" => "user_group",
            "values" => %{"role_groups" => "Rol User"},
            "widget" => "dropdown"
          },
          %{
            "cardinality" => "*",
            "default" => %{"origin" => "default", "value" => ""},
            "group" => "",
            "label" => "multi-user",
            "name" => "multi-user",
            "subscribable" => false,
            "type" => "user",
            "values" => %{"role_users" => "Rol User"},
            "widget" => "dropdown"
          },
          %{
            "cardinality" => "*",
            "default" => %{"origin" => "default", "value" => ""},
            "group" => "",
            "label" => "multi-group",
            "name" => "multi-group",
            "subscribable" => false,
            "type" => "user_group",
            "values" => %{"role_groups" => "Rol User"},
            "widget" => "dropdown"
          },
          %{
            "cardinality" => "*",
            "default" => %{"origin" => "default", "value" => ""},
            "group" => "",
            "label" => "table",
            "name" => "table",
            "subscribable" => false,
            "type" => "dynamic_table",
            "values" => %{
              "table_columns" => [
                %{
                  "name" => "col1",
                  "cardinality" => "?",
                  "default" => %{"origin" => "default", "value" => ""},
                  "group" => "",
                  "label" => "col1",
                  "type" => "string",
                  "values" => nil,
                  "widget" => "string"
                }
              ]
            },
            "widget" => "dynamic_table"
          }
        ],
        "name" => "test-group"
      }
    ],
    inserted_at: DateTime.utc_now(),
    updated_at: DateTime.utc_now()
  }

  def insert_template do
    %{id: template_id} = template = @template
    {:ok, _} = TemplateCache.put(template, publish: false)
    on_exit(fn -> TemplateCache.delete(template_id) end)
    template
  end
end
