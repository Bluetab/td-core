defmodule TdCore.XLSX.BulkLoad do
  @moduledoc """
  Generic XLSX bulk load.

  Dispatches to the appropriate BulkLoadProtocol implementation based on job scope.

  `BulkLoadProtocol.get_opts/1` may include `:bulk_load_items`, a function that
  receives all parsed items and the load context and returns `{item, result}`
  pairs in the same order. Each result follows the `bulk_load_item/3` contract.
  Without this option, items are processed one at a time.
  """

  alias TdCluster.Cluster.TdAudit.UploadJobs
  alias TdCore.XLSX.BulkLoadProtocol
  alias TdDfLib.Format

  require Logger

  def bulk_load(raw_sheets, ctx) do
    opts = BulkLoadProtocol.get_opts(ctx.impl_for)

    required_headers = Keyword.get(opts, :required_headers, [])
    discarded_headers = Keyword.get(opts, :discarded_headers, [])
    extra_headers = Keyword.get(opts, :extra_headers, [])
    translate_fn = Keyword.get(opts, :translate_fn, fn h, _ -> h end)

    # Columns are described as `{technical_name, label}` pairs, where the label
    # is the value returned by `translate_fn` (or the technical name itself).
    to_pairs = fn headers ->
      Enum.map(headers, &{&1, translate_fn.(&1, ctx.lang) || &1})
    end

    extra_context = Keyword.get(opts, :extra_context, %{})

    required_pairs = to_pairs.(required_headers)
    base_pairs = required_pairs ++ to_pairs.(extra_headers)
    discarded_pairs = to_pairs.(discarded_headers)

    headers_ctx =
      Map.merge(ctx, %{
        required_pairs: required_pairs,
        base_pairs: base_pairs,
        discarded_pairs: discarded_pairs
      })

    {valid_sheets, invalid_sheet_count} = validate_sheets_headers(raw_sheets, headers_ctx)

    params = parse_sheets(valid_sheets, headers_ctx)

    ctx =
      valid_sheets
      |> Enum.map(&elem(&1, 0))
      |> then(&BulkLoadProtocol.sheets_to_templates(ctx.impl_for, &1))
      |> parse_templates(ctx.lang)
      |> then(&Map.put(ctx, :templates, &1))
      |> Map.merge(extra_context)

    {inserted_ids, updated_ids, extra_ids, error_count, unchanged_count} =
      process_items(params, opts, ctx)

    all_ids = inserted_ids ++ updated_ids ++ extra_ids

    if all_ids != [] do
      BulkLoadProtocol.on_complete(ctx.impl_for, all_ids)
    end

    {:ok,
     %{
       insert_count: length(inserted_ids),
       update_count: length(updated_ids),
       error_count: error_count,
       unchanged_count: unchanged_count,
       invalid_sheet_count: invalid_sheet_count
     }}
  end

  defp process_items(items, opts, ctx) do
    case Keyword.get(opts, :bulk_load_items) do
      nil ->
        Enum.reduce(items, empty_acc(), &process_item(&1, &2, ctx))

      bulk_load_items ->
        process_batch(items, bulk_load_items, ctx)
    end
  end

  defp process_batch(items, bulk_load_items, ctx) do
    results =
      try do
        bulk_load_items.(items, ctx)
      rescue
        exception ->
          Logger.error(
            "bulk_load_items raised: " <> Exception.format(:error, exception, __STACKTRACE__)
          )

          {:exception, Exception.message(exception)}
      end

    process_batch_results(results, items, ctx)
  end

  defp process_batch_results({:exception, message}, items, ctx),
    do: create_batch_unexpected_errors(items, ctx, message)

  defp process_batch_results(results, items, ctx) do
    if valid_batch_results?(items, results) do
      Enum.reduce(results, empty_acc(), fn {item, result}, acc ->
        emit_result(result, item, acc, ctx)
      end)
    else
      create_batch_unexpected_errors(items, ctx, "Unexpected bulk_load_items result length")
    end
  end

  defp valid_batch_results?(items, results) when length(items) == length(results) do
    Enum.zip(items, results)
    |> Enum.all?(fn
      {item, {result_item, _result}} when item == result_item -> true
      _ -> false
    end)
  end

  defp valid_batch_results?(_items, _results), do: false

  defp create_batch_unexpected_errors(items, ctx, message) do
    Enum.reduce(items, empty_acc(), fn item, acc ->
      create_unexpected_error(ctx, item, message)
      increment_errors(acc)
    end)
  end

  defp empty_acc, do: {[], [], [], 0, 0}

  defp process_item(item, {ids, updated_ids, extra_ids, errors, unchanged_count}, ctx) do
    BulkLoadProtocol.bulk_load_item(ctx.impl_for, item, ctx)
    |> emit_result(item, {ids, updated_ids, extra_ids, errors, unchanged_count}, ctx)
  rescue
    exception ->
      Logger.error(
        "bulk_load_item raised: " <> Exception.format(:error, exception, __STACKTRACE__)
      )

      create_unexpected_error(ctx, item, Exception.message(exception))
      {ids, updated_ids, extra_ids, errors + 1, unchanged_count}
  end

  defp emit_result(
         {:error, {type, details}},
         item,
         {ids, updated_ids, extra_ids, errors, unchanged_count},
         ctx
       ) do
    UploadJobs.create_error(ctx.job_id, %{
      type: type,
      sheet: item["_sheet"],
      row_number: item["_row_number"],
      details: details
    })

    {ids, updated_ids, extra_ids, errors + 1, unchanged_count}
  end

  defp emit_result(
         {:unchanged, details},
         item,
         {ids, updated_ids, extra_ids, errors, unchanged_count},
         ctx
       ) do
    UploadJobs.create_info(ctx.job_id, %{
      type: "unchanged",
      sheet: item["_sheet"],
      row_number: item["_row_number"],
      details: details
    })

    {ids, updated_ids, extra_ids, errors, unchanged_count + 1}
  end

  defp emit_result(
         {:created, {id, details}},
         item,
         {ids, updated_ids, extra_ids, errors, unchanged_count},
         ctx
       ) do
    UploadJobs.create_info(ctx.job_id, %{
      type: "created",
      sheet: item["_sheet"],
      row_number: item["_row_number"],
      details: details
    })

    {[id | ids], updated_ids, extra_ids, errors, unchanged_count}
  end

  defp emit_result(
         {:updated, {[id | rest], details}},
         item,
         {ids, updated_ids, extra_ids, errors, unchanged_count},
         ctx
       ) do
    UploadJobs.create_info(ctx.job_id, %{
      type: "updated",
      sheet: item["_sheet"],
      row_number: item["_row_number"],
      details: details
    })

    {ids, [id | updated_ids], rest ++ extra_ids, errors, unchanged_count}
  end

  defp emit_result(
         {:updated, {id, details}},
         item,
         {ids, updated_ids, extra_ids, errors, unchanged_count},
         ctx
       ) do
    UploadJobs.create_info(ctx.job_id, %{
      type: "updated",
      sheet: item["_sheet"],
      row_number: item["_row_number"],
      details: details
    })

    {ids, [id | updated_ids], extra_ids, errors, unchanged_count}
  end

  defp emit_result(unexpected, item, acc, ctx) do
    Logger.error("Unexpected bulk_load_item result: #{inspect(unexpected)}")
    create_unexpected_error(ctx, item, inspect(unexpected))
    increment_errors(acc)
  end

  defp increment_errors({ids, updated_ids, extra_ids, errors, unchanged_count}),
    do: {ids, updated_ids, extra_ids, errors + 1, unchanged_count}

  defp create_unexpected_error(ctx, item, message) when is_binary(message) do
    UploadJobs.create_error(ctx.job_id, %{
      type: "unexpected_error",
      sheet: item["_sheet"],
      row_number: item["_row_number"],
      details: %{message: message}
    })
  end

  defp validate_sheets_headers(sheets, ctx) do
    Enum.reduce(sheets, {[], 0}, fn {sheet_name, {headers, _rows}} = sheet,
                                    {valid_sheets, invalid_count} ->
      if validate_sheet_headers(sheet_name, headers, ctx) do
        {valid_sheets ++ [sheet], invalid_count}
      else
        {valid_sheets, invalid_count + 1}
      end
    end)
  end

  # Maps each file header of a sheet to the technical name of the column it
  # identifies. A file header is recognised when it equals the label or the
  # technical name of a column. Collisions (two file headers resolving to the
  # same column): the label match wins, and a technical-name match is ignored
  # when another file header already claims that column through its label.
  # This keeps the label-only behaviour that existed before technical names
  # were accepted.
  defp match_headers(file_headers, pairs) do
    label_hits =
      for {technical, label} <- pairs, Enum.member?(file_headers, label), do: {label, technical}

    claimed = MapSet.new(label_hits, fn {_label, technical} -> technical end)

    technical_hits =
      for {technical, _label} <- pairs,
          Enum.member?(file_headers, technical),
          not MapSet.member?(claimed, technical),
          do: {technical, technical}

    Map.new(technical_hits ++ label_hits)
  end

  defp validate_sheet_headers(sheet_name, headers, ctx) do
    matched = match_headers(headers, ctx.base_pairs)

    ctx.required_pairs
    |> Enum.reject(fn {technical, _label} ->
      Enum.any?(matched, fn {_header, tech} -> tech == technical end)
    end)
    |> Enum.map(fn {_technical, label} -> label end)
    |> case do
      [] ->
        true

      missing_headers ->
        UploadJobs.create_error(ctx.job_id, %{
          type: "missing_required_headers",
          sheet: sheet_name,
          details: %{missing_headers: missing_headers}
        })

        false
    end
  end

  defp parse_sheets(sheets, ctx) do
    sheets
    |> Enum.flat_map(&parse_sheet(&1, ctx))
    |> List.flatten()
  end

  defp parse_sheet({sheet_name, {headers, rows}}, ctx) do
    matched = match_headers(headers, ctx.base_pairs)
    discarded = match_headers(headers, ctx.discarded_pairs)

    {base_headers, df_headers} =
      headers
      |> Enum.with_index()
      |> Enum.reduce({[], []}, fn {header, idx} = value, {headers, df_headers} ->
        cond do
          Map.has_key?(discarded, header) ->
            {headers, df_headers}

          Map.has_key?(matched, header) ->
            {[{Map.get(matched, header), idx} | headers], df_headers}

          true ->
            {headers, [value | df_headers]}
        end
      end)

    Enum.with_index(rows, fn row, index ->
      df_content =
        Enum.into(df_headers, %{}, fn {header, idx} ->
          value = Enum.at(row, idx)
          {header, %{"value" => value, "origin" => "file"}}
        end)

      base_headers
      |> Enum.into(%{}, fn {header, idx} ->
        {header, Enum.at(row, idx)}
      end)
      |> Map.merge(%{
        "df_content" => df_content,
        "_sheet" => sheet_name,
        "_row_number" => index + 2
      })
    end)
  end

  defp parse_templates(templates, lang) do
    templates
    |> Enum.reject(fn {_df_name, template} -> is_nil(template) end)
    |> Enum.into(%{}, fn {df_name, template} ->
      content_schema = Format.flatten_content_fields(template.content, lang)
      translations = Enum.into(content_schema, %{}, &{&1["definition"], &1["name"]})

      {df_name, %{template: template, translations: translations, content_schema: content_schema}}
    end)
  end
end
