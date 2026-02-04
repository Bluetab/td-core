defmodule TdCore.XLSX.UploadWorker do
  @moduledoc """
  Processes XLSX file uploads based on job scope, dispatching to `TdCore.XLSX.BulkLoad` and tracking job status events.
  """
  require Logger

  alias TdCluster.Cluster.TdAudit.UploadJobs
  alias TdCore.Auth.Claims
  alias TdCore.XLSX.BulkLoad
  alias TdCore.XLSX.Reader

  def run(%{
        "path" => path,
        "job_id" => job_id,
        "scope" => scope,
        "opts" => %{
          "lang" => lang,
          "auto_publish" => auto_publish,
          "claims" => claims
        }
      }) do
    impl_for =
      :td_core
      |> Application.get_env(:bulk_load_implementations)
      |> Map.get(scope)
      |> struct

    ctx = %{
      job_id: job_id,
      impl_for: impl_for,
      lang: lang,
      claims: Claims.coerce(claims),
      to_status: if(auto_publish == "true", do: "published", else: "draft")
    }

    UploadJobs.create_started(job_id)

    with {:ok, sheets} <- Reader.read(path),
         {:ok, result} <- BulkLoad.bulk_load(sheets, ctx) do
      UploadJobs.create_completed(job_id, result)
    else
      {:error, reason} -> UploadJobs.create_failed(job_id, reason)
    end
  rescue
    e ->
      Logger.error("Error running upload worker for file: #{path}, error: #{inspect(e)}")
      UploadJobs.create_failed(job_id, inspect(e))
  end
end
