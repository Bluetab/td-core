defmodule TdCore.XLSX.Upload do
  @moduledoc """
  Shared XLSX upload endpoint flow.

  `accept/2` fixes the order in which an upload is accepted, so that every entity
  behaves the same way:

    1. Authorization (`:authorize`, injected by the entity).
    2. Hash of the uploaded file (`TdCore.Utils.FileHash`, md5).
    3. Job creation in td-audit (`create_job`).
    4. Job set to pending (`create_pending`).
    5. Move of the uploaded file to the upload directory (`TdCore.XLSX.Reader.move_file!/2`).
    6. Enqueue of the Oban job (`:enqueue`, injected because td-core does not depend on Oban).

  If the enqueue fails, the job is marked as failed with the message
  `"job enqueue failed"` and the moved file is removed.

  If the move fails, the job is marked as failed with the message
  `"file move failed"`, any partial destination file is removed, and the error is
  returned without enqueueing.

  Failures before the move (authorization, `create_job`, `create_pending`) return
  `{:error, reason}` without moving the file or enqueueing. Authorization failures
  also happen before any job is created. If `create_pending` fails, the job is
  marked as failed.

  ## Options

    * `:authorize` (required) — zero-arity function. Returns `:ok` or `true` to allow;
      `false` maps to `{:error, :forbidden}`; `{:error, reason}` is returned as is.
    * `:enqueue` (required) — one-arity function that receives the job args
      (`%{path:, job_id:, scope:, opts:}`, atom keys) and returns `{:ok, _}` or
      `{:error, _}`. The consumer builds and inserts its Oban job here, for example
      `fn args -> args |> MyWorker.new() |> Oban.insert() end`.
    * `:upload_dir` (required) — directory where the file is moved to.
    * `:audit` — module exposing `create_job/1`, `create_pending/1` and
      `create_failed/2`. Defaults to `TdCluster.Cluster.TdAudit.UploadJobs`.
    * `:hash_fn` — one-arity function receiving the path and returning the hash.
      Defaults to `TdCore.Utils.FileHash.hash(path, :md5)`.
    * `:move_fn` — function `(path, upload_dir -> new_path)`. Defaults to
      `TdCore.XLSX.Reader.move_file!/2`. If it raises or exits, the upload is
      reported as a move failure.

  ## Upload

  `upload` is a map with the keys `:path` (temporary file), `:filename`, `:user_id`,
  `:scope` and `:opts`. `:opts` is passed as is to the job args and is expected to
  contain the entity options (for example `lang`, `auto_publish` and `claims`).
  """

  alias TdCore.Utils.FileHash
  alias TdCore.XLSX.Reader

  @default_audit TdCluster.Cluster.TdAudit.UploadJobs
  @enqueue_failed_message "job enqueue failed"
  @move_failed_message "file move failed"

  @typedoc "Upload description. See the module documentation."
  @type upload :: %{
          required(:path) => String.t(),
          required(:filename) => String.t(),
          required(:user_id) => term(),
          required(:scope) => String.t(),
          required(:opts) => map()
        }

  @doc """
  Accepts an XLSX upload following the common order (authorize, hash, create job,
  create pending, move file, enqueue).

  Returns `{:ok, job_id}` when the job has been enqueued, or `{:error, reason}`.
  See `accept_with_details/2` to also get the hash of the file and the result of
  the enqueue.

  ## Examples

      Upload.accept(upload, authorize: fn -> can?(claims, xlsx_upload(Concept)) end,
                            enqueue: &enqueue_concepts/1,
                            upload_dir: "/var/lib/td/uploads")
  """
  @spec accept(upload(), keyword()) :: {:ok, integer() | term()} | {:error, term()}
  def accept(upload, opts) do
    with {:ok, %{job_id: job_id}} <- accept_with_details(upload, opts), do: {:ok, job_id}
  end

  @doc """
  Same flow as `accept/2`, but returns a map with the `:job_id` of the td-audit
  upload job, the `:hash` of the file (computed after the authorization) and the
  `:enqueued` value returned by the `:enqueue` function (for example the inserted
  Oban job).
  """
  @spec accept_with_details(upload(), keyword()) ::
          {:ok, %{job_id: term(), hash: term(), enqueued: term()}} | {:error, term()}
  def accept_with_details(
        %{path: path, filename: filename, user_id: user_id, scope: scope} = upload,
        opts
      ) do
    authorize = Keyword.fetch!(opts, :authorize)
    enqueue = Keyword.fetch!(opts, :enqueue)
    upload_dir = Keyword.fetch!(opts, :upload_dir)
    audit = Keyword.get(opts, :audit, @default_audit)
    hash_fn = Keyword.get(opts, :hash_fn, &FileHash.hash(&1, :md5))
    move_fn = Keyword.get(opts, :move_fn, &Reader.move_file!/2)

    with :ok <- authorize(authorize),
         hash = hash_fn.(path),
         {:ok, %{id: job_id}} <-
           audit.create_job(%{
             user_id: user_id,
             hash: hash,
             filename: filename,
             scope: scope
           }),
         {:ok, _} <- create_pending(audit, job_id),
         {:ok, file_path} <- safe_move(audit, job_id, move_fn, path, upload_dir) do
      job_args = %{
        path: file_path,
        job_id: job_id,
        scope: scope,
        opts: Map.get(upload, :opts, %{})
      }

      case enqueue.(job_args) do
        {:ok, enqueued} ->
          {:ok, %{job_id: job_id, hash: hash, enqueued: enqueued}}

        {:error, reason} ->
          audit.create_failed(job_id, @enqueue_failed_message)
          File.rm(file_path)
          {:error, reason}
      end
    end
  end

  defp authorize(authorize) do
    case authorize.() do
      :ok -> :ok
      true -> :ok
      false -> {:error, :forbidden}
      {:error, reason} -> {:error, reason}
    end
  end

  # The move runs after the job is pending, so any failure must mark the job as
  # failed and remove the partial destination file before returning. Exits are
  # caught too, since a move that exits would otherwise leave the job pending.
  defp safe_move(audit, job_id, move_fn, path, upload_dir) do
    {:ok, move_fn.(path, upload_dir)}
  rescue
    exception -> move_failed(audit, job_id, upload_dir, path, exception)
  catch
    kind, reason -> move_failed(audit, job_id, upload_dir, path, {kind, reason})
  end

  defp move_failed(audit, job_id, upload_dir, path, reason) do
    audit.create_failed(job_id, @move_failed_message)
    File.rm(Path.join(upload_dir, Path.basename(path)))
    {:error, reason}
  end

  # create_pending failures are reported as the job failure, as the entity
  # controllers already do. The reason is normalized so that td-audit can always
  # encode it as the failure message.
  defp create_pending(audit, job_id) do
    case audit.create_pending(job_id) do
      {:ok, _} = ok ->
        ok

      {:error, reason} ->
        audit.create_failed(job_id, failure_message(reason))
        {:error, reason}

      other ->
        other
    end
  end

  defp failure_message(reason) when is_binary(reason) or is_atom(reason), do: reason
  defp failure_message(reason), do: inspect(reason)
end
