-- name: UpsertNotebookModelLink :exec
INSERT INTO notebook_model_links (id, notebook_id, model_id, output_cell_id)
VALUES (?, ?, ?, ?)
ON CONFLICT(notebook_id) DO UPDATE SET
    model_id = excluded.model_id,
    output_cell_id = excluded.output_cell_id,
    updated_at = datetime('now');

-- name: GetNotebookModelLinkByNotebookID :one
SELECT * FROM notebook_model_links WHERE notebook_id = ?;

-- name: GetNotebookModelLinkByModelID :one
SELECT * FROM notebook_model_links WHERE model_id = ?;

-- name: DeleteNotebookModelLinkByNotebookID :exec
DELETE FROM notebook_model_links WHERE notebook_id = ?;
