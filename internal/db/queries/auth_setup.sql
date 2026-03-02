-- name: GetSetupState :one
SELECT * FROM setup_state WHERE id = 1;

-- name: CompleteSetupState :exec
UPDATE setup_state
SET setup_completed = 1,
    setup_completed_at = CURRENT_TIMESTAMP,
    setup_completed_by = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE id = 1;

-- name: SetSetupBootstrapToken :exec
UPDATE setup_state
SET bootstrap_token_hash = ?,
    bootstrap_token_expires_at = ?,
    updated_at = CURRENT_TIMESTAMP
WHERE id = 1;

-- name: ClearSetupBootstrapToken :exec
UPDATE setup_state
SET bootstrap_token_hash = NULL,
    bootstrap_token_expires_at = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE id = 1;
