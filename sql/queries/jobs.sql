-- name: InsertJob :one
INSERT INTO goqueue_jobs (created_at, finished_at, status, error, arguments)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: UpdateJob :one
UPDATE goqueue_jobs
SET created_at = $1,
    finished_at = $2,
    status = $3,
    error = $4,
    arguments = $5
WHERE job_id = $6
RETURNING *;

-- name: GetJob :one
SELECT *
FROM goqueue_jobs
WHERE status = 'available'
LIMIT 1;
