-- name: InsertJob :one
INSERT INTO goqueue_jobs (created_at, status, arguments, max_retries, retry_policy)
VALUES (NOW(), 'available', $1, $2, $3)
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

-- name: UpdateJobStatus :one
UPDATE goqueue_jobs
SET status = $1
WHERE job_id = $2
RETURNING *;

-- name: UpdateJobFailed :one
UPDATE goqueue_jobs
SET
    status = 'failed',
    next_retry_at = $1,
    error = $2
WHERE job_id = $3
RETURNING *;

-- name: UpdateJobFinished :one
UPDATE goqueue_jobs
SET
    status = 'finished',
    finished_at = NOW()
WHERE job_id = $1
RETURNING *;

-- name: FetchJobLocked :one
UPDATE goqueue_jobs
SET
    status = 'pending',
    started_at = NOW()
WHERE job_id = (
    SELECT job_id
    FROM goqueue_jobs
    WHERE status = 'available'
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: FetchRescheduableJobsLocked :many
SELECT * FROM goqueue_jobs
WHERE status = 'failed'
  AND retry_attempt <= max_retries
  AND NOW() >= next_retry_at
ORDER BY created_at
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: RescheduleJob :one
UPDATE goqueue_jobs
SET
    status = 'available',
    retry_attempt = $1
WHERE job_id = $2
RETURNING *;