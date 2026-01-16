-- name: InsertJob :one
INSERT INTO goqueue_jobs (queue_name, created_at, status, scheduled_at, arguments, max_retries, retry_policy)
VALUES ($1, NOW(), 'available', $2, $3, $4, $5)
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
    error = $1
WHERE job_id = $2
RETURNING *;

-- name: UpdateJobFinished :one
UPDATE goqueue_jobs
SET
    status = 'finished',
    finished_at = NOW()
WHERE job_id = $1
RETURNING *;

-- name: FetchJob :one
UPDATE goqueue_jobs AS j
SET
    status = 'running',
    retry_attempt = j.retry_attempt + 1,
    started_at = NOW()
WHERE job_id = (
    SELECT job_id
    FROM goqueue_jobs AS j2
    WHERE j2.queue_name = $1
      AND j2.status = 'available'
      AND j2.scheduled_at <= NOW()
    ORDER BY j2.created_at
    LIMIT 1
)
RETURNING j.*;

-- name: FetchJobLocked :one
UPDATE goqueue_jobs AS j
SET
    status = 'running',
    retry_attempt = j.retry_attempt + 1,
    started_at = NOW()
WHERE job_id = (
    SELECT job_id
    FROM goqueue_jobs AS j2
    WHERE j2.queue_name = $1
      AND j2.status = 'available'
      AND j2.scheduled_at <= NOW()
    ORDER BY j2.created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING j.*;

-- name: RescheduleJob :one
UPDATE goqueue_jobs
SET
    status = 'available',
    scheduled_at = $1
WHERE job_id = $2
RETURNING *;

-- name: MoveJobToDLQ :one
UPDATE goqueue_jobs
SET
    queue_name = $1,
    status = 'available',
    retry_attempt = 0,
    scheduled_at = NOW()
WHERE job_id = $2
RETURNING *;