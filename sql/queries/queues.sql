-- name: LockQueue :one
SELECT pg_try_advisory_xact_lock(
    hashtext($1)::bigint
) AS acquired;

-- name: InsertQueue :one
INSERT INTO goqueue_queues (queue_name, is_fifo)
VALUES ($1, $2)
ON CONFLICT (queue_name) DO NOTHING
RETURNING *;

-- name: GetQueue :one
SELECT *
FROM goqueue_queues
WHERE queue_name = $1
LIMIT 1;
