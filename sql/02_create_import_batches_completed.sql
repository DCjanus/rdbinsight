-- import_batches_completed: Record successfully completed import batch information for atomic snapshot reads
CREATE TABLE IF NOT EXISTS import_batches_completed
(
    `cluster` String COMMENT 'Logical name of the Redis cluster',
    `batch`   DateTime64(9, 'UTC') COMMENT 'Unique timestamp identifier for the data import batch (nanosecond precision)'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(batch)
ORDER BY (cluster, batch)
TTL toDateTime(batch) + INTERVAL 3 DAY
COMMENT 'Batch completion marker table for completed data imports' 