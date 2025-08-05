-- redis_records_raw: Store raw Redis key-value records parsed from RDB
CREATE TABLE IF NOT EXISTS redis_records_raw
(
    `cluster`      String COMMENT 'Logical name of the Redis cluster',
    `batch`        DateTime64(9, 'UTC') COMMENT 'Unique timestamp identifier for the data import batch (nanosecond precision)',
    `instance`     String COMMENT 'Physical address of the Redis instance, e.g., IP:Port',
    `db`           UInt64 COMMENT 'Redis database number (e.g., 0-15)',
    `key`          String COMMENT 'Redis key name',
    `type`         LowCardinality(String) COMMENT 'Redis data structure type (e.g., string, hash, list)',
    `member_count` UInt64 COMMENT 'Number of elements in the data structure',
    `rdb_size`     UInt64 COMMENT 'Bytes occupied by this record in the RDB file',
    `encoding`     LowCardinality(String) COMMENT 'RDB internal encoding type',
    `expire_at`    Nullable(DateTime64(3, 'UTC')) COMMENT 'Absolute expiration timestamp of the key (UTC)',
    `idle_seconds` Nullable(UInt64) COMMENT 'LRU idle time of the key (seconds)',
    `freq`         Nullable(UInt8) COMMENT 'LFU access frequency of the key',
    `codis_slot`   Nullable(UInt16) COMMENT 'Codis Slot ID (0-1023), only for Codis clusters',
    `redis_slot` Nullable(UInt16) COMMENT 'Redis Cluster Slot ID (0-16383), only for Redis Cluster'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(batch)
ORDER BY (cluster, batch, instance, db, key)
TTL toDateTime(batch) + INTERVAL 3 DAY
COMMENT 'Raw data table storing all Redis records parsed from RDB' 