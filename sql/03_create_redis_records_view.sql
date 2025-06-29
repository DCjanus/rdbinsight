-- redis_records_view: Safe query view that only exposes confirmed completed import data
CREATE OR REPLACE VIEW redis_records_view AS
SELECT
    *
FROM
    redis_records_raw
WHERE
    (cluster, batch) IN (
        SELECT
            cluster,
            batch
        FROM
            import_batches_completed
    ) 