-- Visualize engine_temp_c anomalies in the Flink UI.
-- Run this in a SQL workspace, then click on the `anomaly_result` column graph.
-- Expected: degrading trucks (TRK-112, TRK-207, TRK-356, TRK-418) show a
-- rising trend that crosses the upper bound; healthy trucks stay inside it.

WITH windowed_telemetry AS (
    SELECT
        window_start,
        window_end,
        window_time,
        truck_id,
        AVG(engine_temp_c)       AS avg_engine_temp_c,
        MAX(engine_temp_c)       AS max_engine_temp_c,
        AVG(oil_pressure_psi)    AS avg_oil_pressure_psi,
        AVG(vibration_g)         AS avg_vibration_g,
        COUNT(*)                 AS reading_count
    FROM TABLE(
        TUMBLE(TABLE truck_telemetry, DESCRIPTOR(`reading_ts`), INTERVAL '1' MINUTE)
    )
    GROUP BY window_start, window_end, window_time, truck_id
)
SELECT
    truck_id,
    window_time,
    avg_engine_temp_c,
    max_engine_temp_c,
    avg_oil_pressure_psi,
    avg_vibration_g,
    reading_count,
    ML_DETECT_ANOMALIES(
        CAST(avg_engine_temp_c AS DOUBLE),
        window_time,
        JSON_OBJECT(
            'minTrainingSize'      VALUE 200,
            'maxTrainingSize'      VALUE 7000,
            'confidencePercentage' VALUE 99.9,
            'enableStl'            VALUE FALSE
        )
    ) OVER (
        PARTITION BY truck_id
        ORDER BY window_time
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS anomaly_result
FROM windowed_telemetry;
