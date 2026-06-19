-- Continuous Flink job that filters down to confirmed engine-temperature anomalies
-- per truck. Output table `engine_temp_anomalies` is the trigger feed that a
-- downstream agent (RAG enrichment, dispatcher tool calls) would consume.

CREATE TABLE engine_temp_anomalies AS
WITH windowed_telemetry AS (
    SELECT
        window_start,
        window_end,
        window_time,
        truck_id,
        AVG(engine_temp_c) AS avg_engine_temp_c,
        MAX(engine_temp_c) AS max_engine_temp_c,
        COUNT(*)           AS reading_count
    FROM TABLE(
        TUMBLE(TABLE truck_telemetry, DESCRIPTOR(`reading_ts`), INTERVAL '1' MINUTE)
    )
    GROUP BY window_start, window_end, window_time, truck_id
),
anomaly_detection AS (
    SELECT
        truck_id,
        window_time,
        avg_engine_temp_c,
        max_engine_temp_c,
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
    FROM windowed_telemetry
)
SELECT
    truck_id,
    window_time,
    'engine_temp_c'                                  AS signal,
    avg_engine_temp_c                                AS observed_value,
    CAST(ROUND(anomaly_result.forecast_value, 2) AS DOUBLE) AS expected_value,
    anomaly_result.upper_bound                       AS upper_bound,
    anomaly_result.lower_bound                       AS lower_bound,
    avg_engine_temp_c - anomaly_result.forecast_value AS deviation,
    anomaly_result.is_anomaly                        AS is_anomaly
FROM anomaly_detection
WHERE anomaly_result.is_anomaly = TRUE
  AND avg_engine_temp_c > anomaly_result.upper_bound;
