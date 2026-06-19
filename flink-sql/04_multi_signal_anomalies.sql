

CREATE TABLE truck_anomalies (
    truck_id STRING,
    window_time TIMESTAMP(3),
    signal STRING,
    observed_value DOUBLE,
    expected_value DOUBLE,
    upper_bound DOUBLE,
    lower_bound DOUBLE,
    deviation DOUBLE,
    direction STRING,
    WATERMARK FOR window_time AS window_time - INTERVAL '5' SECOND
) AS
WITH windowed_telemetry AS (
    SELECT
        window_start,
        window_end,
        CAST(window_time AS TIMESTAMP(3)) AS window_time,
        truck_id,
        AVG(engine_temp_c)       AS engine_temp_c,
        AVG(oil_pressure_psi)    AS oil_pressure_psi,
        AVG(vibration_g)         AS vibration_g,
        AVG(tire_psi_avg)        AS tire_psi_avg,
        AVG(brake_pad_mm)        AS brake_pad_mm,
        AVG(fuel_efficiency_mpg) AS fuel_efficiency_mpg
    FROM TABLE(
        TUMBLE(TABLE truck_telemetry, DESCRIPTOR(`reading_ts`), INTERVAL '1' MINUTE)
    )
    GROUP BY window_start, window_end, window_time, truck_id
),
egt AS (
    SELECT truck_id, CAST(window_time AS TIMESTAMP(3)) AS window_time, 'engine_temp_c' AS signal, engine_temp_c AS observed_value,
           ML_DETECT_ANOMALIES(
               CAST(engine_temp_c AS DOUBLE), window_time,
               JSON_OBJECT('min_training_size' VALUE 200, 'max_training_size' VALUE 7000,
                           'confidence_percentage' VALUE 99.9, 'enable_stl' VALUE FALSE)
           ) OVER (PARTITION BY truck_id ORDER BY window_time
                   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r
    FROM windowed_telemetry
),
oil AS (
    SELECT truck_id, CAST(window_time AS TIMESTAMP(3)) AS window_time, 'oil_pressure_psi' AS signal, oil_pressure_psi AS observed_value,
           ML_DETECT_ANOMALIES(
               CAST(oil_pressure_psi AS DOUBLE), window_time,
               JSON_OBJECT('min_training_size' VALUE 200, 'max_training_size' VALUE 7000,
                           'confidence_percentage' VALUE 99.9, 'enable_stl' VALUE FALSE)
           ) OVER (PARTITION BY truck_id ORDER BY window_time
                   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r
    FROM windowed_telemetry
),
vib AS (
    SELECT truck_id, CAST(window_time AS TIMESTAMP(3)) AS window_time, 'vibration_g' AS signal, vibration_g AS observed_value,
           ML_DETECT_ANOMALIES(
               CAST(vibration_g AS DOUBLE), window_time,
               JSON_OBJECT('min_training_size' VALUE 200, 'max_training_size' VALUE 7000,
                           'confidence_percentage' VALUE 99.9, 'enable_stl' VALUE FALSE)
           ) OVER (PARTITION BY truck_id ORDER BY window_time
                   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r
    FROM windowed_telemetry
),
tire AS (
    SELECT truck_id, CAST(window_time AS TIMESTAMP(3)) AS window_time, 'tire_psi_avg' AS signal, tire_psi_avg AS observed_value,
           ML_DETECT_ANOMALIES(
               CAST(tire_psi_avg AS DOUBLE), window_time,
               JSON_OBJECT('min_training_size' VALUE 200, 'max_training_size' VALUE 7000,
                           'confidence_percentage' VALUE 99.9, 'enable_stl' VALUE FALSE)
           ) OVER (PARTITION BY truck_id ORDER BY window_time
                   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r
    FROM windowed_telemetry
),
fuel AS (
    SELECT truck_id, CAST(window_time AS TIMESTAMP(3)) AS window_time, 'fuel_efficiency_mpg' AS signal, fuel_efficiency_mpg AS observed_value,
           ML_DETECT_ANOMALIES(
               CAST(fuel_efficiency_mpg AS DOUBLE), window_time,
               JSON_OBJECT('min_training_size' VALUE 200, 'max_training_size' VALUE 7000,
                           'confidence_percentage' VALUE 99.9, 'enable_stl' VALUE FALSE)
           ) OVER (PARTITION BY truck_id ORDER BY window_time
                   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r
    FROM windowed_telemetry
)
SELECT truck_id, CAST(window_time AS TIMESTAMP(3)) AS window_time, signal, observed_value,
       CAST(ROUND(r.forecast_value, 2) AS DOUBLE) AS expected_value,
       r.upper_bound, r.lower_bound,
       observed_value - r.forecast_value AS deviation,
       CASE 
         WHEN observed_value > r.upper_bound THEN 'HIGH' 
         WHEN observed_value < r.lower_bound THEN 'LOW'
         ELSE 'OUTLIER' 
       END AS direction
FROM egt WHERE r.is_anomaly = TRUE
UNION ALL
SELECT truck_id, CAST(window_time AS TIMESTAMP(3)) AS window_time, signal, observed_value,
       CAST(ROUND(r.forecast_value, 2) AS DOUBLE), r.upper_bound, r.lower_bound,
       observed_value - r.forecast_value,
       CASE 
         WHEN observed_value > r.upper_bound THEN 'HIGH' 
         WHEN observed_value < r.lower_bound THEN 'LOW'
         ELSE 'OUTLIER' 
       END
FROM oil WHERE r.is_anomaly = TRUE
UNION ALL
SELECT truck_id, CAST(window_time AS TIMESTAMP(3)) AS window_time, signal, observed_value,
       CAST(ROUND(r.forecast_value, 2) AS DOUBLE), r.upper_bound, r.lower_bound,
       observed_value - r.forecast_value,
       CASE 
         WHEN observed_value > r.upper_bound THEN 'HIGH' 
         WHEN observed_value < r.lower_bound THEN 'LOW'
         ELSE 'OUTLIER' 
       END
FROM vib WHERE r.is_anomaly = TRUE
UNION ALL
SELECT truck_id, CAST(window_time AS TIMESTAMP(3)) AS window_time, signal, observed_value,
       CAST(ROUND(r.forecast_value, 2) AS DOUBLE), r.upper_bound, r.lower_bound,
       observed_value - r.forecast_value,
       CASE 
         WHEN observed_value > r.upper_bound THEN 'HIGH' 
         WHEN observed_value < r.lower_bound THEN 'LOW'
         ELSE 'OUTLIER' 
       END
FROM tire WHERE r.is_anomaly = TRUE
UNION ALL
SELECT truck_id, CAST(window_time AS TIMESTAMP(3)) AS window_time, signal, observed_value,
       CAST(ROUND(r.forecast_value, 2) AS DOUBLE), r.upper_bound, r.lower_bound,
       observed_value - r.forecast_value,
       CASE 
         WHEN observed_value > r.upper_bound THEN 'HIGH' 
         WHEN observed_value < r.lower_bound THEN 'LOW'
         ELSE 'OUTLIER' 
       END
FROM fuel WHERE r.is_anomaly = TRUE;
