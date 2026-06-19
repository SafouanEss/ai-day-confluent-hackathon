-- Enrich each at-risk truck with an LLM-generated diagnosis + severity tier
-- using ML_PREDICT against the text-generation model created in the parent
-- project's core terraform (`llm_textgen_model`).
--
-- The prompt is constructed entirely in Flink SQL from the structured
-- `at_risk_trucks` row, so this is genuinely streaming inference — every new
-- escalation produces a fresh diagnosis with no external orchestration.

CREATE TABLE at_risk_trucks_diagnosed
WITH (
    'changelog.mode' = 'append'
)
AS SELECT
    arr.truck_id,
    arr.window_start,
    arr.window_end,
    arr.signals_tripped,
    arr.tripped_signals,
    arr.engine_temp_c,
    arr.oil_pressure_psi,
    arr.vibration_g,
    arr.tire_psi_avg,
    arr.fuel_efficiency_mpg,
    TRIM(REGEXP_EXTRACT(CAST(llm.response AS STRING), '(?i)SEVERITY:\s*(CRITICAL|HIGH|MEDIUM|LOW)', 1)) AS severity,
    TRIM(REGEXP_EXTRACT(CAST(llm.response AS STRING), '(?i)LIKELY_CAUSE:\s*([\s\S]+?)(?=\n\s*(?:RECOMMENDED_ACTION|$))', 1)) AS likely_cause,
    TRIM(REGEXP_EXTRACT(CAST(llm.response AS STRING), '(?i)RECOMMENDED_ACTION:\s*([\s\S]+?)$', 1)) AS recommended_action,
    CAST(llm.response AS STRING) AS raw_diagnosis
FROM at_risk_trucks AS arr,
LATERAL TABLE(ML_PREDICT(
    'llm_textgen_model',
    CONCAT(
        'You are a senior diesel-truck maintenance engineer. Diagnose the ',
        'following multi-signal anomaly and respond in EXACTLY this format ',
        '(no extra text):\n\n',
        'SEVERITY: <CRITICAL|HIGH|MEDIUM|LOW>\n',
        'LIKELY_CAUSE: <one sentence root cause hypothesis>\n',
        'RECOMMENDED_ACTION: <one sentence concrete action>\n\n',
        'Truck: ', CAST(arr.truck_id AS STRING), '\n',
        'Signals tripped: ', arr.tripped_signals, ' (', CAST(arr.signals_tripped AS STRING), ' distinct signals)\n',
        'Engine temperature (C): ', COALESCE(CAST(ROUND(arr.engine_temp_c, 1) AS STRING), 'n/a'), ' (healthy baseline ~88)\n',
        'Oil pressure (psi): ', COALESCE(CAST(ROUND(arr.oil_pressure_psi, 1) AS STRING), 'n/a'), ' (healthy baseline ~52)\n',
        'Vibration (g): ', COALESCE(CAST(ROUND(arr.vibration_g, 3) AS STRING), 'n/a'), ' (healthy baseline ~0.32)\n',
        'Tire pressure (psi): ', COALESCE(CAST(ROUND(arr.tire_psi_avg, 1) AS STRING), 'n/a'), ' (healthy baseline ~102)\n',
        'Fuel efficiency (mpg): ', COALESCE(CAST(ROUND(arr.fuel_efficiency_mpg, 2) AS STRING), 'n/a'), ' (healthy baseline ~7.2)\n\n',
        'Reason about the combination of signals. Severity should be CRITICAL only if there is imminent risk of breakdown within ~50 miles.'
    )
)) AS llm;

