-- Continuous invocation of the maintenance_dispatch_agent.
--
-- Every new row in at_risk_trucks_diagnosed triggers the agent. The agent
-- looks up nearby shops via http_get, decides the dispatch, posts it via
-- http_post, and returns a three-section response that we parse into
-- structured columns and persist to the `completed_actions` topic.

CREATE TABLE truck_completed_actions (
    truck_id STRING NOT NULL,
    window_start TIMESTAMP(3),
    severity STRING,
    likely_cause STRING,
    recommended_action STRING,
    dispatch_summary STRING,
    dispatch_json STRING,
    api_response STRING,
    raw_response STRING,
    PRIMARY KEY (truck_id) NOT ENFORCED
)
WITH ('changelog.mode' = 'append')
AS SELECT
    COALESCE(truck_id, 'UNKNOWN') AS truck_id,
    window_start,
    severity,
    likely_cause,
    recommended_action,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING),
        '\*{0,2}Dispatch Summary:\*{0,2}\s*\n([\s\S]+?)(?=\n\n\*{0,2}Dispatch JSON:\*{0,2})', 1)) AS dispatch_summary,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING),
        '\*{0,2}Dispatch JSON:\*{0,2}\s*\n(?:```json\s*)?([\s\S]+?)(?:```)?(?=\n\n\*{0,2}API Response:\*{0,2})', 1)) AS dispatch_json,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING),
        '\*{0,2}API Response:\*{0,2}\s*\n(?:```json\s*)?([\s\S]+?)(?:```)?$', 1)) AS api_response,
    CAST(response AS STRING) AS raw_response
FROM at_risk_trucks_diagnosed,
LATERAL TABLE(AI_RUN_AGENT(
    'maintenance_dispatch_agent',
    CONCAT(
        'truck_id: ', truck_id,
        ', severity: ', COALESCE(severity, 'UNKNOWN'),
        ', likely_cause: ', COALESCE(likely_cause, ''),
        ', recommended_action: ', COALESCE(recommended_action, ''),
        ', engine_temp_c: ', CAST(engine_temp_c AS STRING),
        ', oil_pressure_psi: ', CAST(oil_pressure_psi AS STRING),
        ', vibration_g: ', CAST(vibration_g AS STRING),
        ', tire_psi_avg: ', CAST(tire_psi_avg AS STRING),
        ', fuel_efficiency_mpg: ', CAST(fuel_efficiency_mpg AS STRING)
    ),
    CONCAT(truck_id, '_', CAST(window_start AS STRING))
));
