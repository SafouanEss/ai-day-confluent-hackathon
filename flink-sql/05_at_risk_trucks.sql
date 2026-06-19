-- Roll up signal-level anomalies into a per-truck "at risk" view.
-- A truck is escalated when multiple distinct signals trip within a 5-minute
-- window. This is the table a dispatcher dashboard or maintenance routing
-- agent would subscribe to.

CREATE TABLE at_risk_trucks AS
SELECT
    truck_id,
    window_start,
    window_end,
    COUNT(DISTINCT signal)                                                       AS signals_tripped,
    LISTAGG(DISTINCT signal, ', ')                                               AS tripped_signals,
    MAX(CASE WHEN signal = 'engine_temp_c'       THEN observed_value END)        AS engine_temp_c,
    MAX(CASE WHEN signal = 'oil_pressure_psi'    THEN observed_value END)        AS oil_pressure_psi,
    MAX(CASE WHEN signal = 'vibration_g'         THEN observed_value END)        AS vibration_g,
    MAX(CASE WHEN signal = 'tire_psi_avg'        THEN observed_value END)        AS tire_psi_avg,
    MAX(CASE WHEN signal = 'fuel_efficiency_mpg' THEN observed_value END)        AS fuel_efficiency_mpg
FROM TABLE(
    TUMBLE(TABLE truck_anomalies, DESCRIPTOR(`window_time`), INTERVAL '5' MINUTE)
)
GROUP BY truck_id, window_start, window_end
HAVING COUNT(DISTINCT signal) >= 2;
