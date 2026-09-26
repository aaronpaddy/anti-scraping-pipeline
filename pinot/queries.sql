-- Run in the Pinot query console at http://localhost:9000

-- Alerts per minute, by action
SELECT DATETRUNC('MINUTE', evaluated_at) AS minute, action_taken, COUNT(*) AS alerts
FROM telemetry_anomaly_alerts
GROUP BY minute, action_taken
ORDER BY minute DESC
LIMIT 60;

-- Action breakdown and why each fired
SELECT action_taken, geographic_velocity_triggered, ai_score_available, COUNT(*) AS alerts
FROM telemetry_anomaly_alerts
GROUP BY action_taken, geographic_velocity_triggered, ai_score_available;

-- p50 / p99 processing time of alerted events
SELECT PERCENTILETDIGEST(pipeline_processing_duration_ms, 50) AS p50_ms,
       PERCENTILETDIGEST(pipeline_processing_duration_ms, 99) AS p99_ms
FROM telemetry_anomaly_alerts;

-- Most-alerted users
SELECT viewer_user_id, COUNT(*) AS alerts, MAX(ai_scraping_anomaly_score) AS max_score
FROM telemetry_anomaly_alerts
GROUP BY viewer_user_id
ORDER BY alerts DESC
LIMIT 20;
