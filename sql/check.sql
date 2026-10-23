-- ตรวจจำนวนแถวและจำนวนแหล่งข้อมูล
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT source) AS source_count
FROM weather_observations;

-- ตรวจข้อมูลล่าสุด
SELECT source, location, observed_at, temperature_c, humidity_pct, rainfall_mm
FROM weather_observations
ORDER BY observed_at DESC
LIMIT 30;

-- ตรวจสถานะ pipeline
SELECT dag_run_id, source, status, records_received, records_loaded, error_message, updated_at
FROM pipeline_runs
ORDER BY updated_at DESC
LIMIT 30;

-- ตรวจข้อมูลที่ไม่ผ่าน quality check
SELECT source, reject_reason, COUNT(*)
FROM weather_rejected
GROUP BY source, reject_reason
ORDER BY COUNT(*) DESC;
