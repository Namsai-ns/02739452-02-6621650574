from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import re
import time
import xml.etree.ElementTree as ET

import requests
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Representative coordinates for the provincial capital / main urban area of
# each Thai province. These are approximate city-center coordinates, not
# official province centroids. They avoid relying on geocoding Thai names.
PROVINCE_COORDS = {
    "กรุงเทพมหานคร": (13.7563, 100.5018),
    "กระบี่": (8.0863, 98.9063),
    "กาญจนบุรี": (14.0228, 99.5328),
    "กาฬสินธุ์": (16.4328, 103.5066),
    "กำแพงเพชร": (16.4828, 99.5227),
    "ขอนแก่น": (16.4419, 102.8350),
    "จันทบุรี": (12.6114, 102.1039),
    "ฉะเชิงเทรา": (13.6904, 101.0779),
    "ชลบุรี": (13.3611, 100.9847),
    "ชัยนาท": (15.1852, 100.1251),
    "ชัยภูมิ": (15.8068, 102.0315),
    "ชุมพร": (10.4930, 99.1800),
    "เชียงราย": (19.9105, 99.8406),
    "เชียงใหม่": (18.7883, 98.9853),
    "ตรัง": (7.5594, 99.6114),
    "ตราด": (12.2428, 102.5175),
    "ตาก": (16.8839, 99.1258),
    "นครนายก": (14.2069, 101.2131),
    "นครปฐม": (13.8199, 100.0443),
    "นครพนม": (17.3920, 104.7696),
    "นครราชสีมา": (14.9799, 102.0978),
    "นครศรีธรรมราช": (8.4304, 99.9631),
    "นครสวรรค์": (15.7047, 100.1372),
    "นนทบุรี": (13.8621, 100.5144),
    "นราธิวาส": (6.4255, 101.8253),
    "น่าน": (18.7756, 100.7730),
    "บึงกาฬ": (18.3609, 103.6466),
    "บุรีรัมย์": (14.9930, 103.1029),
    "ปทุมธานี": (14.0208, 100.5250),
    "ประจวบคีรีขันธ์": (11.8030, 99.8000),
    "ปราจีนบุรี": (14.0500, 101.3720),
    "ปัตตานี": (6.8695, 101.2505),
    "พระนครศรีอยุธยา": (14.3532, 100.5689),
    "พะเยา": (19.1664, 99.9019),
    "พังงา": (8.4509, 98.5253),
    "พัทลุง": (7.6167, 100.0740),
    "พิจิตร": (16.4418, 100.3488),
    "พิษณุโลก": (16.8211, 100.2659),
    "เพชรบุรี": (13.1119, 99.9447),
    "เพชรบูรณ์": (16.4190, 101.1606),
    "แพร่": (18.1446, 100.1403),
    "ภูเก็ต": (7.8804, 98.3923),
    "มหาสารคาม": (16.1851, 103.3026),
    "มุกดาหาร": (16.5425, 104.7235),
    "แม่ฮ่องสอน": (19.3013, 97.9685),
    "ยโสธร": (15.7926, 104.1453),
    "ยะลา": (6.5411, 101.2804),
    "ร้อยเอ็ด": (16.0567, 103.6531),
    "ระนอง": (9.9529, 98.6085),
    "ระยอง": (12.6814, 101.2816),
    "ราชบุรี": (13.5283, 99.8134),
    "ลพบุรี": (14.7995, 100.6534),
    "ลำปาง": (18.2888, 99.4909),
    "ลำพูน": (18.5745, 99.0087),
    "เลย": (17.4860, 101.7223),
    "ศรีสะเกษ": (15.1186, 104.3220),
    "สกลนคร": (17.1546, 104.1348),
    "สงขลา": (7.1897, 100.5954),
    "สตูล": (6.6238, 100.0674),
    "สมุทรปราการ": (13.5991, 100.5998),
    "สมุทรสงคราม": (13.4098, 100.0023),
    "สมุทรสาคร": (13.5475, 100.2744),
    "สระแก้ว": (13.8240, 102.0646),
    "สระบุรี": (14.5289, 100.9101),
    "สิงห์บุรี": (14.8936, 100.3967),
    "สุโขทัย": (17.0078, 99.8230),
    "สุพรรณบุรี": (14.4745, 100.1177),
    "สุราษฎร์ธานี": (9.1401, 99.3331),
    "สุรินทร์": (14.8829, 103.4937),
    "หนองคาย": (17.8785, 102.7413),
    "หนองบัวลำภู": (17.2041, 102.4407),
    "อ่างทอง": (14.5896, 100.4551),
    "อำนาจเจริญ": (15.8657, 104.6258),
    "อุดรธานี": (17.4138, 102.7872),
    "อุตรดิตถ์": (17.6201, 100.0993),
    "อุทัยธานี": (15.3794, 100.0245),
    "อุบลราชธานี": (15.2287, 104.8564),
}

PROVINCES = list(PROVINCE_COORDS.keys())

# Keep the original five cities for wttr.in so existing source behavior remains.
CITIES = [
    {"name": "Bangkok", "province": "กรุงเทพมหานคร", "lat": 13.7563, "lon": 100.5018},
    {"name": "Chiang Mai", "province": "เชียงใหม่", "lat": 18.7883, "lon": 98.9853},
    {"name": "Phuket", "province": "ภูเก็ต", "lat": 7.8804, "lon": 98.3923},
    {"name": "Khon Kaen", "province": "ขอนแก่น", "lat": 16.4419, "lon": 102.8350},
    {"name": "Nakhon Ratchasima", "province": "นครราชสีมา", "lat": 14.9799, "lon": 102.0978},
]
TIMEOUT = 20


def db_connect():
    return psycopg2.connect(
        host=os.getenv("WEATHER_DB_HOST", "postgres"),
        port=int(os.getenv("WEATHER_DB_PORT", "5432")),
        dbname=os.getenv("WEATHER_DB_NAME", "airflow"),
        user=os.getenv("WEATHER_DB_USER", "airflow"),
        password=os.getenv("WEATHER_DB_PASSWORD", "airflow"),
        connect_timeout=8,
    )


def init_schema():
    sql = """
    CREATE TABLE IF NOT EXISTS weather_observations (
        id BIGSERIAL PRIMARY KEY,
        source TEXT NOT NULL,
        location TEXT NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        temperature_c DOUBLE PRECISION,
        humidity_pct DOUBLE PRECISION,
        rainfall_mm DOUBLE PRECISION,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        weather_description TEXT,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_weather_source_location_time UNIQUE (source, location, observed_at)
    );
    CREATE INDEX IF NOT EXISTS idx_weather_time ON weather_observations(observed_at);
    CREATE INDEX IF NOT EXISTS idx_weather_location ON weather_observations(location);
    CREATE TABLE IF NOT EXISTS weather_rejected (
        id BIGSERIAL PRIMARY KEY,
        source TEXT,
        raw_record JSONB,
        reject_reason TEXT NOT NULL,
        rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        id BIGSERIAL PRIMARY KEY,
        dag_run_id TEXT NOT NULL,
        source TEXT NOT NULL,
        status TEXT NOT NULL,
        records_received INTEGER NOT NULL DEFAULT 0,
        records_loaded INTEGER NOT NULL DEFAULT 0,
        error_message TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (dag_run_id, source)
    );
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    return "schema ready"


def parse_float(value):
    if value is None or value == "":
        return None
    try:
        x = float(value)
        return x if x == x and abs(x) != float("inf") else None
    except (ValueError, TypeError):
        return None


def iso_time(value):
    if not value:
        return datetime.now(timezone.utc).isoformat()
    try:
        s = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def fetch_open_meteo():
    """Fetch current Open-Meteo conditions for all 77 provinces by fixed coordinates."""
    records = []
    failed = []

    for province, (lat, lon) in PROVINCE_COORDS.items():
        try:
            response = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "current": "temperature_2m,relative_humidity_2m,precipitation",
                    "timezone": "UTC",
                },
                timeout=TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            current = payload.get("current") or {}

            if not current:
                failed.append(f"{province}: no current data")
                continue

            temperature = parse_float(current.get("temperature_2m"))
            humidity = parse_float(current.get("relative_humidity_2m"))
            rainfall = parse_float(current.get("precipitation"))

            if all(value is None for value in (temperature, humidity, rainfall)):
                failed.append(f"{province}: all measurements are null")
                continue

            # Use the API's returned coordinates when available.
            actual_lat = parse_float(payload.get("latitude"))
            actual_lon = parse_float(payload.get("longitude"))

            records.append({
                "source": "open_meteo",
                "location": province,
                "observed_at": iso_time(current.get("time")),
                "temperature_c": temperature,
                "humidity_pct": humidity,
                "rainfall_mm": rainfall,
                "latitude": actual_lat if actual_lat is not None else lat,
                "longitude": actual_lon if actual_lon is not None else lon,
                "weather_description": "Current weather (Open-Meteo model estimate)",
            })
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            print(f"[open_meteo] {province} failed: {message}")
            failed.append(f"{province}: {message}")

        # Gentle pacing to avoid making rapid consecutive API requests.
        time.sleep(0.10)

    print(f"Open-Meteo province coverage: {len(records)}/{len(PROVINCES)}")
    if failed:
        print(f"Open-Meteo unavailable provinces ({len(failed)}): {failed}")

    if not records:
        raise RuntimeError(
            "Open-Meteo returned no province records. Check the task log for per-province errors."
        )

    return records


def fetch_wttr():
    records = []
    for c in CITIES:
        response = requests.get(
            f"https://wttr.in/{c['lat']},{c['lon']}?format=j1",
            headers={"User-Agent": "WeatherFusionStudentProject/1.0"},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        current = (payload.get("current_condition") or [{}])[0]
        records.append({
            "source": "wttr_in",
            "location": c["province"],
            "observed_at": iso_time(current.get("localObsDateTime")),
            "temperature_c": parse_float(current.get("temp_C")),
            "humidity_pct": parse_float(current.get("humidity")),
            "rainfall_mm": parse_float(current.get("precipMM")),
            "latitude": c["lat"],
            "longitude": c["lon"],
            "weather_description": str(((current.get("weatherDesc") or [{}])[0]).get("value", ""))[:250],
        })
        time.sleep(0.4)
    return records


def walk_dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from walk_dicts(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from walk_dicts(value)


def first_value(d, keys):
    normalized = {re.sub(r"[^a-z0-9]", "", str(k).lower()): v for k, v in d.items()}
    for key in keys:
        k = re.sub(r"[^a-z0-9]", "", key.lower())
        if k in normalized and normalized[k] not in (None, ""):
            return normalized[k]
    return None


def fetch_tmd():
    url = os.getenv(
        "TMD_API_URL",
        "https://data.tmd.go.th/api/WeatherToday/V2/?uid=api&ukey=api12345&format=json",
    )
    response = requests.get(url, headers={"Accept": "application/json"}, timeout=TIMEOUT)
    response.raise_for_status()
    try:
        payload = response.json()
    except Exception:
        try:
            payload = json.loads(response.text)
        except Exception:
            root = ET.fromstring(response.text)
            payload = {child.tag: child.text for child in root.iter() if child.text and child.text.strip()}

    records = []
    now = datetime.now(timezone.utc).isoformat()

    for item in walk_dicts(payload):
        temp = parse_float(first_value(item, ["temperature", "temp", "temperaturec", "tempC", "t2m"]))
        if temp is None:
            continue
        location = first_value(item, ["province", "provinceName", "stationName", "station", "amphoe", "location", "name"])
        if not location:
            continue
        hum = parse_float(first_value(item, ["humidity", "relativeHumidity", "rh"]))
        rain = parse_float(first_value(item, ["rainfall", "rain", "rainfall24h", "precipitation", "precipMM"]))
        lat = parse_float(first_value(item, ["latitude", "lat"]))
        lon = parse_float(first_value(item, ["longitude", "lon", "lng"]))
        observed = first_value(item, ["datetime", "dateTime", "observationTime", "time", "date"])
        records.append({
            "source": "tmd",
            "location": str(location).strip()[:160],
            "observed_at": iso_time(observed or now),
            "temperature_c": temp,
            "humidity_pct": hum,
            "rainfall_mm": rain,
            "latitude": lat,
            "longitude": lon,
            "weather_description": "TMD observation",
        })

    if not records:
        raise ValueError(
            "TMD API returned no records that could be mapped to weather data; check TMD_API_URL/schema"
        )

    unique = {}
    for record in records:
        unique[(record["location"], record["observed_at"])] = record
    return list(unique.values())[:500]


def extract_source(source, ti):
    enabled = os.getenv({
        "open_meteo": "ENABLE_OPEN_METEO",
        "wttr_in": "ENABLE_WTTR",
        "tmd": "ENABLE_TMD",
    }[source], "true").lower() in ("1", "true", "yes", "on")

    dag_run_id = ti.run_id
    try:
        if not enabled:
            raise RuntimeError(f"{source} disabled via environment variable")

        records = {
            "open_meteo": fetch_open_meteo,
            "wttr_in": fetch_wttr,
            "tmd": fetch_tmd,
        }[source]()

        status = "success" if records else "empty"
        error = None if records else "No records returned"
        print(f"[{source}] status={status}; records_received={len(records)}")
    except Exception as exc:
        print(f"[{source}] ERROR: {type(exc).__name__}: {exc}")
        records, status, error = [], "failed", str(exc)[:1000]

    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs
                    (dag_run_id, source, status, records_received, records_loaded, error_message, updated_at)
                VALUES (%s,%s,%s,%s,0,%s,NOW())
                ON CONFLICT (dag_run_id, source) DO UPDATE SET
                    status=EXCLUDED.status,
                    records_received=EXCLUDED.records_received,
                    error_message=EXCLUDED.error_message,
                    updated_at=NOW()
            """, (dag_run_id, source, status, len(records), error))

    return {"source": source, "status": status, "records": records, "error": error}


def combine_and_load(ti):
    results = [
        ti.xcom_pull(task_ids="open_meteo_group.extract"),
        ti.xcom_pull(task_ids="wttr_group.extract"),
        ti.xcom_pull(task_ids="tmd_group.extract"),
    ]

    records = []
    source_status = []
    for result in results:
        if not result:
            continue
        source_status.append((
            result["source"],
            result["status"],
            len(result.get("records", [])),
            result.get("error"),
        ))
        records.extend(result.get("records", []))

    valid, rejected = [], []
    for record in records:
        reason = None
        if not record.get("location") or not record.get("observed_at"):
            reason = "missing location or observed_at"
        elif record.get("temperature_c") is not None and not (-90 <= record["temperature_c"] <= 65):
            reason = "temperature outside -90..65 C"
        elif record.get("humidity_pct") is not None and not (0 <= record["humidity_pct"] <= 100):
            reason = "humidity outside 0..100%"
        elif record.get("rainfall_mm") is not None and not (0 <= record["rainfall_mm"] <= 2000):
            reason = "rainfall outside 0..2000 mm"
        elif all(record.get(key) is None for key in ("temperature_c", "humidity_pct", "rainfall_mm")):
            reason = "all weather measurements are null"

        if reason:
            rejected.append((
                record.get("source"),
                json.dumps(record, ensure_ascii=False),
                reason,
            ))
        else:
            valid.append(record)

    # Remove exact duplicates within the same source.
    deduped = {}
    for record in valid:
        key = (
            record["source"],
            record["location"].strip().lower(),
            record["observed_at"],
        )
        deduped[key] = record
    valid = list(deduped.values())

    # Collapse only near-identical cross-source observations for the same
    # location and timestamp. Different weather values remain separate.
    priority = {"tmd": 0, "open_meteo": 1, "wttr_in": 2}
    valid.sort(key=lambda record: priority.get(record["source"], 99))
    cross_deduped = []

    for record in valid:
        duplicate = False
        for old in cross_deduped:
            if old["location"].strip().lower() != record["location"].strip().lower():
                continue

            try:
                old_time = datetime.fromisoformat(old["observed_at"].replace("Z", "+00:00"))
                new_time = datetime.fromisoformat(record["observed_at"].replace("Z", "+00:00"))
                minutes = abs((old_time - new_time).total_seconds()) / 60
            except Exception:
                minutes = 9999

            if minutes <= 10:
                same_temp = (
                    old["temperature_c"] is not None
                    and record["temperature_c"] is not None
                    and abs(old["temperature_c"] - record["temperature_c"]) <= 0.2
                )
                same_humidity = (
                    old["humidity_pct"] is not None
                    and record["humidity_pct"] is not None
                    and abs(old["humidity_pct"] - record["humidity_pct"]) <= 2
                )
                if same_temp and same_humidity:
                    duplicate = True
                    break

        if not duplicate:
            cross_deduped.append(record)

    with db_connect() as conn:
        with conn.cursor() as cur:
            if rejected:
                execute_values(
                    cur,
                    """
                    INSERT INTO weather_rejected (source, raw_record, reject_reason)
                    VALUES %s
                    """,
                    [(source, json.loads(raw), reason) for source, raw, reason in rejected],
                )

            if cross_deduped:
                values = [(
                    record["source"],
                    record["location"],
                    record["observed_at"],
                    record.get("temperature_c"),
                    record.get("humidity_pct"),
                    record.get("rainfall_mm"),
                    record.get("latitude"),
                    record.get("longitude"),
                    record.get("weather_description"),
                ) for record in cross_deduped]

                execute_values(
                    cur,
                    """
                    INSERT INTO weather_observations
                        (source, location, observed_at, temperature_c, humidity_pct,
                         rainfall_mm, latitude, longitude, weather_description)
                    VALUES %s
                    ON CONFLICT (source, location, observed_at) DO UPDATE SET
                        temperature_c=EXCLUDED.temperature_c,
                        humidity_pct=EXCLUDED.humidity_pct,
                        rainfall_mm=EXCLUDED.rainfall_mm,
                        latitude=COALESCE(EXCLUDED.latitude, weather_observations.latitude),
                        longitude=COALESCE(EXCLUDED.longitude, weather_observations.longitude),
                        weather_description=EXCLUDED.weather_description,
                        loaded_at=NOW()
                    """,
                    values,
                )

            for source, status, received, error in source_status:
                loaded = sum(1 for record in cross_deduped if record["source"] == source)
                cur.execute(
                    """
                    UPDATE pipeline_runs
                    SET records_loaded=%s, updated_at=NOW()
                    WHERE dag_run_id=%s AND source=%s
                    """,
                    (loaded, ti.run_id, source),
                )

            pipeline_status = "success" if cross_deduped else "no_data"
            pipeline_error = "; ".join(
                f"{source}:{status}" + (f" ({error})" if error else "")
                for source, status, _, error in source_status
            )

            cur.execute(
                """
                INSERT INTO pipeline_runs
                    (dag_run_id, source, status, records_received, records_loaded, error_message)
                VALUES (%s, 'pipeline', %s, %s, %s, %s)
                ON CONFLICT (dag_run_id, source) DO UPDATE SET
                    status=EXCLUDED.status,
                    records_received=EXCLUDED.records_received,
                    records_loaded=EXCLUDED.records_loaded,
                    error_message=EXCLUDED.error_message,
                    updated_at=NOW()
                """,
                (ti.run_id, pipeline_status, len(records), len(cross_deduped), pipeline_error),
            )

    print(
        f"Pipeline summary: received={len(records)}, "
        f"loaded={len(cross_deduped)}, rejected={len(rejected)}"
    )
    print(f"Source summary: {source_status}")

    if not cross_deduped and not any(status == "failed" for _, status, _, _ in source_status):
        return {
            "loaded": 0,
            "rejected": len(rejected),
            "source_status": source_status,
        }

    return {
        "loaded": len(cross_deduped),
        "rejected": len(rejected),
        "source_status": source_status,
    }


with DAG(
    dag_id="weather_multi_source_pipeline",
    description="Collect weather from multiple APIs, normalize, quality-check and UPSERT PostgreSQL",
    start_date=datetime(2025, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    default_args={"owner": "student", "retries": 0},
    tags=["weather", "ETL", "PostgreSQL"],
) as dag:
    create_schema = PythonOperator(
        task_id="create_schema",
        python_callable=init_schema,
    )

    with TaskGroup(group_id="open_meteo_group") as open_meteo_group:
        om_extract = PythonOperator(
            task_id="extract",
            python_callable=extract_source,
            op_kwargs={"source": "open_meteo"},
        )

    with TaskGroup(group_id="wttr_group") as wttr_group:
        wttr_extract = PythonOperator(
            task_id="extract",
            python_callable=extract_source,
            op_kwargs={"source": "wttr_in"},
        )

    with TaskGroup(group_id="tmd_group") as tmd_group:
        tmd_extract = PythonOperator(
            task_id="extract",
            python_callable=extract_source,
            op_kwargs={"source": "tmd"},
        )

    combine = PythonOperator(
        task_id="combine_quality_deduplicate_upsert",
        python_callable=combine_and_load,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_schema >> [open_meteo_group, wttr_group, tmd_group] >> combine
