
import os
from flask import Flask, jsonify, render_template, request
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)


def db_connect():
    """Connect to PostgreSQL using environment variables."""
    return psycopg2.connect(
        host=os.getenv("WEATHER_DB_HOST", "localhost"),
        port=int(os.getenv("WEATHER_DB_PORT", "5432")),
        dbname=os.getenv("WEATHER_DB_NAME", "airflow"),
        user=os.getenv("WEATHER_DB_USER", "airflow"),
        password=os.getenv("WEATHER_DB_PASSWORD", "airflow"),
        connect_timeout=5,
    )


def query(sql, params=()):
    """Execute a SQL query and return rows as dictionaries."""
    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)

            if cur.description:
                return [dict(row) for row in cur.fetchall()]

            return []


def json_safe(rows):
    """Convert dates, timestamps and Decimal values to JSON-safe values."""
    from decimal import Decimal

    for row in rows:
        for key, value in list(row.items()):
            if hasattr(value, "isoformat"):
                row[key] = value.isoformat()
            elif isinstance(value, Decimal):
                row[key] = float(value)

    return rows



def build_filters():
    where = []
    params = []

    location = request.args.get("location", "").strip()
    source = request.args.get("source", "").strip()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()

    # กรองจังหวัด โดยไม่สนตัวพิมพ์เล็ก/ใหญ่
    if location:
        where.append("TRIM(location) = %s")
        params.append(location)

    # กรองแหล่งข้อมูล
    if source:
        where.append("LOWER(TRIM(source)) = LOWER(%s)")
        params.append(source)

    # กรองตั้งแต่วันที่เริ่มต้น
    if date_from:
        where.append("observed_at >= %s::date")
        params.append(date_from)

    # รวมข้อมูลตลอดวันที่สิ้นสุด
    if date_to:
        where.append("observed_at < (%s::date + INTERVAL '1 day')")
        params.append(date_to)

    clause = ""
    if where:
        clause = " WHERE " + " AND ".join(where)

    return clause, params



@app.route("/")
def index():
    try:
        return render_template("index.html")
    except Exception:
        app.logger.exception("Unable to render dashboard page")
        return "Weather Dashboard is starting. Refresh in a few seconds.", 200


@app.route("/api/filters")
def filters():
    try:
        locations = query("""
            SELECT DISTINCT location
            FROM weather_observations
            WHERE location IS NOT NULL
            ORDER BY location
        """)

        sources = query("""
            SELECT DISTINCT source
            FROM weather_observations
            WHERE source IS NOT NULL
            ORDER BY source
        """)

        return jsonify({
            "locations": [row["location"] for row in locations],
            "sources": [row["source"] for row in sources],
        })

    except Exception as exc:
        app.logger.exception("Failed to load dashboard filters")
        return jsonify({
            "error": str(exc),
            "locations": [],
            "sources": [],
        }), 200


@app.route("/api/dashboard")
def dashboard_data():
    try:
        clause, params = build_filters()

        # KPI summary
        kpis = query(f"""
            SELECT
                COUNT(*) AS records,
                COUNT(DISTINCT location) AS locations,
                ROUND(AVG(temperature_c)::numeric, 1) AS avg_temp,
                ROUND(
                    COALESCE(SUM(rainfall_mm), 0)::numeric,
                    1
                ) AS rainfall_sum,
                COUNT(DISTINCT source) AS sources
            FROM weather_observations
            {clause}
        """, params)

        # Detailed records for the data table
        rows = query(f"""
            SELECT
                source,
                location,
                observed_at,
                temperature_c,
                humidity_pct,
                rainfall_mm,
                weather_description,
                loaded_at
            FROM weather_observations
            {clause}
            ORDER BY observed_at DESC, location, source
            LIMIT 500
        """, params)

        # Chart 1: temperature and rainfall trend over time.
        # Records observed within the same hour are averaged together.
        time_chart = query(f"""
            SELECT
                date_trunc('hour', observed_at) AS hour,
                ROUND(AVG(temperature_c)::numeric, 1) AS temperature,
                ROUND(
                    AVG(rainfall_mm)::numeric,
                    2
                ) AS rainfall
            FROM weather_observations
            {clause}
            GROUP BY date_trunc('hour', observed_at)
            ORDER BY hour
        """, params)

        # Chart 2: compare temperatures between locations.
        # Each province/location gets its own result.
        province_chart = query(f"""
            SELECT
                location,
                ROUND(AVG(temperature_c)::numeric, 1) AS temperature,
                ROUND(
                    AVG(rainfall_mm)::numeric,
                    2
                ) AS rainfall,
                COUNT(*) AS records
            FROM weather_observations
            {clause}
            GROUP BY location
            ORDER BY temperature DESC NULLS LAST, location
            LIMIT 100
        """, params)

        return jsonify({
            "kpis": json_safe(kpis)[0] if kpis else {},
            "rows": json_safe(rows),
            "chart": json_safe(time_chart),
            "province_chart": json_safe(province_chart),
        })

    except Exception as exc:
        app.logger.exception("Failed to load dashboard data")

        return jsonify({
            "error": str(exc),
            "kpis": {},
            "rows": [],
            "chart": [],
            "province_chart": [],
        }), 200


@app.route("/api/pipeline")
def pipeline():
    try:
        rows = query("""
            SELECT
                dag_run_id,
                source,
                status,
                records_received,
                records_loaded,
                error_message,
                updated_at
            FROM pipeline_runs
            ORDER BY updated_at DESC
            LIMIT 30
        """)

        return jsonify(json_safe(rows))

    except Exception as exc:
        app.logger.exception("Failed to load pipeline status")

        return jsonify({
            "error": str(exc),
            "rows": [],
        }), 200


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
    )
