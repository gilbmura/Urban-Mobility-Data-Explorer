import os
import csv
import io
from datetime import datetime
from typing import Dict, Any, List, Tuple

from flask import Flask, jsonify, request, send_file
from dotenv import load_dotenv
import MySQLdb

from algorithms import top_k_by_tip_percentage


load_dotenv()

def get_db_connection():
    return MySQLdb.connect(
        host=os.getenv('DB_HOST', '127.0.0.1'),
        user=os.getenv('DB_USER', 'nyc_user'),
        passwd=os.getenv('DB_PASSWORD', 'nyc_pass'),
        db=os.getenv('DB_NAME', 'nyc_mobility'),
        charset='utf8mb4'
    )


app = Flask(__name__)


@app.get('/health')
def health():
    return jsonify({"status": "ok"})


def parse_date(s: str) -> datetime:
    return datetime.fromisoformat(s)


@app.get('/stats/summary')
def stats_summary():
    params = request.args
    from_dt = params.get('from')
    to_dt = params.get('to')

    where = []
    args: List[Any] = []
    if from_dt:
        where.append("pickup_datetime >= %s")
        args.append(from_dt)
    if to_dt:
        where.append("pickup_datetime <= %s")
        args.append(to_dt)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
    SELECT COUNT(*) AS trips,
           AVG(speed_kmh) AS avg_speed_kmh,
           AVG(fare_per_km) AS avg_fare_per_km,
           AVG(duration_min) AS avg_duration_min
    FROM trips {where_sql}
    """

    conn = get_db_connection()
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql, args)
        row = cur.fetchone()
        return jsonify(row)
    finally:
        conn.close()


@app.get('/trips')
def list_trips():
    params = request.args
    limit = min(int(params.get('limit', 50)), 500)
    offset = int(params.get('offset', 0))
    conn = get_db_connection()
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT * FROM trips ORDER BY pickup_datetime DESC LIMIT %s OFFSET %s", (limit, offset))
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()


@app.get('/aggregations/hourly')
def aggregations_hourly():
    params = request.args
    from_dt = params.get('from')
    to_dt = params.get('to')
    where = []
    args: List[Any] = []
    if from_dt:
        where.append("pickup_datetime >= %s")
        args.append(from_dt)
    if to_dt:
        where.append("pickup_datetime <= %s")
        args.append(to_dt)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
    SELECT DATE_FORMAT(pickup_datetime, '%%Y-%%m-%%d %%H:00:00') AS hour,
           COUNT(*) AS trips,
           AVG(speed_kmh) AS avg_speed
    FROM trips {where_sql}
    GROUP BY hour
    ORDER BY hour
    """

    conn = get_db_connection()
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql, args)
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()


@app.get('/insights/top_tipped')
def insights_top_tipped():
    params = request.args
    limit = min(int(params.get('limit', 20)), 200)
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT trip_id, fare_amount, tip_amount FROM trips WHERE fare_amount > 0 AND tip_amount >= 0")
        heap_items: List[Tuple[float, int, float, float]] = []
        for trip_id, fare_amount, tip_amount in cur.fetchall():
            tip_pct = float(tip_amount) / float(fare_amount)
            top_k_by_tip_percentage(heap_items, limit, (tip_pct, trip_id, float(fare_amount), float(tip_amount)))

        # Extract sorted desc by tip_pct
        result = sorted(heap_items, key=lambda x: x[0], reverse=True)
        payload = [
            {"trip_id": item[1], "tip_pct": round(item[0]*100, 2), "fare_amount": item[2], "tip_amount": item[3]}
            for item in result
        ]
        return jsonify(payload)
    finally:
        conn.close()


def validate_and_transform_row(row: Dict[str, str]) -> Tuple[Dict[str, Any], List[str]]:
    reasons: List[str] = []
    try:
        pickup = datetime.fromisoformat(row['pickup_datetime'])
        dropoff = datetime.fromisoformat(row['dropoff_datetime'])
    except Exception:
        reasons.append('invalid_timestamp')
        return {}, reasons
    if dropoff <= pickup:
        reasons.append('non_positive_duration')
        return {}, reasons

    def to_float(val: str, key: str, min_v: float = None, max_v: float = None) -> float:
        try:
            x = float(val)
        except Exception:
            reasons.append(f'invalid_{key}')
            return 0.0
        if min_v is not None and x < min_v:
            reasons.append(f'out_of_range_{key}')
        if max_v is not None and x > max_v:
            reasons.append(f'out_of_range_{key}')
        return x

    pickup_lat = to_float(row.get('pickup_lat', ''), 'pickup_lat', -90, 90)
    pickup_lng = to_float(row.get('pickup_lng', ''), 'pickup_lng', -180, 180)
    dropoff_lat = to_float(row.get('dropoff_lat', ''), 'dropoff_lat', -90, 90)
    dropoff_lng = to_float(row.get('dropoff_lng', ''), 'dropoff_lng', -180, 180)
    distance_km = to_float(row.get('distance_km', ''), 'distance_km', 0.0, 200.0)
    duration_min = to_float(row.get('duration_min', ''), 'duration_min', 0.01, 24*60)
    fare_amount = to_float(row.get('fare_amount', ''), 'fare_amount', 0.0, None)
    tip_amount = to_float(row.get('tip_amount', ''), 'tip_amount', 0.0, None)

    if any(k.startswith('invalid_') or k.startswith('out_of_range_') for k in reasons):
        return {}, reasons

    return {
        'vendor_id': row.get('vendor_id') or None,
        'pickup_datetime': pickup.strftime('%Y-%m-%d %H:%M:%S'),
        'dropoff_datetime': dropoff.strftime('%Y-%m-%d %H:%M:%S'),
        'pickup_lat': round(pickup_lat, 6),
        'pickup_lng': round(pickup_lng, 6),
        'dropoff_lat': round(dropoff_lat, 6),
        'dropoff_lng': round(dropoff_lng, 6),
        'distance_km': round(distance_km, 3),
        'duration_min': round(duration_min, 3),
        'fare_amount': round(fare_amount, 2),
        'tip_amount': round(tip_amount, 2),
        'payment_type': row.get('payment_type') or None
    }, reasons


@app.post('/process')
def process_file():
    if 'file' not in request.files:
        return jsonify({"error": "file missing"}), 400
    f = request.files['file']
    stream = io.TextIOWrapper(f.stream, encoding='utf-8')
    reader = csv.DictReader(stream)

    output = io.StringIO()
    fieldnames = [
        'vendor_id','pickup_datetime','dropoff_datetime','pickup_lat','pickup_lng','dropoff_lat','dropoff_lng',
        'distance_km','duration_min','fare_amount','tip_amount','payment_type'
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    kept = 0
    excluded = 0
    for row in reader:
        clean_row, reasons = validate_and_transform_row(row)
        if reasons:
            excluded += 1
            continue
        writer.writerow(clean_row)
        kept += 1

    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name='cleaned.csv',
        etag=False
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)


