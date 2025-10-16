import os
from typing import Any, List, Tuple

from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
from dotenv import load_dotenv
import MySQLdb

from algorithms import top_k_by_tip_percentage


# load_dotenv()

def get_db_connection():
    """Get database connection with error handling."""
    try:
        return MySQLdb.connect(
            host=os.getenv('DB_HOST', '127.0.0.1'),
            user=os.getenv('DB_USER', 'nyc_user'),
            passwd=os.getenv('DB_PASSWORD', 'nyc_pass'),
            db=os.getenv('DB_NAME', 'nyc_mobility'),
            port=int(os.getenv('DB_PORT', 3306)),
            charset='utf8mb4'
        )
    except MySQLdb.Error as e:
        print(f"Database connection error: {e}")
        return None

app = Flask(__name__, template_folder='../templates', static_folder='../static')
CORS(app)  # Enable CORS for all routes

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.get('/health')
def health():
    return jsonify({"status": "ok"})


# TESTING

@app.route('/trips', methods=['GET'])
def api_trips():
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        # Get query parameters
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        limit = int(request.args.get('limit', 1000000))  # Default limit for dashboard
        
        # Build SQL query with date filtering
        where_conditions = []
        params = []
        
        if start_date:
            where_conditions.append("DATE(pickup_datetime) >= %s")
            params.append(start_date)
        
        if end_date:
            where_conditions.append("DATE(pickup_datetime) <= %s")
            params.append(end_date)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        sql = f"""
        SELECT trip_id, vendor_id, pickup_datetime, dropoff_datetime, 
               pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
               distance_km, duration_min, fare_amount, tip_amount, payment_type,
               speed_kmh, fare_per_km, hour_of_day, day_of_week, rush_hour, is_weekend
        FROM trips {where_clause}
        ORDER BY pickup_datetime DESC
        LIMIT %s
        """
        params.append(limit)
        
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        
        return jsonify(rows)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# Removed unused parse_date function


@app.get('/stats/summary')
def stats_summary():
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        params = request.args
        from_dt = params.get('from') or params.get('start')
        to_dt = params.get('to') or params.get('end')

        where = []
        args: List[Any] = []
        if from_dt:
            where.append("DATE(pickup_datetime) >= %s")
            args.append(from_dt)
        if to_dt:
            where.append("DATE(pickup_datetime) <= %s")
            args.append(to_dt)
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        sql = f"""
        SELECT COUNT(*) AS trips,
               AVG(speed_kmh) AS avg_speed_kmh,
               AVG(fare_per_km) AS avg_fare_per_km,
               AVG(duration_min) AS avg_duration_min,
               AVG(fare_amount) AS avg_fare_amount,
               AVG(distance_km) AS avg_distance_km
        FROM trips {where_sql}
        """

        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql, args)
        row = cur.fetchone()
        cur.close()
        return jsonify(row)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# @app.get('/trips')
# def list_trips():
#     params = request.args
#     # limit = min(int(params.get('limit', 50)), 500)
#     # offset = int(params.get('offset', 0))
    
#     conn = get_db_connection()
#     if not conn:
#         return jsonify({"error": "Database connection failed"}), 500
    
#     try:
#         cur = conn.cursor(MySQLdb.cursors.DictCursor)
#         cur.execute("SELECT * FROM trips ORDER BY pickup_datetime DESC LIMIT 10 OFFSET 0")
#         rows = cur.fetchall()
#         return jsonify(rows)
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500
#     finally:
#         conn.close()


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
    FROM trips{where_sql}
    GROUP BY hour
    ORDER BY hour
    """

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql, args)
        rows = cur.fetchall()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.get('/insights/top_tipped')
def insights_top_tipped():
    params = request.args
    limit = min(int(params.get('limit', 20)), 200)
    
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
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
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# File processing removed - use scripts/simple_loader.py instead


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)


