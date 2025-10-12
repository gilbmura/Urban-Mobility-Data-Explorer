import os
import io
import csv
import zipfile
import argparse
from datetime import datetime
from typing import Dict, Any, List, Tuple, Iterable
import math
import math, itertools, sys, traceback
from MySQLdb import IntegrityError
import MySQLdb


def connect_db(host: str, user: str, password: str, name: str):
    return MySQLdb.connect(host=host, user=user, passwd=password, db=name, charset='utf8mb4')


def iter_csv_rows_from_input(input_path: str) -> Iterable[Dict[str, str]]:
    if os.path.isdir(input_path):
        for root, _, files in os.walk(input_path):
            for fn in files:
                if fn.lower().endswith('.csv'):
                    with open(os.path.join(root, fn), 'r', encoding='utf-8') as f:
                        yield from csv.DictReader(f)
    elif input_path.lower().endswith('.zip'):
        with zipfile.ZipFile(input_path, 'r') as zf:
            for info in zf.infolist():
                if info.filename.lower().endswith('.csv'):
                    with zf.open(info, 'r') as f:
                        text = io.TextIOWrapper(f, encoding='utf-8')
                        yield from csv.DictReader(text)
    else:
        with open(input_path, 'r', encoding='utf-8') as f:
            yield from csv.DictReader(f)


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on earth in kilometers"""
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Radius of earth in kilometers
    r = 6371
    return c * r


def validate_and_transform_row(row: Dict[str, str]) -> Tuple[Dict[str, Any], List[str]]:
    reasons: List[str] = []
    
    def parse_dt(val: str) -> datetime:
        # Accept common formats
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%m/%d/%Y %H:%M:%S'):
            try:
                return datetime.strptime(val, fmt)
            except Exception:
                continue
        return datetime.fromisoformat(val)

    try:
        pickup = parse_dt(row.get('pickup_datetime', ''))
        dropoff = parse_dt(row.get('dropoff_datetime', ''))
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

    # Use the actual column names from the data
    pickup_lat = to_float(row.get('pickup_latitude', ''), 'pickup_lat', -90, 90)
    pickup_lng = to_float(row.get('pickup_longitude', ''), 'pickup_lng', -180, 180)
    dropoff_lat = to_float(row.get('dropoff_latitude', ''), 'dropoff_lat', -90, 90)
    dropoff_lng = to_float(row.get('dropoff_longitude', ''), 'dropoff_lng', -180, 180)
    
    # Convert trip_duration from seconds to minutes
    trip_duration_seconds = to_float(row.get('trip_duration', ''), 'trip_duration', 1, 24*3600)  # 1 second to 24 hours
    duration_min = trip_duration_seconds / 60.0
    
    # Calculate distance using coordinates
    try:
        distance_km = haversine_distance(pickup_lat, pickup_lng, dropoff_lat, dropoff_lng)
    except:
        distance_km = 0.0
        reasons.append('invalid_coordinates')
    
    # Set default values for missing fields
    fare_amount = 10.0  # Default fare - you might want to calculate this based on distance/duration
    tip_amount = 0.0    # Default tip
    payment_type = 'unknown'  # Default payment type

    if any(k.startswith('invalid_') or k.startswith('out_of_range_') for k in reasons):
        return {}, reasons

    # Filter out unrealistic trips
    if distance_km > 100:  # More than 100km
        reasons.append('unrealistic_distance')
    if duration_min > 180:  # More than 3 hours
        reasons.append('unrealistic_duration')
    if distance_km < 0.1 and duration_min > 10:  # Very short distance but long time
        reasons.append('suspicious_trip')

    if reasons:
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
        'payment_type': payment_type
    }, reasons


def write_cleaned_csv(rows: Iterable[Dict[str, Any]], output_path: str) -> int:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    count = 0
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['vendor_id','pickup_datetime','dropoff_datetime','pickup_lat','pickup_lng','dropoff_lat','dropoff_lng','distance_km','duration_min','fare_amount','tip_amount','payment_type']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
            count += 1
    return count


# def upsert_missing_parents(conn, kept_rows):
#     cur = conn.cursor()
#     # vendors
#     csv_vendor_ids = sorted({r['vendor_id'] for r in kept_rows if r.get('vendor_id')})
#     if csv_vendor_ids:
#         # fetch existing
#         cur.execute("SELECT vendor_id FROM vendors WHERE vendor_id IN (%s)" %
#                     ",".join(["%s"]*len(csv_vendor_ids)), csv_vendor_ids)
#         existing = {row[0] for row in cur.fetchall()}
#         to_insert = [v for v in csv_vendor_ids if v not in existing]
#         if to_insert:
#             cur.executemany("INSERT IGNORE INTO vendors (vendor_id, name) VALUES (%s, %s)",
#                             [(v, 'unknown') for v in to_insert])
#     # payment_types (same pattern)
#     cur.close()
#     conn.commit()



# def load_into_mysql(rows: Iterable[Dict[str, Any]], conn) -> int:
#     cur = conn.cursor()
#     sql = (
#         "INSERT INTO trips (vendor_id, pickup_datetime, dropoff_datetime, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, distance_km, duration_min, fare_amount, tip_amount, payment_type) "
#         "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#     )
#     batch: List[Tuple] = []
#     total = 0
#     for r in rows:
#         batch.append((r['vendor_id'], r['pickup_datetime'], r['dropoff_datetime'], r['pickup_lat'], r['pickup_lng'], r['dropoff_lat'], r['dropoff_lng'], r['distance_km'], r['duration_min'], r['fare_amount'], r['tip_amount'], r['payment_type']))
#         if len(batch) >= 2000:
#             cur.executemany(sql, batch)
#             conn.commit()
#             total += len(batch)
#             print(f"Loaded {total} records so far...")
#             batch.clear()
#     if batch:
#         cur.executemany(sql, batch)
#         conn.commit()
#         total += len(batch)
#     return total


# place near top of file with your other imports
# import csv, math, itertools, sys, traceback
# from MySQLdb import IntegrityError

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            return
        yield chunk

def upsert_parents_for_batch(cur, batch_rows):
    """Ensure vendor and payment_type parents exist for this batch.
       cur is a MySQLdb cursor (not the connection)."""
    # vendors
    vendor_ids = sorted({ (r.get('vendor_id') or '').strip() for r in batch_rows if r.get('vendor_id') })
    vendor_ids = [v for v in vendor_ids if v != '']
    if vendor_ids:
        # fetch existing
        q = "SELECT vendor_id FROM vendors WHERE vendor_id IN ({})".format(",".join(["%s"]*len(vendor_ids)))
        cur.execute(q, vendor_ids)
        existing = {row[0] for row in cur.fetchall()}
        to_insert = [v for v in vendor_ids if v not in existing]
        if to_insert:
            cur.executemany("INSERT IGNORE INTO vendors (vendor_id, name) VALUES (%s, %s)", [(v, 'unknown') for v in to_insert])

    # payment_types
    payment_codes = sorted({ (r.get('payment_type') or '').strip() for r in batch_rows if r.get('payment_type') })
    payment_codes = [c for c in payment_codes if c != '']
    if payment_codes:
        q = "SELECT code FROM payment_types WHERE code IN ({})".format(",".join(["%s"]*len(payment_codes)))
        cur.execute(q, payment_codes)
        existing = {row[0] for row in cur.fetchall()}
        to_insert = [c for c in payment_codes if c not in existing]
        if to_insert:
            cur.executemany("INSERT IGNORE INTO payment_types (code, label) VALUES (%s, %s)", [(c, 'unknown') for c in to_insert])

def load_into_mysql(kept_rows, conn, batch_size=1000):
    """
    kept_rows: list of dict-like rows (keys must match CSV headers used below)
    conn: MySQLdb connection
    """
    cur = conn.cursor()
    insert_sql = (
        "INSERT INTO trips "
        "(vendor_id, pickup_datetime, dropoff_datetime, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, distance_km, duration_min, fare_amount, tip_amount, payment_type) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    # open a simple failure log
    fail_log_path = "db/failed_rows.log"
    with open(fail_log_path, "a", newline='') as flog:
        flog.write("=== New load run ===\n")

    total = 0
    for batch in chunked(kept_rows, batch_size):
        # ensure parents exist for this batch
        try:
            upsert_parents_for_batch(cur, batch)
            conn.commit()
        except Exception:
            # log and continue (rare)
            with open(fail_log_path, "a") as flog:
                flog.write("Parent upsert exception:\n")
                flog.write(traceback.format_exc() + "\n")
            conn.rollback()

        # prepare tuples for insertion (map fields exactly as your schema expects)
        values = []
        for r in batch:
            # normalize and convert; adapt keys if your CSV header differs
            vendor_id = r.get('vendor_id') or None
            pickup_datetime = r.get('pickup_datetime') or None
            dropoff_datetime = r.get('dropoff_datetime') or None
            pickup_lat = r.get('pickup_lat') or None
            pickup_lng = r.get('pickup_lng') or None
            dropoff_lat = r.get('dropoff_lat') or None
            dropoff_lng = r.get('dropoff_lng') or None
            distance_km = r.get('distance_km') or None
            duration_min = r.get('duration_min') or None
            fare_amount = r.get('fare_amount') if r['fare_amount'] is not None else 10.0,
            tip_amount = r.get('tip_amount') if r['tip_amount']  is not None else 0.0,
            payment_type = r.get('payment_type') if r['payment_type'] is not None else 'unknown'

            values.append((
                vendor_id, pickup_datetime, dropoff_datetime,
                pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
                distance_km, duration_min, fare_amount, tip_amount, payment_type
            ))

        # try batch insert
        try:
            cur.executemany(insert_sql, values)
            conn.commit()
            total += len(values)
        except IntegrityError as e:
            # write the error and the batch to log, then try single-row inserts to locate failing rows
            conn.rollback()
            with open(fail_log_path, "a") as flog:
                flog.write("Batch IntegrityError: {}\n".format(e))
                flog.write("Batch size: {}\n".format(len(values)))
            # fallback: insert one-by-one and log failing rows
            for idx, single_vals in enumerate(values):
                try:
                    cur.execute(insert_sql, single_vals)
                    conn.commit()
                    total += 1
                except IntegrityError as ie:
                    conn.rollback()
                    # log the row and the exact error
                    with open(fail_log_path, "a") as flog:
                        flog.write("Failed row index in batch {} : {}\n".format(idx, ie))
                        flog.write("Row data: {}\n".format(single_vals))
                        flog.write(traceback.format_exc() + "\n")
                except Exception as ex:
                    conn.rollback()
                    with open(fail_log_path, "a") as flog:
                        flog.write("Other insert exception on single row: {}\n".format(ex))
                        flog.write("Row data: {}\n".format(single_vals))
                        flog.write(traceback.format_exc() + "\n")
        except Exception as ex:
            # unexpected errors
            conn.rollback()
            with open(fail_log_path, "a") as flog:
                flog.write("Unexpected exception during batch insert:\n")
                flog.write(traceback.format_exc() + "\n")

    cur.close()
    return total



def main():
    parser = argparse.ArgumentParser(description='ETL for NYC trips (fixed version)')
    parser.add_argument('--input', required=True, help='Path to CSV, dir, or zip')
    parser.add_argument('--output', required=True, help='Path to cleaned CSV')
    parser.add_argument('--load-db', action='store_true', help='Also load cleaned data into MySQL')
    parser.add_argument('--db-host', default=os.getenv('DB_HOST', '127.0.0.1'))
    parser.add_argument('--db-user', default=os.getenv('DB_USER', 'nyc_user'))
    parser.add_argument('--db-pass', default=os.getenv('DB_PASSWORD', 'nyc_pass'))
    parser.add_argument('--db-name', default=os.getenv('DB_NAME', 'nyc_mobility'))
    parser.add_argument('--max-rows', type=int, default=None, help='Limit number of rows to process (for testing)')
    args = parser.parse_args()

    raw_iter = iter_csv_rows_from_input(args.input)
    kept_rows: List[Dict[str, Any]] = []
    excluded: int = 0
    kept: int = 0

    print("Starting data processing...")
    
    for i, row in enumerate(raw_iter):
        if args.max_rows and i >= args.max_rows:
            break
            
        clean_row, reasons = validate_and_transform_row(row)
        if reasons:
            excluded += 1
            if excluded <= 5:  # Show first few exclusion reasons for debugging
                print(f"Excluded row {i+1}, reasons: {reasons}")
            continue
        kept_rows.append(clean_row)
        kept += 1
        
        if (i + 1) % 10000 == 0:
            print(f"Processed {i+1} rows, kept {kept}, excluded {excluded}")

    written = write_cleaned_csv(kept_rows, args.output)
    print(f"Cleaned rows written to CSV: {written}; Total excluded: {excluded}")

    if args.load_db and written > 0:
        print("Loading data into MySQL...")
        conn = connect_db(args.db_host, args.db_user, args.db_pass, args.db_name)
        try:
            loaded = load_into_mysql(kept_rows, conn)
            print(f"Successfully loaded into MySQL: {loaded} records")
        finally:
            conn.close()


if __name__ == '__main__':
    main()