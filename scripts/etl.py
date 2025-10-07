import os
import io
import csv
import zipfile
import argparse
from datetime import datetime
from typing import Dict, Any, List, Tuple, Iterable

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


def load_into_mysql(rows: Iterable[Dict[str, Any]], conn) -> int:
    cur = conn.cursor()
    sql = (
        "INSERT INTO trips (vendor_id, pickup_datetime, dropoff_datetime, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, distance_km, duration_min, fare_amount, tip_amount, payment_type) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    batch: List[Tuple] = []
    total = 0
    for r in rows:
        batch.append((r['vendor_id'], r['pickup_datetime'], r['dropoff_datetime'], r['pickup_lat'], r['pickup_lng'], r['dropoff_lat'], r['dropoff_lng'], r['distance_km'], r['duration_min'], r['fare_amount'], r['tip_amount'], r['payment_type']))
        if len(batch) >= 2000:
            cur.executemany(sql, batch)
            conn.commit()
            total += len(batch)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)
        conn.commit()
        total += len(batch)
    return total


def main():
    parser = argparse.ArgumentParser(description='ETL for NYC trips')
    parser.add_argument('--input', required=True, help='Path to CSV, dir, or zip')
    parser.add_argument('--output', required=True, help='Path to cleaned CSV')
    parser.add_argument('--load-db', action='store_true', help='Also load cleaned data into MySQL')
    parser.add_argument('--db-host', default=os.getenv('DB_HOST', '127.0.0.1'))
    parser.add_argument('--db-user', default=os.getenv('DB_USER', 'nyc_user'))
    parser.add_argument('--db-pass', default=os.getenv('DB_PASSWORD', 'nyc_pass'))
    parser.add_argument('--db-name', default=os.getenv('DB_NAME', 'nyc_mobility'))
    args = parser.parse_args()

    raw_iter = iter_csv_rows_from_input(args.input)
    kept_rows: List[Dict[str, Any]] = []
    excluded: int = 0
    kept: int = 0

    for row in raw_iter:
        clean_row, reasons = validate_and_transform_row(row)
        if reasons:
            excluded += 1
            continue
        kept_rows.append(clean_row)
        kept += 1

    written = write_cleaned_csv(kept_rows, args.output)
    print(f"Cleaned rows written: {written}; Excluded: {excluded}")

    if args.load_db and written > 0:
        conn = connect_db(args.db_host, args.db_user, args.db_pass, args.db_name)
        try:
            loaded = load_into_mysql(kept_rows, conn)
            print(f"Loaded into MySQL: {loaded}")
        finally:
            conn.close()


if __name__ == '__main__':
    main()


