#!/usr/bin/env python3
"""
Simple data loader for NYC mobility data.
Loads cleaned CSV data into MySQL database.
"""

import csv
import os
import sys
import argparse
import MySQLdb
from typing import Dict, Any, List


def get_db_connection(host='127.0.0.1', user='nyc_user', password='nyc_pass', database='nyc_mobility', port=3306):
    """Create database connection with error handling."""
    try:
        return MySQLdb.connect(
            host=host,
            user=user,
            passwd=password,
            db=database,
            port=port,
            charset='utf8mb4'
        )
    except MySQLdb.Error as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)


def load_csv_to_db(csv_file: str, batch_size: int = 1000):
    """Load CSV data into database."""
    if not os.path.exists(csv_file):
        print(f"CSV file not found: {csv_file}")
        sys.exit(1)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Clear existing data
        print("Clearing existing data...")
        cursor.execute("DELETE FROM trips")
        cursor.execute("DELETE FROM vendors")
        cursor.execute("DELETE FROM payment_types")
        conn.commit()
        
        # First, collect all unique vendor_ids and payment_types
        print("Collecting unique vendors and payment types...")
        vendors = set()
        payment_types = set()
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('vendor_id'):
                    vendors.add(row['vendor_id'])
                if row.get('payment_type'):
                    payment_types.add(row['payment_type'])
        
        # Insert vendors
        print(f"Inserting {len(vendors)} vendors...")
        for vendor_id in vendors:
            cursor.execute("INSERT IGNORE INTO vendors (vendor_id, name) VALUES (%s, %s)", 
                          (vendor_id, 'Unknown'))
        
        # Insert payment types
        print(f"Inserting {len(payment_types)} payment types...")
        for payment_type in payment_types:
            cursor.execute("INSERT IGNORE INTO payment_types (code, label) VALUES (%s, %s)", 
                          (payment_type, 'Unknown'))
        
        conn.commit()
        
        # Now load trips data
        print(f"Loading trips from {csv_file}...")
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            batch = []
            total_loaded = 0
            
            for row in reader:
                # Prepare data for insertion
                trip_data = (
                    row.get('vendor_id') or None,
                    row.get('pickup_datetime'),
                    row.get('dropoff_datetime'),
                    float(row.get('pickup_lat', 0)),
                    float(row.get('pickup_lng', 0)),
                    float(row.get('dropoff_lat', 0)),
                    float(row.get('dropoff_lng', 0)),
                    float(row.get('distance_km', 0)),
                    float(row.get('duration_min', 0)),
                    float(row.get('fare_amount', 0)),
                    float(row.get('tip_amount', 0)),
                    row.get('payment_type') or None
                )
                
                batch.append(trip_data)
                
                if len(batch) >= batch_size:
                    # Insert batch
                    insert_sql = """
                    INSERT INTO trips 
                    (vendor_id, pickup_datetime, dropoff_datetime, pickup_lat, pickup_lng, 
                     dropoff_lat, dropoff_lng, distance_km, duration_min, fare_amount, tip_amount, payment_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(insert_sql, batch)
                    conn.commit()
                    total_loaded += len(batch)
                    print(f"Loaded {total_loaded} records...")
                    batch = []
            
            # Insert remaining records
            if batch:
                insert_sql = """
                INSERT INTO trips 
                (vendor_id, pickup_datetime, dropoff_datetime, pickup_lat, pickup_lng, 
                 dropoff_lat, dropoff_lng, distance_km, duration_min, fare_amount, tip_amount, payment_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_loaded += len(batch)
            
            print(f"Successfully loaded {total_loaded} records into database!")
            
    except Exception as e:
        print(f"Error loading data: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Simple data loader for NYC mobility data')
    parser.add_argument('--csv', required=True, help='Path to cleaned CSV file')
    parser.add_argument('--host', default='127.0.0.1', help='Database host')
    parser.add_argument('--user', default='nyc_user', help='Database user')
    parser.add_argument('--password', default='nyc_pass', help='Database password')
    parser.add_argument('--database', default='nyc_mobility', help='Database name')
    parser.add_argument('--port', type=int, default=3306, help='Database port')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for inserts')
    
    args = parser.parse_args()
    
    print("Starting data load...")
    load_csv_to_db(args.csv, args.batch_size)
    print("Data load completed!")


if __name__ == '__main__':
    main()
