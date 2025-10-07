-- Schema for NYC Mobility (MySQL)
CREATE TABLE IF NOT EXISTS vendors (
  vendor_id VARCHAR(8) PRIMARY KEY,
  name VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS payment_types (
  code VARCHAR(8) PRIMARY KEY,
  label VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS trips (
  trip_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  vendor_id VARCHAR(8),
  pickup_datetime DATETIME NOT NULL,
  dropoff_datetime DATETIME NOT NULL,
  pickup_lat DECIMAL(9,6) NOT NULL,
  pickup_lng DECIMAL(9,6) NOT NULL,
  dropoff_lat DECIMAL(9,6) NOT NULL,
  dropoff_lng DECIMAL(9,6) NOT NULL,
  distance_km DECIMAL(10,3) NOT NULL,
  duration_min DECIMAL(10,3) NOT NULL,
  fare_amount DECIMAL(10,2) NOT NULL,
  tip_amount DECIMAL(10,2) NOT NULL,
  payment_type VARCHAR(8),
  -- Derived features
  speed_kmh DECIMAL(10,3) GENERATED ALWAYS AS (CASE WHEN duration_min > 0 THEN (distance_km / (duration_min/60)) ELSE NULL END) STORED,
  fare_per_km DECIMAL(10,3) GENERATED ALWAYS AS (CASE WHEN distance_km > 0 THEN (fare_amount / distance_km) ELSE NULL END) STORED,
  hour_of_day TINYINT GENERATED ALWAYS AS (HOUR(pickup_datetime)) STORED,
  day_of_week TINYINT GENERATED ALWAYS AS ((DAYOFWEEK(pickup_datetime) + 5) % 7) STORED, -- 0=Mon
  rush_hour TINYINT GENERATED ALWAYS AS (CASE WHEN HOUR(pickup_datetime) IN (7,8,9,16,17,18,19) THEN 1 ELSE 0 END) STORED,
  is_weekend TINYINT GENERATED ALWAYS AS (CASE WHEN ((DAYOFWEEK(pickup_datetime) IN (1,7))) THEN 1 ELSE 0 END) STORED,
  CONSTRAINT fk_trips_vendor FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
  CONSTRAINT fk_trips_payment FOREIGN KEY (payment_type) REFERENCES payment_types(code)
);


