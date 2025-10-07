CREATE INDEX IF NOT EXISTS idx_trips_pickup_datetime ON trips (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_trips_dropoff_datetime ON trips (dropoff_datetime);
CREATE INDEX IF NOT EXISTS idx_trips_payment_type ON trips (payment_type);
CREATE INDEX IF NOT EXISTS idx_trips_hour_dow ON trips (day_of_week, hour_of_day);
CREATE INDEX IF NOT EXISTS idx_trips_pickup_coords ON trips (pickup_lat, pickup_lng);
CREATE INDEX IF NOT EXISTS idx_trips_dropoff_coords ON trips (dropoff_lat, dropoff_lng);

