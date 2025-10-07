# Urban Mobility Data Explorer 🚕📊

A comprehensive Flask-based web application for analyzing NYC taxi trip data with advanced ETL processing, real-time insights, and machine learning algorithms.

## Features 🎯

### Data Processing & ETL
- **Intelligent Data Cleaning**: Validates coordinates, timestamps, and trip metrics
- **Multiple Input Formats**: Supports CSV files, ZIP archives, and directories
- **Distance Calculation**: Uses Haversine formula for accurate geographic distances
- **Data Quality Filtering**: Removes unrealistic trips and suspicious patterns

### REST API Endpoints
- **Health Monitoring**: System status checks
- **Trip Statistics**: Summary metrics with date filtering
- **Data Aggregations**: Hourly trip patterns and trends  
- **Advanced Analytics**: Top-tipped trips using heap-based algorithms
- **File Processing**: Upload and clean CSV files on-demand

### Database Features
- **MySQL Integration**: Optimized schema with computed columns
- **Auto-generated Fields**: Speed, fare efficiency, time-based features
- **Indexed Queries**: Fast lookups on dates, coordinates, and patterns
- **Foreign Key Constraints**: Data integrity across vendors and payment types

## Project Structure 📁

```
Urban-Mobility-Data-Explorer/
├── backend/
│   ├── app.py              # Main Flask application
│   ├── algorithms.py       # Heap-based analytics algorithms
│   └── __init__.py
├── scripts/
│   ├── etl.py              # Original ETL script
│   └── etl_fixed.py        # Enhanced ETL with format handling
├── db/
│   ├── schema.sql          # Database table definitions
│   └── indexes.sql         # Performance optimization indexes
├── data/
│   ├── raw/                # Original data files (gitignored)
│   └── processed/          # Cleaned CSV output (gitignored)
├── requirements.txt        # Python dependencies
├── .env                    # Environment configuration (gitignored)
├── .gitignore             # Git ignore rules
└── README.md              # This file
```

## Quick Start 🚀

### Prerequisites
- Python 3.8+
- MySQL/MariaDB server
- XAMPP (or standalone MySQL installation)

### 1. Clone and Setup
```bash
git clone git@github.com:gilbmura/Urban-Mobility-Data-Explorer.git
cd Urban-Mobility-Data-Explorer
```

### 2. Install Dependencies
```bash
# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # On Windows
# source .venv/bin/activate  # On macOS/Linux

# Install packages
pip install -r requirements.txt
```

### 3. Database Setup
```bash
# Start MySQL server (XAMPP or service)
# Then create database and user
mysql -u root -p

CREATE DATABASE nyc_mobility CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'nyc_user'@'localhost' IDENTIFIED BY 'nyc_pass';
GRANT ALL PRIVILEGES ON nyc_mobility.* TO 'nyc_user'@'localhost';
FLUSH PRIVILEGES;
exit;

# Import schema and indexes
mysql -u nyc_user -p nyc_mobility < db/schema.sql
mysql -u nyc_user -p nyc_mobility < db/indexes.sql
```

### 4. Environment Configuration
Create `.env` file:
```env
DB_HOST=127.0.0.1
DB_USER=nyc_user
DB_PASSWORD=nyc_pass
DB_NAME=nyc_mobility
```

### 5. Data Processing
```bash
# Process sample data (limit for testing)
python scripts/etl_fixed.py --input data/raw/train.zip --output data/processed/cleaned.csv --load-db --max-rows 100000

# Process full dataset
python scripts/etl_fixed.py --input data/raw/train.zip --output data/processed/cleaned.csv --load-db
```

### 6. Start Application
```bash
cd backend
python app.py
```

Visit: http://127.0.0.1:5000

## API Endpoints 🔌

### Health Check
```http
GET /health
```

### Trip Statistics
```http
GET /stats/summary
GET /stats/summary?from=2016-01-01&to=2016-06-30
```

### Trip Listings
```http
GET /trips?limit=50&offset=0
```

### Hourly Aggregations
```http
GET /aggregations/hourly?from=2016-01-01&to=2016-01-31
```

### Top Tipped Trips (ML Algorithm)
```http
GET /insights/top_tipped?limit=20
```

### File Processing
```http
POST /process
Content-Type: multipart/form-data
Body: file=<csv_file>
```

## Data Schema 📋

### Trips Table
- **trip_id**: Auto-increment primary key
- **vendor_id**: Taxi company identifier  
- **pickup/dropoff_datetime**: Trip timestamps
- **pickup/dropoff_lat/lng**: Geographic coordinates
- **distance_km**: Calculated trip distance
- **duration_min**: Trip duration in minutes
- **fare_amount**: Trip fare cost
- **tip_amount**: Tip amount
- **payment_type**: Payment method code

### Computed Columns (Auto-generated)
- **speed_kmh**: Average trip speed
- **fare_per_km**: Fare efficiency metric
- **hour_of_day**: Pickup hour (0-23)
- **day_of_week**: Day index (0=Monday)
- **rush_hour**: Boolean rush hour indicator
- **is_weekend**: Boolean weekend indicator

## Performance Features ⚡

### Database Optimizations
- **Multi-column indexes** on frequently queried fields
- **Computed columns** for real-time analytics
- **Batch insertions** with 2000-record chunks
- **Connection pooling** for concurrent requests

### ETL Optimizations
- **Streaming processing** for large files
- **Memory-efficient** CSV parsing
- **Parallel validation** with batch commits
- **Progress monitoring** with regular updates

## Development 🛠️

### Running Tests
```bash
# Test individual endpoints
curl http://127.0.0.1:5000/health
curl "http://127.0.0.1:5000/stats/summary"
curl "http://127.0.0.1:5000/trips?limit=5"
```

### Code Structure
- **Flask App** (`backend/app.py`): REST API with database connections
- **Algorithms** (`backend/algorithms.py`): Heap-based analytics for top-K queries  
- **ETL Pipeline** (`scripts/etl_fixed.py`): Data validation and transformation
- **Database Schema** (`db/`): SQL table definitions and indexes

## Contributing 🤝

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## License 📄

This project is open source and available under the [MIT License](LICENSE).

## Acknowledgments 🙏

- NYC Taxi & Limousine Commission for providing open datasets
- Flask community for excellent web framework
- MySQL team for robust database engine
- Contributors and open source community

---

**Built with ❤️ for urban mobility analysis and data science applications**


