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

### 1. Clone and Setup
```bash
git clone https://github.com/gilbmura/Urban-Mobility-Data-Explorer.git
cd Urban-Mobility-Data-Explorer

# Install dependencies
python setup.py
# OR manually:
pip install -r requirements.txt
```

> **Note:** Replace `yourusername` with your actual GitHub username

### 2. Database Setup

#### **Option A: XAMPP (Easiest for Windows)**
1. **Download XAMPP:** https://www.apachefriends.org/download.html
2. **Install and start XAMPP**
3. **Start MySQL** from XAMPP Control Panel
4. **Open phpMyAdmin:** http://localhost/phpmyadmin
5. **Create database:** Click "New" → Enter `nyc_mobility` → Create
6. **Import schema:** Select `nyc_mobility` → Import → Choose `db/schema.sql` → Go

#### **Option B: MySQL Server (Windows)**
1. **Download MySQL:** https://dev.mysql.com/downloads/installer/
2. **Install MySQL Server** (choose "Server only" or "Developer Default")
3. **Set root password** during installation
4. **Start MySQL service:**
   ```bash
   net start mysql
   # OR check if running:
   Get-Service | Where-Object {$_.Name -like "*mysql*"}
   ```

5. **Create database and user:**
   ```bash
   # Find MySQL installation (usually):
   "C:\Program Files\MySQL\MySQL Server 9.4\bin\mysql.exe" -u root -p
   ```
   **Note:** For us we used "C:\Program Files\MySQL\MySQL Server 9.4\bin\mysql.exe" -u nyc_user -p nyc_mobility
   
   Then run:
   ```sql
   CREATE DATABASE nyc_mobility CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   CREATE USER 'nyc_user'@'localhost' IDENTIFIED BY 'nyc_pass';
   GRANT ALL PRIVILEGES ON nyc_mobility.* TO 'nyc_user'@'localhost';
   FLUSH PRIVILEGES;
   exit;
   ```

6. **Import schema:**
   ```bash
   "C:\Program Files\MySQL\MySQL Server 9.4\bin\mysql.exe" -u nyc_user -pnyc_pass nyc_mobility < db/schema.sql
   ```

#### **Option C: Docker (Advanced)**
```bash
# Stop any existing MySQL containers
docker stop $(docker ps -q --filter ancestor=mysql)

# Run MySQL container
docker run -d --name nyc-mysql \
  -e MYSQL_ROOT_PASSWORD=root_password \
  -e MYSQL_DATABASE=nyc_mobility \
  -e MYSQL_USER=nyc_user \
  -e MYSQL_PASSWORD=nyc_pass \
  -p 3306:3306 \
  -v ./db:/docker-entrypoint-initdb.d:ro \
  mysql:8.0

# Wait for initialization
docker logs nyc-mysql
```

### 3. Load Data
```bash
# Load the cleaned CSV data into database
python scripts/simple_loader.py --csv data/processed/cleaned.csv
```

### 4. Start API
```bash
cd backend
python app.py
```

### 5. Test API
```bash
# Test all endpoints
python test_api.py

# OR test individual endpoints:
# Health check: http://127.0.0.1:5000/health
# Stats: http://127.0.0.1:5000/stats/summary
# Trips: http://127.0.0.1:5000/trips?limit=5
```

**API will be available at:** http://127.0.0.1:5000

## Troubleshooting 🔧

### Database Connection Issues
```bash
# Check if MySQL is running
Get-Service | Where-Object {$_.Name -like "*mysql*"}

# Test connection
"C:\Program Files\MySQL\MySQL Server 9.4\bin\mysql.exe" -u nyc_user -pnyc_pass -e "SELECT 1;"

# Check database exists
"C:\Program Files\MySQL\MySQL Server 9.4\bin\mysql.exe" -u nyc_user -pnyc_pass -e "SHOW DATABASES;"
```

### Port Issues
- **Default MySQL port:** 3306
- **API port:** 5000
- **Check ports:** `netstat -an | findstr :3306`

### Data Loading Issues
```bash
# Check if data loaded correctly
"C:\Program Files\MySQL\MySQL Server 9.4\bin\mysql.exe" -u nyc_user -pnyc_pass -e "USE nyc_mobility; SELECT COUNT(*) FROM trips;"
```

### API Not Starting
```bash
# Check if port 5000 is free
netstat -an | findstr :5000

# Start API manually
cd backend
python app.py
```

### Common Issues & Solutions

#### **Issue: "Access denied for user 'nyc_user'@'localhost'"**
**Solution:** User doesn't exist or wrong password
```bash
# Connect as root and recreate user
"C:\Program Files\MySQL\MySQL Server 9.4\bin\mysql.exe" -u root -p
```
```sql
DROP USER IF EXISTS 'nyc_user'@'localhost';
CREATE USER 'nyc_user'@'localhost' IDENTIFIED BY 'nyc_pass';
GRANT ALL PRIVILEGES ON nyc_mobility.* TO 'nyc_user'@'localhost';
FLUSH PRIVILEGES;
```

#### **Issue: "Foreign key constraint fails"**
**Solution:** The simple_loader.py handles this automatically by creating vendors and payment_types first

#### **Issue: "Lost connection to server"**
**Solution:** Usually a port mismatch or MySQL not running
```bash
# Check MySQL service
Get-Service | Where-Object {$_.Name -like "*mysql*"}

# Check if port is listening
netstat -an | findstr :3306
```

#### **Issue: "mysql command not found"**
**Solution:** Use full path to MySQL executable
```bash
"C:\Program Files\MySQL\MySQL Server 9.4\bin\mysql.exe" -u nyc_user -p
```

#### **Issue: API returns "Database connection failed"**
**Solution:** Check database credentials and connection
```bash
# Test database connection
"C:\Program Files\MySQL\MySQL Server 9.4\bin\mysql.exe" -u nyc_user -pnyc_pass -e "USE nyc_mobility; SELECT COUNT(*) FROM trips;"
```

### Development Notes 📝

#### **What We Fixed:**
1. **Removed complex ETL script** - replaced with simple_loader.py
2. **Fixed foreign key constraints** - loader now creates parent records first
3. **Added proper error handling** - all API endpoints have error responses
4. **Streamlined dependencies** - only essential packages in requirements.txt
5. **Added port configuration** - supports different MySQL ports
6. **Created test suite** - test_api.py for easy validation

#### **File Structure:**
```
├── backend/
│   ├── app.py              # Main API server
│   └── algorithms.py       # Tip calculation algorithms
├── scripts/
│   └── simple_loader.py    # CSV to database loader
├── db/
│   └── schema.sql         # Database schema
├── data/processed/
│   └── cleaned.csv        # Processed data (99,658 records)
├── requirements.txt       # Python dependencies
├── setup.py              # Easy setup script
└── test_api.py          # API testing script
```

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


