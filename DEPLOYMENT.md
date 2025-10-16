# Deployment Guide 🚀

This guide covers deploying the Urban Mobility Data Explorer to different platforms.

## Local Development Setup

### Prerequisites
- Python 3.8+
- MySQL/MariaDB
- Git

### Quick Start
```bash
# Clone repository
git clone https://github.com/yourusername/Urban-Mobility-Data-Explorer.git
cd Urban-Mobility-Data-Explorer

# Install dependencies
pip install -r requirements.txt

# Setup database (see README.md for detailed steps)
# Load data
python scripts/simple_loader.py --csv data/processed/cleaned.csv

# Start API
cd backend
python app.py
```

## Production Deployment

### Option 1: Traditional VPS/Server

#### Requirements
- Ubuntu 20.04+ or CentOS 8+
- Python 3.8+
- MySQL 8.0+
- Nginx (optional, for reverse proxy)

#### Setup Steps
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python and pip
sudo apt install python3 python3-pip python3-venv -y

# Install MySQL
sudo apt install mysql-server -y
sudo mysql_secure_installation

# Clone repository
git clone https://github.com/yourusername/Urban-Mobility-Data-Explorer.git
cd Urban-Mobility-Data-Explorer

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Setup database
sudo mysql -u root -p
# Run database setup commands from README.md

# Load data
python scripts/simple_loader.py --csv data/processed/cleaned.csv

# Install production WSGI server
pip install gunicorn

# Start with Gunicorn
cd backend
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

#### Nginx Configuration (Optional)
```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Option 2: Docker Deployment

#### Create Dockerfile
```dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 5000

# Start application
CMD ["python", "backend/app.py"]
```

#### Create docker-compose.yml
```yaml
version: '3.8'

services:
  db:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: root_password
      MYSQL_DATABASE: nyc_mobility
      MYSQL_USER: nyc_user
      MYSQL_PASSWORD: nyc_pass
    ports:
      - "3306:3306"
    volumes:
      - db_data:/var/lib/mysql
      - ./db:/docker-entrypoint-initdb.d:ro

  app:
    build: .
    ports:
      - "5000:5000"
    depends_on:
      - db
    environment:
      - DB_HOST=db
      - DB_USER=nyc_user
      - DB_PASSWORD=nyc_pass
      - DB_NAME=nyc_mobility
      - DB_PORT=3306

volumes:
  db_data:
```

#### Deploy with Docker
```bash
# Build and start services
docker-compose up -d

# Load data
docker-compose exec app python scripts/simple_loader.py --csv data/processed/cleaned.csv

# Check logs
docker-compose logs app
```

### Option 3: Cloud Platforms

#### Heroku
```bash
# Install Heroku CLI
# Create Procfile
echo "web: gunicorn backend.app:app" > Procfile

# Create runtime.txt
echo "python-3.9.16" > runtime.txt

# Deploy
heroku create your-app-name
heroku addons:create cleardb:ignite
git push heroku main
```

#### Railway
```bash
# Install Railway CLI
npm install -g @railway/cli

# Login and deploy
railway login
railway init
railway up
```

## Environment Variables

Create `.env` file for production:
```env
DB_HOST=your-db-host
DB_USER=nyc_user
DB_PASSWORD=your-secure-password
DB_NAME=nyc_mobility
DB_PORT=3306
FLASK_ENV=production
```

## Security Considerations

### Database Security
- Use strong passwords
- Enable SSL connections
- Restrict database access to application server only
- Regular backups

### API Security
- Add authentication if needed
- Use HTTPS in production
- Implement rate limiting
- Validate all inputs

### Example Security Headers
```python
from flask import Flask
from flask_talisman import Talisman

app = Flask(__name__)
Talisman(app, force_https=True)
```

## Monitoring

### Health Checks
```bash
# Check API health
curl http://your-domain.com/health

# Check database connection
curl http://your-domain.com/stats/summary
```

### Logging
```python
import logging
logging.basicConfig(level=logging.INFO)
```

## Backup Strategy

### Database Backup
```bash
# Create backup
mysqldump -u nyc_user -p nyc_mobility > backup_$(date +%Y%m%d).sql

# Restore backup
mysql -u nyc_user -p nyc_mobility < backup_20231015.sql
```

### Automated Backups
```bash
# Add to crontab
0 2 * * * mysqldump -u nyc_user -p nyc_mobility > /backups/backup_$(date +\%Y\%m\%d).sql
```

## Troubleshooting Production Issues

### Common Issues
1. **Database connection timeouts** - Check MySQL configuration
2. **Memory issues** - Monitor with `htop` or `top`
3. **Port conflicts** - Use `netstat -tulpn` to check ports
4. **SSL certificate issues** - Verify certificate installation

### Performance Optimization
- Use connection pooling
- Add database indexes
- Implement caching (Redis)
- Use CDN for static files

## Scaling

### Horizontal Scaling
- Use load balancer (Nginx, HAProxy)
- Multiple application instances
- Database read replicas
- Caching layer (Redis)

### Vertical Scaling
- Increase server resources
- Optimize database queries
- Use faster storage (SSD)
- Increase memory allocation
