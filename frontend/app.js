const API_BASE_URL = 'http://127.0.0.1:5000';
const state = {
    charts: {
        hourly: null,
        weekly: null
    },
    dateFrom: '2016-01-01',
    dateTo: '2016-12-31'
};

// INITIALIZATION

document.addEventListener('DOMContentLoaded', () => {
    initDateFilters();
    loadStatistics();
    loadCharts();
    loadActivity();
    
    // Set up filter button
    document.getElementById('applyFilter')?.addEventListener('click', applyDateFilter);
});

// DATE FILTERS

function initDateFilters() {
    const dateFrom = document.getElementById('dateFrom');
    const dateTo = document.getElementById('dateTo');
    
    if (dateFrom) {
        dateFrom.value = '2016-01-01';
        dateFrom.min = '2016-01-01';
        dateFrom.max = '2016-12-31';
    }
    
    if (dateTo) {
        dateTo.value = '2016-12-31';
        dateTo.min = '2016-01-01';
        dateTo.max = '2016-12-31';
    }
}

function applyDateFilter() {
    const dateFrom = document.getElementById('dateFrom').value;
    const dateTo = document.getElementById('dateTo').value;
    
    if (dateFrom && dateTo) {
        state.dateFrom = dateFrom;
        state.dateTo = dateTo;
        loadStatistics();
        loadCharts();
        showToast('Filters applied successfully', 'success');
    } else {
        showToast('Please select both dates', 'warning');
    }
}

// LOAD STATISTICS

async function loadStatistics() {
    try {
        showLoading();
        const response = await fetch(`${API_BASE_URL}/stats/summary?date_from=${state.dateFrom}&date_to=${state.dateTo}`);
        
        if (!response.ok) throw new Error('Failed to fetch statistics');
        
        const data = await response.json();
        
        document.getElementById('totalTrips').textContent = formatNumber(data.total_trips || 0);
        document.getElementById('avgFare').textContent = `$${(data.avg_fare || 0).toFixed(2)}`;
        document.getElementById('avgDistance').textContent = `${(data.avg_distance || 0).toFixed(2)} km`;
        document.getElementById('avgSpeed').textContent = `${(data.avg_speed || 0).toFixed(1)} km/h`;
        
        hideLoading();
    } catch (error) {
        console.error('Failed to load statistics:', error);
        hideLoading();
        // Load fallback data
        document.getElementById('totalTrips').textContent = '0';
        document.getElementById('avgFare').textContent = '$0.00';
        document.getElementById('avgDistance').textContent = '0.00 km';
        document.getElementById('avgSpeed').textContent = '0.0 km/h';
    }
}

// LOAD CHARTS

async function loadCharts() {
    try {
        // Load hourly distribution
        const hourlyResponse = await fetch(`${API_BASE_URL}/aggregations/hourly?date_from=${state.dateFrom}&date_to=${state.dateTo}`);
        const hourlyData = await hourlyResponse.json();
        createHourlyChart(hourlyData);
        
        // Load weekly patterns
        const weeklyResponse = await fetch(`${API_BASE_URL}/aggregations/weekly?date_from=${state.dateFrom}&date_to=${state.dateTo}`);
        const weeklyData = await weeklyResponse.json();
        createWeeklyChart(weeklyData);
    } catch (error) {
        console.error('Failed to load charts:', error);
        createFallbackCharts();
    }
}

function createHourlyChart(data) {
    const ctx = document.getElementById('hourlyChart');
    if (!ctx) return;
    
    if (state.charts.hourly) {
        state.charts.hourly.destroy();
    }
    
    const labels = data.map(d => `${d.hour}:00`);
    const values = data.map(d => d.trip_count);
    
    state.charts.hourly = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Trips',
                data: values,
                borderColor: '#6366f1',
                backgroundColor: 'rgba(99, 102, 241, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: '#f8fafc'
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: '#f8fafc'
                    }
                }
            }
        }
    });
}

function createWeeklyChart(data) {
    const ctx = document.getElementById('weeklyChart');
    if (!ctx) return;
    
    if (state.charts.weekly) {
        state.charts.weekly.destroy();
    }
    
    const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    const labels = data.map(d => days[d.day_of_week]);
    const values = data.map(d => d.trip_count);
    
    state.charts.weekly = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Trips',
                data: values,
                backgroundColor: 'rgba(236, 72, 153, 0.8)',
                borderColor: '#ec4899',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    ticks: {
                        color: '#f8fafc'
                    }
                },
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        color: '#f8fafc'
                    }
                }
            }
        }
    });
}

function createFallbackCharts() {
    // Create sample data for demonstration
    const hourlyData = Array.from({length: 24}, (_, i) => ({
        hour: i,
        trip_count: Math.floor(Math.random() * 1000) + 100
    }));
    createHourlyChart(hourlyData);
    
    const weeklyData = Array.from({length: 7}, (_, i) => ({
        day_of_week: i,
        trip_count: Math.floor(Math.random() * 5000) + 1000
    }));
    createWeeklyChart(weeklyData);
}

// ACTIVITY FEED
async function loadActivity() {
    try {
        const response = await fetch(`${API_BASE_URL}/trips?limit=5`);
        const trips = await response.json();
        
        const activityList = document.getElementById('activityList');
        if (!activityList) return;
        
        activityList.innerHTML = trips.map(trip => `
            <div class="activity-item">
                <div class="activity-icon">
                    <i class="fas fa-taxi"></i>
                </div>
                <div class="activity-content">
                    <div>Trip #${trip.trip_id} completed</div>
                    <div class="activity-time">${formatDateTime(trip.pickup_datetime)}</div>
                </div>
                <div style="font-weight: 600; color: #10b981;">
                    $${trip.fare_amount.toFixed(2)}
                </div>
            </div>
        `).join('');
    } catch (error) {
        console.error('Failed to load activity:', error);
        document.getElementById('activityList').innerHTML = '<p style="color: #64748b;">No recent activity</p>';
    }
}

// UTILITY FUNCTIONS

function formatNumber(num) {
    return new Intl.NumberFormat('en-US').format(num);
}

function formatDateTime(dateStr) {
    const date = new Date(dateStr);
    return date.toLocaleString('en-US', { 
        month: 'short', 
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function showLoading() {
    document.getElementById('loadingOverlay')?.classList.remove('hidden');
}

function hideLoading() {
    document.getElementById('loadingOverlay')?.classList.add('hidden');
}

function showToast(message, type = 'info') {
    const container = document.getElementById('toastContainer');
    if (!container) return;
    
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    
    const icon = type === 'success' ? 'check-circle' : 
                 type === 'error' ? 'exclamation-circle' : 
                 type === 'warning' ? 'exclamation-triangle' : 'info-circle';
    
    toast.innerHTML = `
        <div class="toast-icon">
            <i class="fas fa-${icon}"></i>
        </div>
        <div class="toast-content">${message}</div>
        <div class="toast-close">
            <i class="fas fa-times"></i>
        </div>
    `;
    
    container.appendChild(toast);
    
    toast.querySelector('.toast-close').addEventListener('click', () => {
        toast.remove();
    });
    
    setTimeout(() => {
        toast.remove();
   }, 5000);
}