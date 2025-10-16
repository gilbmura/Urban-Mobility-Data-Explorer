// script.js

document.addEventListener("DOMContentLoaded", () => {
  const BASE_URL = "http://127.0.0.1:5000/trips"; 
  const loadingOverlay = document.getElementById("loadingOverlay");

  const totalTripsEl = document.getElementById("totalTrips");
  const avgFareEl = document.getElementById("avgFare");
  const avgDistanceEl = document.getElementById("avgDistance");
  const avgSpeedEl = document.getElementById("avgSpeed");

  const dateFromInput = document.getElementById("dateFrom");
  const dateToInput = document.getElementById("dateTo");
  const applyFilterBtn = document.getElementById("applyFilter");
  const filterDay = document.getElementById('filterDay');

  let hourlyChart;
  let weeklyChart;

  // ========== Load and Filter Function ==========
  async function loadDashboard(startDate = null, endDate = null) {
    try {
      loadingOverlay.classList.remove("hidden");

      // Build URL with optional date range
      let apiUrl = BASE_URL;
      if (startDate && endDate) {
        apiUrl += `?start=${startDate}&end=${endDate}`;
      }

      const response = await fetch(apiUrl);
      const data = await response.json();

      if (!Array.isArray(data)) throw new Error("Invalid data format");

      updateStats(data);
      renderCharts(data);
      showActivityFeed(data);

    } catch (error) {
      console.error("Error loading data:", error);
      showToast("Error loading data from API", "error");
    } finally {
      loadingOverlay.classList.add("hidden");
    }
  }

  // ========== Filter Button ==========
  applyFilterBtn.addEventListener("click", () => {
    console.log(filterDay.value);
    
  });

  // ========== Summary Statistics ==========
  function updateStats(data) {
    const totalTrips = data.length;
    const avgFare = (data.reduce((sum, d) => sum + parseFloat(d.fare_amount), 0) / totalTrips).toFixed(2);
    const avgDistance = (data.reduce((sum, d) => sum + parseFloat(d.distance_km), 0) / totalTrips).toFixed(2);
    const avgSpeed = (data.reduce((sum, d) => sum + parseFloat(d.speed_kmh), 0) / totalTrips).toFixed(2);

    totalTripsEl.textContent = totalTrips.toLocaleString();
    avgFareEl.textContent = `$${avgFare}`;
    avgDistanceEl.textContent = `${avgDistance} km`;
    avgSpeedEl.textContent = `${avgSpeed} km/h`;


    // Update stats with filtered data
    // updateDashboardStats(filteredData);

    // Display results
    // displayResults(filteredData, dataType);
  }

  // ========== Chart Rendering ==========
  function renderCharts(data) {
    const hours = Array.from({ length: 24 }, (_, i) => i);
    const days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

    const hourlyCounts = new Array(24).fill(0);
    const weeklyCounts = new Array(7).fill(0);

    data.forEach(d => {
      hourlyCounts[d.hour_of_day]++;
      weeklyCounts[d.day_of_week]++;
    });

    if (hourlyChart) hourlyChart.destroy();
    if (weeklyChart) weeklyChart.destroy();

    const hourlyCtx = document.getElementById("hourlyChart").getContext("2d");
    const weeklyCtx = document.getElementById("weeklyChart").getContext("2d");

    hourlyChart = new Chart(hourlyCtx, {
      type: "bar",
      data: {
        labels: hours.map(h => `${h}:00`),
        datasets: [{
          label: "Trips per Hour",
          data: hourlyCounts,
          backgroundColor: "rgba(54, 162, 235, 0.7)",
          borderColor: "rgba(54, 162, 235, 1)",
          borderWidth: 1
        }]
      },
      options: {
        responsive: true,
        scales: { y: { beginAtZero: true } }
      }
    });

    weeklyChart = new Chart(weeklyCtx, {
      type: "line",
      data: {
        labels: days,
        datasets: [{
          label: "Trips per Day",
          data: weeklyCounts,
          fill: true,
          borderColor: "rgba(255, 99, 132, 1)",
          backgroundColor: "rgba(255, 99, 132, 0.2)",
          tension: 0.3,
          borderWidth: 2
        }]
      },
      options: {
        responsive: true,
        scales: { y: { beginAtZero: true } }
      }
    });
  }

  // ========== Live Activity ==========
  function showActivityFeed(data) {
    const activityList = document.getElementById("activityList");
    activityList.innerHTML = "";

    const recentTrips = data.slice(-20).reverse();
    recentTrips.forEach(t => {
      const item = document.createElement("div");
      item.classList.add("activity-item");
      item.innerHTML = `
        <i class="fas fa-car-side"></i>
        <div>
          <strong>Trip #${t.trip_id}</strong> - ${parseFloat(t.distance_km).toFixed(1)} km 
          <br><small>${t.pickup_datetime}</small>
        </div>
      `;
      activityList.appendChild(item);
    });
  }

  // ========== Toast Notification ==========
  function showToast(message, type = "info") {
    const toast = document.createElement("div");
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    document.getElementById("toastContainer").appendChild(toast);
    setTimeout(() => toast.remove(), 4000);
  }

  // ========== Expose Refresh Function ==========
  window.loadCharts = loadDashboard;

  // ========== Load on Startup ==========
  loadDashboard();
});
