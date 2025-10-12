const apiBase = '';

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error('Request failed');
  return await res.json();
}

function setKPIs(data){
  document.getElementById('kpiTrips').textContent = data.trips ?? 0;
  document.getElementById('kpiAvgSpeed').textContent = (data.avg_speed_kmh ?? 0).toFixed(2);
  document.getElementById('kpiAvgFare').textContent = (data.avg_fare_per_km ?? 0).toFixed(2);
  document.getElementById('kpiAvgDuration').textContent = (data.avg_duration_min ?? 0).toFixed(2);
}

function drawHourlyChart(rows){
  const canvas = document.getElementById('hourlyChart');
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0,0,canvas.width, canvas.height);
  const labels = rows.map(r => r.hour);
  const values = rows.map(r => Number(r.trips));
  const max = Math.max(1, ...values);
  const padding = 30;
  const w = canvas.width - padding*2;
  const h = canvas.height - padding*2;
  ctx.strokeStyle = '#999';
  ctx.beginPath();
  ctx.moveTo(padding, padding);
  ctx.lineTo(padding, padding+h);
  ctx.lineTo(padding+w, padding+h);
  ctx.stroke();
  const barW = Math.max(2, Math.floor(w / Math.max(1, values.length*1.5)));
  const gap = barW * 0.5;
  ctx.fillStyle = '#0b5fff';
  values.forEach((v, i) => {
    const x = padding + i*(barW+gap);
    const barH = Math.floor((v/max) * (h-2));
    const y = padding+h - barH;
    ctx.fillRect(x, y, barW, barH);
  });
}

async function refresh(){
  const from = document.getElementById('fromDate').value;
  const to = document.getElementById('toDate').value;
  const q = new URLSearchParams();
  if (from) q.set('from', from);
  if (to) q.set('to', to);
  const [summary, hourly, topTipped] = await Promise.all([
    fetchJSON(`${apiBase}/stats/summary?${q.toString()}`),
    fetchJSON(`${apiBase}/aggregations/hourly?${q.toString()}`),
    fetchJSON(`${apiBase}/insights/top_tipped?limit=20`)
  ]);
  setKPIs(summary);
  drawHourlyChart(hourly);
  const tbody = document.querySelector('#topTippedTable tbody');
  tbody.innerHTML = '';
  topTipped.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${r.trip_id}</td><td>${r.tip_pct}%</td><td>${r.fare_amount}</td><td>${r.tip_amount}</td>`;
    tbody.appendChild(tr);
  });
}

document.getElementById('btnRefresh').addEventListener('click', () => {
  refresh().catch(console.error);
});

document.getElementById('uploadForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const file = document.getElementById('fileInput').files[0];
  if (!file) return;
  const form = new FormData();
  form.append('file', file);
  const res = await fetch(`${apiBase}/process`, { method: 'POST', body: form });
  if (!res.ok) { alert('Processing failed'); return; }
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'cleaned.csv'; a.click();
  URL.revokeObjectURL(url);
});

// Initial load
refresh().catch(console.error);


