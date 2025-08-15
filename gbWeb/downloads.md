# Download geoBoundaries Data

> This page provides the most recent geoBoundaries data.
> Archives are available on Github.

<div id="gb-app">
  <div class="gb-controls">
    <input id="gb-search" type="search" placeholder="Search (country, ISO, year, license, source…)" aria-label="Search">
    <select id="gb-filter-adm" aria-label="Filter by ADM level">
      <option value="">ADM: All</option>
    </select>
    <select id="gb-filter-cont" aria-label="Filter by Continent">
      <option value="">Continent: All</option>
    </select>
    <button id="gb-download-csv" type="button" title="Download current rows as CSV">⬇︎ CSV</button>
    <span id="gb-count" aria-live="polite"></span>
  </div>

  <div class="gb-table-wrap">
    <table id="gb-table" aria-describedby="gb-count">
      <thead id="gb-thead"></thead>
      <tbody id="gb-tbody"></tbody>
    </table>
  </div>

  <div class="gb-pager" aria-label="Pagination">
    <button id="gb-prev" disabled>‹ Prev</button>
    <span id="gb-page"></span>
    <button id="gb-next" disabled>Next ›</button>
  </div>

  <noscript>This page requires JavaScript to load the downloads from the API.</noscript>
</div>

<script>
(function () {
  // ---------- CONFIG ----------
  const API_URL = 'https://www.geoboundaries.org/api/current/gbOpen/ALL/ALL/';
  const PAGE_SIZE = 50;
  const LINK_COLUMNS = ['ZIP','GeoJSON','TopoJSON','Simplified','Preview','Source'];

  // ---------- STATE ----------
  let raw = [];
  let rows = [];
  let filtered = [];
  let page = 1;
  let sortKey = 'Country';
  let sortDir = 1; // 1 asc, -1 desc
  let columns = ['Country','ISO3','ADM','Year','Canonical Type','License','Source','ZIP','GeoJSON','TopoJSON','Simplified','Preview','Units'];

  // ---------- ELEMENTS ----------
  const el = (id) => document.getElementById(id);
  const thead = el('gb-thead');
  const tbody = el('gb-tbody');
  const search = el('gb-search');
  const countEl = el('gb-count');
  const selAdm = el('gb-filter-adm');
  const selCont = el('gb-filter-cont');
  const btnCSV = el('gb-download-csv');
  const btnPrev = el('gb-prev');
  const btnNext = el('gb-next');
  const pageLabel = el('gb-page');

  // ---------- HELPERS ----------
  const esc = (s) => String(s ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
  const nz = (s, d='') => (s == null || String(s).trim() === '') ? d : s;

  function a(href, label) {
    if (!href) return '';
    const safe = href.trim();
    if (!safe) return '';
    const text = esc(label || safe);
    return `<a href="${esc(safe)}" target="_blank" rel="noopener">${text}</a>`;
  }

  function toCSV(arr, cols) {
    const head = cols.join(',');
    const lines = arr.map(r => cols.map(c => {
      const v = r[c] ?? '';
      const s = String(v).replaceAll('"','""');
      return /[",\n]/.test(s) ? `"${s}"` : s;
    }).join(','));
    return [head, ...lines].join('\n');
  }

  function uniqueSorted(list) {
    return Array.from(new Set(list.filter(Boolean))).sort((a,b)=>String(a).localeCompare(String(b)));
  }

  function numish(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  // ---------- RENDER ----------
  function renderHeader() {
    thead.innerHTML = '<tr>' + columns.map(h => {
      const arrow = (sortKey === h) ? (sortDir > 0 ? ' ▲' : ' ▼') : '';
      return `<th scope="col" data-key="${esc(h)}" tabindex="0">${esc(h)}<span class="sort">${arrow}</span></th>`;
    }).join('') + '</tr>';

    thead.querySelectorAll('th').forEach(th => {
      const key = th.getAttribute('data-key');
      function sortHandler() {
        if (sortKey === key) { sortDir = -sortDir; } else { sortKey = key; sortDir = 1; }
        sortRows();
        page = 1;
        renderBody();
        renderHeader(); // update arrows
      }
      th.addEventListener('click', sortHandler);
      th.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); sortHandler(); }});
    });
  }

  function renderBody() {
    const total = filtered.length;
    const maxPage = Math.max(1, Math.ceil(total / PAGE_SIZE));
    page = Math.min(page, maxPage);
    const start = (page - 1) * PAGE_SIZE;
    const slice = filtered.slice(start, start + PAGE_SIZE);

    tbody.innerHTML = slice.map(r => {
      return '<tr>' + columns.map(h => {
        const val = r[h] ?? '';
        if (LINK_COLUMNS.includes(h)) return `<td class="link">${val}</td>`;
        return `<td>${esc(val)}</td>`;
      }).join('') + '</tr>';
    }).join('');

    countEl.textContent = `${filtered.length} rows`;
    pageLabel.textContent = `Page ${page} of ${maxPage}`;
    btnPrev.disabled = (page <= 1);
    btnNext.disabled = (page >= maxPage);
  }

  function applyFilters() {
    const q = search.value.trim().toLowerCase();
    const wantADM = selAdm.value;
    const wantCont = selCont.value;

    filtered = rows.filter(r => {
      if (wantADM && r.ADM !== wantADM) return false;
      if (wantCont && r.Continent !== wantCont) return false;
      if (!q) return true;
      return columns.some(h => String(r[h] ?? '').toLowerCase().includes(q));
    });
  }

  function sortRows() {
    filtered.sort((A, B) => {
      const a = A[sortKey]; const b = B[sortKey];
      const na = numish(a), nb = numish(b);
      let cmp = 0;
      if (na != null && nb != null) { cmp = na - nb; }
      else { cmp = String(a ?? '').localeCompare(String(b ?? '')); }
      return cmp * sortDir;
    });
  }

  // ---------- BOOT ----------
  function buildRows(json) {
    // Map API records to table rows
    // Fields per https://www.geoboundaries.org/api.html (boundaryName, boundaryISO, boundaryType, boundaryYearRepresented, boundaryCanonical, boundaryLicense, licenseSource, staticDownloadLink, gjDownloadURL, tjDownloadURL, simplifiedGeometryGeoJSON, imagePreview, Continent, admUnitCount)
    rows = json.map(x => {
      const country = nz(x.boundaryName);
      const iso = nz(x.boundaryISO);
      const adm = nz(x.boundaryType);
      const year = nz(x.boundaryYearRepresented);
      const canon = nz(x.boundaryCanonical);
      const lic = nz(x.boundaryLicense);
      const srcURL = nz(x.licenseSource);
      const src = srcURL ? a(srcURL, 'Source') : '';
      const zip = a(nz(x.staticDownloadLink), 'ZIP');
      const gj = a(nz(x.gjDownloadURL), 'GeoJSON');
      const tj = a(nz(x.tjDownloadURL), 'TopoJSON');
      const simp = a(nz(x.simplifiedGeometryGeoJSON), 'Simplified');
      const prev = a(nz(x.imagePreview), 'Preview');
      const units = nz(x.admUnitCount);

      return {
        Country: country,
        ISO3: iso,
        ADM: adm,
        Year: year,
        'Canonical Type': canon,
        License: lic,
        Source: src,
        ZIP: zip,
        GeoJSON: gj,
        TopoJSON: tj,
        Simplified: simp,
        Preview: prev,
        Units: units,
        Continent: nz(x.Continent) // not shown but used for filter
      };
    });

    // Init filters
    uniqueSorted(rows.map(r => r.ADM)).forEach(v => { if (v) selAdm.insertAdjacentHTML('beforeend', `<option value="${esc(v)}">${esc(v)}</option>`); });
    uniqueSorted(rows.map(r => r.Continent)).forEach(v => { if (v) selCont.insertAdjacentHTML('beforeend', `<option value="${esc(v)}">${esc(v)}</option>`); });

    applyFilters();
    sortRows();
    renderHeader();
    renderBody();
  }

  function attachEvents() {
    search.addEventListener('input', () => { applyFilters(); sortRows(); page = 1; renderBody(); });
    selAdm.addEventListener('change', () => { applyFilters(); sortRows(); page = 1; renderBody(); });
    selCont.addEventListener('change', () => { applyFilters(); sortRows(); page = 1; renderBody(); });
    btnPrev.addEventListener('click', () => { page = Math.max(1, page - 1); renderBody(); });
    btnNext.addEventListener('click', () => { page = page + 1; renderBody(); });
    btnCSV.addEventListener('click', () => {
      const csv = toCSV(filtered, columns);
      const blob = new Blob([csv], {type: 'text/csv;charset=utf-8;'});
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'geoboundaries_downloads.csv';
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    });
  }

  async function init() {
    // Lightweight loading state
    tbody.innerHTML = '<tr><td>Loading…</td></tr>';
    try {
      const res = await fetch(API_URL, { headers: { 'Accept': 'application/json' }, cache: 'no-store' });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      raw = Array.isArray(data) ? data : [];
      buildRows(raw);
    } catch (err) {
      tbody.innerHTML = `<tr><td>Failed to load API: ${esc(err.message || err)}</td></tr>`;
    }
  }

  attachEvents();
  init();
})();
</script>

<style>
  /* Controls */
  .gb-controls { display:flex; flex-wrap:wrap; gap:.6rem; align-items:center; margin:.5rem 0 1rem; }
  .gb-controls input[type="search"], .gb-controls select {
    padding:.55rem .7rem; border:1px solid #dcdcdc; border-radius:10px; font:inherit;
  }
  #gb-download-csv {
    padding:.55rem .8rem; border-radius:10px; border:1px solid #115740; background:#115740; color:#fff; font-weight:700; cursor:pointer;
  }
  #gb-download-csv:hover { filter:brightness(1.05); }
  #gb-count { margin-left:auto; opacity:.7; font-size:.9rem; }

  /* Table */
  .gb-table-wrap { overflow:auto; border:1px solid #eee; border-radius:12px; background:#fff; }
  #gb-table { width:100%; border-collapse:collapse; font-size:.95rem; }
  #gb-table thead th { position:sticky; top:0; background:#fff; z-index:1; cursor:pointer; user-select:none; white-space:nowrap; }
  #gb-table th, #gb-table td { padding:.55rem .7rem; border-bottom:1px solid #f0f0f0; vertical-align:top; }
  #gb-table tbody tr:nth-child(odd) { background:#fafafa; }
  #gb-table td.link a { font-weight:700; text-decoration:none; }
  #gb-table td.link a:hover { text-decoration:underline; }

  /* Pager */
  .gb-pager { display:flex; gap:.6rem; align-items:center; justify-content:center; margin:.9rem 0; }
  .gb-pager button { padding:.45rem .8rem; border:1px solid #dcdcdc; border-radius:10px; background:#fff; cursor:pointer; }
  .gb-pager button[disabled] { opacity:.45; cursor:not-allowed; }
</style>
