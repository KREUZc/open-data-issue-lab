const DATA_URL = './data/issues/energy.json';

const $ = (selector) => document.querySelector(selector);

const fmt = new Intl.NumberFormat('zh-TW', {
  maximumFractionDigits: 1,
});

function mw(value) {
  return `${fmt.format(value)} MW`;
}

function tenMw(value) {
  return `${fmt.format(value)} 萬瓩`;
}

function percent(value) {
  return `${fmt.format(value)}%`;
}

function mbFmt(value) {
  const number = Number(value || 0);
  if (number === 0) return '0 MB';
  if (number < 0.1) return `${number.toFixed(4)} MB`;
  if (number < 1) return `${number.toFixed(3)} MB`;
  return `${fmt.format(number)} MB`;
}

function setText(selector, value) {
  const el = $(selector);
  if (el) el.textContent = value;
}

function escapeAttr(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;',
  }[char]));
}

function renderFreshness(data) {
  const pill = $('#freshness-pill');
  if (!pill) return;
  const status = data.metadata?.stale ? 'stale' : 'fresh';
  pill.classList.remove('fresh', 'stale');
  pill.classList.add(status);
  const sourceTime = data.summary?.source_updated_at || data.metadata?.generated_at || '未知時間';
  pill.textContent = status === 'stale'
    ? `STALE / 最後成功資料 ${sourceTime}`
    : `FRESH / 來源時間 ${sourceTime}`;
}

function renderMixBars(data) {
  const target = $('#mix-bars');
  if (!target) return;
  target.innerHTML = '';
  const mix = data.current?.generation_mix || [];
  const noonReference = data.current?.noon_reference || {};
  const noonMix = noonReference.status === 'available' ? noonReference.mix || [] : [];
  const noonById = new Map(noonMix.map((item) => [item.id, item]));
  const maxShare = Math.max(
    ...mix.map((item) => item.share),
    ...noonMix.map((item) => item.share),
    1
  );

  mix.forEach((item) => {
    const row = document.createElement('div');
    row.className = 'mix-row';

    const label = document.createElement('div');
    label.className = 'mix-label';
    label.textContent = item.label;

    const track = document.createElement('div');
    track.className = 'mix-track';
    const noonItem = noonById.get(item.id);
    if (noonItem) {
      const shadow = document.createElement('div');
      shadow.className = 'mix-shadow';
      shadow.style.width = `${Math.max((noonItem.share / maxShare) * 100, 2)}%`;
      track.append(shadow);
    }

    const fill = document.createElement('div');
    fill.className = 'mix-fill';
    fill.style.width = `${Math.max((item.share / maxShare) * 100, 2)}%`;
    fill.style.background = item.color;
    track.append(fill);

    const value = document.createElement('div');
    value.className = 'mix-value';
    value.textContent = `${mw(item.mw)} / ${percent(item.share)}`;

    row.append(label, track, value);
    target.append(row);
  });

  const note = $('#mix-reference-note');
  if (!note) return;
  note.textContent = noonReference.status === 'available'
    ? `淡色陰影：${noonReference.source_updated_at} 中午分佈`
    : '淡色陰影待今日 12:00 pipeline 捕捉；目前不以月資料替代。';
}

function findMix(data, id) {
  return (data.current?.generation_mix || []).find((item) => item.id === id);
}

function renderMetrics(data) {
  const gas = findMix(data, 'gas')?.mw || 0;
  const coal = findMix(data, 'coal')?.mw || 0;
  const total = data.summary?.current_total_generation_mw || 0;
  const share = total ? ((gas + coal) / total) * 100 : 0;

  setText('#hero-answer', data.summary?.answer || '目前沒有可用的能源摘要。');
  setText('#current-total', mw(total));
  setText('#gas-coal-metric', `${percent(share)} / ${mw(gas + coal)}`);
  setText('#load-metric', tenMw(data.summary?.current_load_10mw || 0));
  setText('#reserve-metric', percent(data.summary?.forecast_reserve_rate || 0));
  setText('#reserve-caption', `預估尖峰時段 ${data.summary?.forecast_peak_hour_range || '未提供'}`);
  setText('#download-metric', mbFmt(data.pipeline_summary?.total_downloaded_mb || 0));
}

function linePath(points) {
  return points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(' ');
}

function chartScale(items, keys, width, height, padding) {
  const values = [];
  items.forEach((item) => keys.forEach((key) => values.push(Number(item[key] || 0))));
  const min = Math.min(...values, 0);
  const max = Math.max(...values, 1);
  const span = max - min || 1;
  const xStep = items.length > 1 ? (width - padding.left - padding.right) / (items.length - 1) : 0;
  const y = (value) => height - padding.bottom - ((value - min) / span) * (height - padding.top - padding.bottom);
  const x = (index) => padding.left + index * xStep;
  return { min, max, x, y };
}

function chartTicks(scale, count = 4) {
  if (count <= 1) return [scale.max];
  const step = (scale.max - scale.min) / (count - 1);
  return Array.from({ length: count }, (_, index) => scale.min + step * index);
}

function renderYAxis(scale, padding, width) {
  return chartTicks(scale).map((value) => {
    const y = scale.y(value);
    return `
      <line class="chart-gridline" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
      <text class="axis-tick" x="${padding.left - 10}" y="${y + 4}" text-anchor="end">${fmt.format(value)}</text>
    `;
  }).join('');
}

function renderLineChart(targetSelector, items, key, color, unitLabel) {
  const target = $(targetSelector);
  if (!target || !items.length) return;

  const width = 760;
  const height = 310;
  const padding = { top: 28, right: 28, bottom: 44, left: 70 };
  const scale = chartScale(items, [key], width, height, padding);
  const points = items.map((item, index) => ({ x: scale.x(index), y: scale.y(item[key]) }));
  const last = items[items.length - 1];

  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="presentation" aria-hidden="true">
      <rect x="0" y="0" width="${width}" height="${height}" fill="#111"></rect>
      ${renderYAxis(scale, padding, width)}
      <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" stroke="#353535"></line>
      <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" stroke="#353535"></line>
      <text class="axis-text" x="${padding.left}" y="18">${escapeAttr(unitLabel)}</text>
      <text class="axis-text" x="${padding.left}" y="${height - 14}">${escapeAttr(items[0].date)}</text>
      <text class="axis-text" x="${width - padding.right - 86}" y="${height - 14}">${escapeAttr(last.date)}</text>
      <path d="${linePath(points)}" fill="none" stroke="${color}" stroke-width="3"></path>
      ${points.map((point) => `<circle cx="${point.x}" cy="${point.y}" r="4" fill="${color}"></circle>`).join('')}
    </svg>
  `;
}

function renderMultiLineChart(targetSelector, items) {
  const target = $(targetSelector);
  if (!target || !items.length) return;

  const width = 900;
  const height = 330;
  const padding = { top: 28, right: 32, bottom: 50, left: 70 };
  const series = [
    { key: 'solar_10mw', label: '太陽', color: '#ffd700' },
    { key: 'wind_10mw', label: '風力', color: '#2e8b57' },
    { key: 'gas_10mw', label: '燃氣', color: '#ff8c00' },
    { key: 'coal_10mw', label: '燃煤', color: '#858585' },
  ];
  const scale = chartScale(items, series.map((item) => item.key), width, height, padding);
  const paths = series.map((serie) => {
    const points = items.map((item, index) => ({ x: scale.x(index), y: scale.y(item[serie.key]) }));
    return `<path d="${linePath(points)}" fill="none" stroke="${serie.color}" stroke-width="2.6"></path>`;
  }).join('');

  const legend = series.map((serie, index) => {
    const x = padding.left + index * 84;
    return `
      <g transform="translate(${x}, ${height - 20})">
        <rect width="16" height="3" y="-8" fill="${serie.color}"></rect>
        <text class="axis-text" x="22" y="-4">${serie.label}</text>
      </g>
    `;
  }).join('');

  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="presentation" aria-hidden="true">
      <rect x="0" y="0" width="${width}" height="${height}" fill="#111"></rect>
      ${renderYAxis(scale, padding, width)}
      <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" stroke="#353535"></line>
      <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" stroke="#353535"></line>
      <text class="axis-text" x="${padding.left}" y="18">萬瓩</text>
      <text class="axis-text" x="${padding.left}" y="${height - 34}">${escapeAttr(items[0].date)}</text>
      <text class="axis-text" x="${width - padding.right - 86}" y="${height - 34}">${escapeAttr(items[items.length - 1].date)}</text>
      ${paths}
      ${legend}
    </svg>
  `;
}

function renderPipeline(data) {
  const summary = data.pipeline_summary || {};
  setText(
    '#pipeline-summary',
    `本次下載 ${mbFmt(summary.total_downloaded_mb || 0)}，解析 ${fmt.format(summary.total_parsed_records || 0)} 筆；另有 ${mbFmt(summary.skipped_resource_content_length_mb || 0)} 大型候選資料只記錄、不下載。`
  );

  const list = $('#source-list');
  if (!list) return;
  list.innerHTML = '';

  (data.sources || []).forEach((source) => {
    const row = document.createElement('article');
    row.className = 'source-row';
    row.innerHTML = `
      <div>
        <strong>${escapeAttr(source.name)}</strong>
        <a href="${escapeAttr(source.official_page)}" target="_blank" rel="noreferrer">data.gov.tw dataset ${escapeAttr(source.dataset_id)}</a>
      </div>
      <span class="source-status ${escapeAttr(source.status)}">${escapeAttr(source.status)}</span>
      <span>${mbFmt(source.downloaded_mb || 0)} down</span>
      <span>${fmt.format(source.parsed_records || 0)} rows</span>
      <span>${source.status === 'skipped' ? `${mbFmt(source.content_length_mb || 0)} skipped` : escapeAttr(source.parser)}</span>
    `;
    list.append(row);
  });

  const limits = $('#limits-list');
  if (!limits) return;
  limits.innerHTML = '';
  (data.limits || []).forEach((item) => {
    const li = document.createElement('li');
    li.textContent = item;
    limits.append(li);
  });
}

function render(data) {
  renderFreshness(data);
  renderMetrics(data);
  renderMixBars(data);
  renderLineChart('#seven-day-chart', data.daily?.seven_days || [], 'peak_load_10mw', '#4da3ff', '尖峰負載 / 萬瓩');
  renderMultiLineChart('#thirty-day-chart', data.daily?.thirty_days || []);
  renderPipeline(data);
}

function showError(error) {
  setText('#hero-answer', '資料讀取失敗。若 pipeline 有最後成功輸出，網站會在下一次部署時標示 stale 並沿用。');
  const target = $('#mix-bars');
  if (target) {
    target.innerHTML = `<div class="error-panel">${escapeAttr(error.message || error)}</div>`;
  }
  setText('#pipeline-summary', `資料讀取失敗：${error.message || error}`);
}

fetch(DATA_URL, { cache: 'no-store' })
  .then((response) => {
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
  })
  .then(render)
  .catch(showError);
