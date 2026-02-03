/**
 * Update declustered flood peaks (NAVD88) for Sea Bright, NJ
 * - Pulls recent/ongoing observations from USGS IV
 * - Optionally merges/declusters from an existing cache JSON
 *
 * Sea Bright gauge:
 * - NWPS gauge id: SBIN4
 * - USGS site: 01407600
 *
 * NOAA tide station (for nearby tide context, if you use it elsewhere):
 * - 8531680 (Sandy Hook, NJ)
 */

import fs from "node:fs";
import path from "node:path";

// =====================
// Sea Bright constants
// =====================
const PLACE_NAME = "Sea Bright, NJ";
const SITE_SLUG = "seabright";

// USGS Sea Bright / Shrewsbury River at Sea Bright
const USGS_SITE = "01407600";

// USGS IV param for water level (ft)
const USGS_PARAM = "72279";

// Cache location
const OUT_JSON = process.env.OUT_JSON || `data/${SITE_SLUG}_peaks_navd88.json`;

// Declustering window (minutes)
const PEAK_MIN_SEP_MINUTES = Number(process.env.PEAK_MIN_SEP_MINUTES || 300);

// How far back to pull from USGS IV (days)
const LOOKBACK_DAYS = Number(process.env.LOOKBACK_DAYS || 14);

// =====================
// Helpers
// =====================
function isoNow() {
  return new Date().toISOString();
}

function parseArgs() {
  const args = process.argv.slice(2);
  const out = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = args[i + 1];
      out[k] = v;
      i++;
    }
  }
  return out;
}

function safeReadJson(fp) {
  if (!fs.existsSync(fp)) return null;
  try {
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return null;
  }
}

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function toEpochMs(iso) {
  return new Date(iso).getTime();
}

function uniqSortByTime(points) {
  const seen = new Set();
  const out = [];
  for (const p of points) {
    const key = `${p.t}|${p.v}`;
    if (!seen.has(key)) {
      seen.add(key);
      out.push(p);
    }
  }
  out.sort((a, b) => toEpochMs(a.t) - toEpochMs(b.t));
  return out;
}

/**
 * Declustering: keep only the max value within each "cluster"
 * where points are within PEAK_MIN_SEP_MINUTES of each other.
 */
function declusterMax(points, minSepMinutes) {
  if (!points.length) return [];
  const minSepMs = minSepMinutes * 60 * 1000;

  const pts = [...points].sort((a, b) => toEpochMs(a.t) - toEpochMs(b.t));

  const clusters = [];
  let cur = [pts[0]];

  for (let i = 1; i < pts.length; i++) {
    const prev = pts[i - 1];
    const p = pts[i];
    const dt = toEpochMs(p.t) - toEpochMs(prev.t);
    if (dt <= minSepMs) cur.push(p);
    else {
      clusters.push(cur);
      cur = [p];
    }
  }
  clusters.push(cur);

  // pick max in each cluster
  const peaks = clusters.map((c) => {
    let max = c[0];
    for (const p of c) if (p.v > max.v) max = p;
    return max;
  });

  // Sort by time and return
  peaks.sort((a, b) => toEpochMs(a.t) - toEpochMs(b.t));
  return peaks;
}

// =====================
// USGS IV fetch (NAVD88 water level)
// =====================
async function fetchUsgsIvPoints({ site, param, lookbackDays }) {
  const end = new Date();
  const start = new Date(end.getTime() - lookbackDays * 24 * 3600 * 1000);

  // USGS IV expects YYYY-MM-DD
  const startStr = start.toISOString().slice(0, 10);
  const endStr = end.toISOString().slice(0, 10);

  const url =
    `https://waterservices.usgs.gov/nwis/iv/?format=json` +
    `&sites=${site}` +
    `&parameterCd=${param}` +
    `&startDT=${startStr}` +
    `&endDT=${endStr}`;

  const res = await fetch(url, { headers: { "User-Agent": `${PLACE_NAME} Peaks Updater` } });
  if (!res.ok) throw new Error(`USGS IV fetch failed: ${res.status} ${res.statusText}`);

  const json = await res.json();

  const ts = json?.value?.timeSeries?.[0];
  const values = ts?.values?.[0]?.value || [];

  // USGS returns strings
  const points = values
    .map((d) => ({
      t: d?.dateTime,
      v: d?.value == null ? null : Number(d.value),
    }))
    .filter((p) => p.t && Number.isFinite(p.v));

  return points;
}

// =====================
// Main
// =====================
async function main() {
  const argv = parseArgs();

  const outJson = argv.out || OUT_JSON;

  const existing = safeReadJson(outJson);

  const existingPoints = Array.isArray(existing?.obs_points_navd88)
    ? existing.obs_points_navd88
    : [];

  const ivPoints = await fetchUsgsIvPoints({
    site: USGS_SITE,
    param: USGS_PARAM,
    lookbackDays: LOOKBACK_DAYS,
  });

  const merged = uniqSortByTime([...existingPoints, ...ivPoints]);

  // Peaks
  const peaks = declusterMax(merged, PEAK_MIN_SEP_MINUTES);

  const payload = {
    place: PLACE_NAME,
    site_slug: SITE_SLUG,
    usgs_site: USGS_SITE,
    usgs_parameter: USGS_PARAM,
    datum: "NAVD88",
    peakMinSepMinutes: PEAK_MIN_SEP_MINUTES,
    updated_utc: isoNow(),
    obs_points_navd88: merged,
    peaks_navd88: peaks,
  };

  writeJson(outJson, payload);

  console.log(`[ok] wrote ${outJson}`);
  console.log(`[info] merged points: ${merged.length}`);
  console.log(`[info] peaks: ${peaks.length}`);
}

main().catch((e) => {
  console.error("[fatal]", e);
  process.exit(1);
});
