/**
 * Sea Bright (SBIN4) peak updater
 * Pulls NWPS observed + forecast stage (in MLLW) and writes peaks in NAVD88
 *
 * Output: data/seabright_peaks_navd88.json
 */

import fs from "fs";
import path from "path";
import process from "process";

const SITE = {
  name: "Shrewsbury River at Sea Bright",
  gaugeId: "SBIN4",

  // Sea Bright datum table implies: NAVD88 = MLLW - 2.10 ft
  mllwToNavd88OffsetFt: 2.10,

  // Flood thresholds in NAVD88 (from your screenshot table)
  thresholdsNavd88: {
    minor: 3.10,
    moderate: 4.10,
    major: 5.10,
  },

  outFile: "data/seabright_peaks_navd88.json",
};

const NWPS_BASE = `https://api.water.noaa.gov/nwps/v1/gauges/${SITE.gaugeId}`;
const OBS_URL = `${NWPS_BASE}/stageflow/observed`;
const FCST_URL = `${NWPS_BASE}/stageflow/forecast`;

function toNavd88(mllwFt) {
  return mllwFt - SITE.mllwToNavd88OffsetFt;
}

function classify(navd88Ft) {
  if (navd88Ft >= SITE.thresholdsNavd88.major) return "major";
  if (navd88Ft >= SITE.thresholdsNavd88.moderate) return "moderate";
  if (navd88Ft >= SITE.thresholdsNavd88.minor) return "minor";
  return "none";
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { "User-Agent": "peak-updater" } });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.json();
}

/**
 * NWPS stageflow payloads typically contain a time series of stage values.
 * Since exact field names can vary, we normalize by searching for arrays of points.
 *
 * This function tries common patterns:
 * - obj.data[] with { validTime / time, stage }
 * - obj.timeseries[] with points
 * - obj.observed / obj.forecast arrays
 */
function extractPoints(payload) {
  // Try a few common shapes
  const candidates = [];

  // 1) payload.data
  if (Array.isArray(payload?.data)) candidates.push(payload.data);

  // 2) payload.observed / payload.forecast
  if (Array.isArray(payload?.observed)) candidates.push(payload.observed);
  if (Array.isArray(payload?.forecast)) candidates.push(payload.forecast);

  // 3) payload.timeseries[0].data or .points
  if (Array.isArray(payload?.timeseries)) {
    for (const ts of payload.timeseries) {
      if (Array.isArray(ts?.data)) candidates.push(ts.data);
      if (Array.isArray(ts?.points)) candidates.push(ts.points);
    }
  }

  // Flatten candidate arrays into normalized points
  const points = [];

  for (const arr of candidates) {
    for (const row of arr) {
      const t =
        row?.validTime ??
        row?.time ??
        row?.t ??
        row?.dateTime ??
        row?.timestamp;

      const stage =
        row?.stage ??
        row?.value ??
        row?.v ??
        row?.primary ??
        row?.primaryValue;

      if (!t || stage === undefined || stage === null) continue;

      const dt = new Date(t);
      const v = Number(stage);
      if (!Number.isFinite(dt.getTime())) continue;
      if (!Number.isFinite(v)) continue;

      points.push({ t: dt, mllw: v, navd88: toNavd88(v) });
    }
  }

  // Sort + de-dupe by time
  points.sort((a, b) => a.t - b.t);
  const deduped = [];
  let lastMs = null;
  for (const p of points) {
    const ms = p.t.getTime();
    if (ms === lastMs) continue;
    deduped.push(p);
    lastMs = ms;
  }

  return deduped;
}

/**
 * Find local maxima with:
 * - minimum separation (hours) to avoid multiple tiny bumps
 * - minimum prominence (ft) relative to surrounding neighborhood
 */
function findPeaks(points, { minSeparationHours = 5, minProminenceFt = 0.05 } = {}) {
  const peaks = [];

  // helper: neighborhood window
  const windowPts = 6; // ~6 points on each side; works for 15–60 min spacing

  for (let i = 1; i < points.length - 1; i++) {
    const p = points[i];
    const prev = points[i - 1];
    const next = points[i + 1];

    // basic local max
    if (!(p.navd88 >= prev.navd88 && p.navd88 >= next.navd88)) continue;

    // prominence: compare to min around it
    const lo = Math.max(0, i - windowPts);
    const hi = Math.min(points.length - 1, i + windowPts);
    let localMin = Infinity;
    for (let j = lo; j <= hi; j++) localMin = Math.min(localMin, points[j].navd88);

    if (p.navd88 - localMin < minProminenceFt) continue;

    // separation: keep only one peak within minSeparationHours
    const minSepMs = minSeparationHours * 3600 * 1000;
    const last = peaks[peaks.length - 1];
    if (last && p.t.getTime() - last.t.getTime() < minSepMs) {
      // keep the higher one
      if (p.navd88 > last.navd88) peaks[peaks.length - 1] = p;
      continue;
    }

    peaks.push(p);
  }

  return peaks;
}

function loadExisting(outPath) {
  if (!fs.existsSync(outPath)) return [];
  try {
    const raw = fs.readFileSync(outPath, "utf8");
    const j = JSON.parse(raw);
    return Array.isArray(j) ? j : (Array.isArray(j?.peaks) ? j.peaks : []);
  } catch {
    return [];
  }
}

function mergeByTime(existing, incoming) {
  const map = new Map();
  for (const e of existing) map.set(e.time, e);

  for (const p of incoming) {
    const time = p.t.toISOString();
    const row = {
      time,
      navd88_ft: Number(p.navd88.toFixed(2)),
      mllw_ft: Number(p.mllw.toFixed(2)),
      category: classify(p.navd88),
      source: p.source,
    };

    // Prefer observed over forecast if timestamps collide
    if (!map.has(time)) map.set(time, row);
    else {
      const cur = map.get(time);
      if (cur.source === "forecast" && row.source === "observed") map.set(time, row);
      else if (row.navd88_ft > cur.navd88_ft) map.set(time, row);
    }
  }

  const merged = Array.from(map.values()).sort((a, b) => new Date(a.time) - new Date(b.time));
  return merged;
}

async function main() {
  const repoRoot = process.cwd();
  const outPath = path.join(repoRoot, SITE.outFile);

  console.log(`Fetching NWPS observed: ${OBS_URL}`);
  const obsPayload = await fetchJson(OBS_URL);
  const obsPoints = extractPoints(obsPayload).map(p => ({ ...p, source: "observed" }));

  console.log(`Fetching NWPS forecast: ${FCST_URL}`);
  const fcstPayload = await fetchJson(FCST_URL);
  const fcstPoints = extractPoints(fcstPayload).map(p => ({ ...p, source: "forecast" }));

  // Find peaks in each
  const obsPeaks = findPeaks(obsPoints).map(p => ({ ...p, source: "observed" }));
  const fcstPeaks = findPeaks(fcstPoints).map(p => ({ ...p, source: "forecast" }));

  // Keep only meaningful peaks (>= minor) OR keep all (your choice)
  const keep = (p) => p.navd88 >= SITE.thresholdsNavd88.minor;
  const peaksToWrite = [...obsPeaks.filter(keep), ...fcstPeaks.filter(keep)];

  // Merge with existing
  const existing = loadExisting(outPath);
  const merged = mergeByTime(existing, peaksToWrite);

  // Write
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(merged, null, 2) + "\n", "utf8");

  console.log(`Wrote ${merged.length} peaks -> ${SITE.outFile}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
