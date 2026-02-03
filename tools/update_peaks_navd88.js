#!/usr/bin/env node
/**
 * Crest-anchored NAVD88 "high tide events" builder for Sea Bright
 * - USGS IV: site 01407600, param 72279 (tidal elevation)
 * - NOAA CO-OPS tide-clock: station 8531804 (hilo highs; type=H) for crest times
 *
 * Writes to: data/peaks_navd88.json
 *
 * Modes:
 *   node tools/update_peaks_navd88.js
 *     -> incremental update from lastProcessedISO (with buffer) to now
 *
 *   node tools/update_peaks_navd88.js --backfill-year=2000
 *     -> backfill exactly that calendar year (UTC)
 *
 *   node tools/update_peaks_navd88.js --backfill-from=2000 --backfill-to=2026
 *     -> backfill inclusive year range (UTC)
 */

const fs = require("fs");
const path = require("path");

// -------------------------
// Config (Sea Bright)
// -------------------------
const CACHE_PATH = path.join(__dirname, "..", "data", "peaks_navd88.json");

const SITE = "01407600";              // USGS Sea Bright
const PARAM = "72279";                // tidal elevation
const NOAA_STATION = "8531804";       // NOAA CO-OPS Sea Bright tide predictions (harmonics)

const TZ = "Etc/GMT+5";               // your dashboard uses fixed EST; leave consistent

// Flood thresholds (NAVD88) for classification
const THRESH_NAVD88 = {
  minorLow: 3.10,
  moderateLow: 4.10,
  majorLow: 5.10
};

// Keep for transparency / compatibility
const PEAK_MIN_SEP_MINUTES = 300;

// Incremental overlap so boundary crests don't get missed
const BUFFER_HOURS = 12;

// Crest anchoring rules (your method)
const CREST_WINDOW_HOURS = 2;      // search max within ±2h of predicted crest
const REQUIRE_WITHIN_HOURS = 1;    // if NO obs points within ±1h, skip that crest entirely

const METHOD = "crest_anchored_highs_v1_seabright";

// -------------------------
// Helpers
// -------------------------
function die(msg) {
  console.error(msg);
  process.exit(1);
}

function loadJSON(p) {
  if (!fs.existsSync(p)) die(`Missing cache file: ${p}`);
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function saveJSON(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function isoNow() {
  return new Date().toISOString();
}

function addHoursISO(iso, hours) {
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return null;
  return new Date(t + hours * 3600 * 1000).toISOString();
}

function clampISO(iso) {
  const t = new Date(iso).getTime();
  return Number.isFinite(t) ? new Date(t).toISOString() : null;
}

function parseArg(name) {
  const a = process.argv.find(x => x.startsWith(name + "="));
  return a ? a.split("=").slice(1).join("=") : null;
}

function roundFt(x) {
  return Math.round(x * 1000) / 1000;
}

function parseNOAATimeToISO_UTC(t) {
  // NOAA predictions return "YYYY-MM-DD HH:MM" (no timezone).
  // We request time_zone=gmt so interpret as UTC and append Z.
  return t.replace(" ", "T") + ":00Z";
}

function classifyNAVD(ft, T) {
  let type = "Below";
  if (ft >= T.majorLow) type = "Major";
  else if (ft >= T.moderateLow) type = "Moderate";
  else if (ft >= T.minorLow) type = "Minor";
  return type;
}

// -------------------------
// USGS IV fetch (15-min-ish)
// -------------------------
async function fetchUSGSIV({ startISO, endISO }) {
  const url =
    "https://waterservices.usgs.gov/nwis/iv/?" +
    new URLSearchParams({
      format: "json",
      sites: SITE,
      parameterCd: PARAM,
      startDT: startISO,
      endDT: endISO,
      siteStatus: "all",
      agencyCd: "USGS"
    }).toString();

  const res = await fetch(url, { headers: { "User-Agent": "seabright-peaks-updater" } });
  if (!res.ok) throw new Error(`USGS IV HTTP ${res.status}: ${url}`);
  const j = await res.json();

  const ts = j?.value?.timeSeries?.[0];
  const vals = ts?.values?.[0]?.value || [];

  const out = [];
  for (const v of vals) {
    const t = v?.dateTime;
    const ft = Number(v?.value);
    if (!t || !Number.isFinite(ft)) continue;
    out.push({ t: new Date(t).toISOString(), ft });
  }

  out.sort((a, b) => new Date(a.t) - new Date(b.t));
  return out;
}

// -------------------------
// NOAA predicted highs fetch
// -------------------------
async function fetchNOAAHiloPredictionsHighs({ startISO, endISO }) {
  // NOAA endpoint uses begin_date/end_date as YYYYMMDD and time_zone=gmt
  function yyyymmdd(iso) {
    const d = new Date(iso);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const day = String(d.getUTCDate()).padStart(2, "0");
    return `${y}${m}${day}`;
  }

  const begin_date = yyyymmdd(startISO);
  const end_date = yyyymmdd(endISO);

  const url =
    "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?" +
    new URLSearchParams({
      product: "predictions",
      application: "seabright-dashboard",
      begin_date,
      end_date,
      datum: "MLLW",
      station: NOAA_STATION,
      time_zone: "gmt",
      interval: "hilo",
      units: "english",
      format: "json"
    }).toString();

  const res = await fetch(url, { headers: { "User-Agent": "seabright-peaks-updater" } });
  if (!res.ok) throw new Error(`NOAA predictions HTTP ${res.status}: ${url}`);
  const j = await res.json();

  const preds = Array.isArray(j?.predictions) ? j.predictions : [];
  const highs = preds
    .filter(p => String(p?.type || "").toUpperCase() === "H")
    .map(p => ({ t: parseNOAATimeToISO_UTC(p.t) }))
    .filter(p => p.t);

  highs.sort((a, b) => new Date(a.t) - new Date(b.t));
  return highs;
}

// -------------------------
// Build crest-anchored events
// -------------------------
function buildCrestAnchoredHighEvents({ series, predictedHighs, thresholdsNAVD88 }) {
  const pts = series.slice().sort((a, b) => new Date(a.t) - new Date(b.t));
  const out = [];

  const w2 = CREST_WINDOW_HOURS * 3600 * 1000;
  const w1 = REQUIRE_WITHIN_HOURS * 3600 * 1000;

  let left = 0;

  for (const h of predictedHighs) {
    const crestISO = h.t;
    const crestMs = new Date(crestISO).getTime();
    if (!Number.isFinite(crestMs)) continue;

    while (left < pts.length) {
      const tMs = new Date(pts[left].t).getTime();
      if (!Number.isFinite(tMs) || tMs < crestMs - w2) left++;
      else break;
    }

    let i = left;
    let hasWithin1h = false;
    let best = null;

    while (i < pts.length) {
      const tMs = new Date(pts[i].t).getTime();
      if (!Number.isFinite(tMs)) { i++; continue; }
      if (tMs > crestMs + w2) break;

      const dt = Math.abs(tMs - crestMs);
      if (dt <= w1) hasWithin1h = true;

      if (!best || pts[i].ft > best.ft) best = pts[i];
      i++;
    }

    if (!hasWithin1h) continue;
    if (!best) continue;

    const ft = Number(best.ft);
    out.push({
      t: new Date(best.t).toISOString(),          // observed time of window max
      ft: roundFt(ft),
      type: classifyNAVD(ft, thresholdsNAVD88),
      crest: new Date(crestISO).toISOString(),    // predicted crest time (stable key)
      kind: "CrestHigh"
    });
  }

  return out;
}

// -------------------------
// Main update logic
// -------------------------
async function main() {
  const cache = loadJSON(CACHE_PATH);

  cache.site = SITE;
  cache.parameterCd = PARAM;
  cache.noaaStation = NOAA_STATION;
  cache.method = METHOD;
  cache.peakMinSepMinutes = PEAK_MIN_SEP_MINUTES;
  cache.thresholdsNAVD88 = THRESH_NAVD88;
  cache.updated_utc = isoNow();

  let startISO, endISO;

  const backfillYear = parseArg("--backfill-year");
  const backfillFrom = parseArg("--backfill-from");
  const backfillTo = parseArg("--backfill-to");

  if (backfillYear) {
    const y = Number(backfillYear);
    if (!Number.isFinite(y)) die("Invalid --backfill-year");
    startISO = new Date(Date.UTC(y, 0, 1, 0, 0, 0)).toISOString();
    endISO = new Date(Date.UTC(y + 1, 0, 1, 0, 0, 0)).toISOString();
    console.log(`Backfill year ${y}: ${startISO} → ${endISO}`);
  } else if (backfillFrom && backfillTo) {
    const lo = Number(backfillFrom);
    const hi = Number(backfillTo);
    if (!Number.isFinite(lo) || !Number.isFinite(hi) || hi < lo) die("Invalid --backfill-from/to");
    startISO = new Date(Date.UTC(lo, 0, 1, 0, 0, 0)).toISOString();
    endISO = new Date(Date.UTC(hi + 1, 0, 1, 0, 0, 0)).toISOString();
    console.log(`Backfill years ${lo}–${hi}: ${startISO} → ${endISO}`);
  } else {
    const last = clampISO(cache.lastProcessedISO || "2000-01-01T00:00:00Z");
    if (!last) die("Cache lastProcessedISO is invalid ISO.");
    startISO = addHoursISO(last, -BUFFER_HOURS);
    endISO = isoNow();
    console.log(`Incremental: ${startISO} → ${endISO}`);
  }

  const series = await fetchUSGSIV({ startISO, endISO });
  if (!series.length) {
    console.log("No USGS IV points returned; nothing to do.");
    return;
  }

  const predStartISO = addHoursISO(startISO, -3);
  const predEndISO = addHoursISO(endISO, +3);

  const predictedHighs = await fetchNOAAHiloPredictionsHighs({ startISO: predStartISO, endISO: predEndISO });
  if (!predictedHighs.length) {
    console.log("No NOAA predicted highs returned; nothing to do.");
    return;
  }

  const crestHighs = buildCrestAnchoredHighEvents({
    series,
    predictedHighs,
    thresholdsNAVD88: THRESH_NAVD88
  });

  const existing = Array.isArray(cache.events) ? cache.events : [];
  const byCrest = new Map();
  for (const e of existing) if (e?.crest) byCrest.set(String(e.crest), e);

  let added = 0;
  let updated = 0;

  for (const e of crestHighs) {
    const key = String(e.crest);
    const prev = byCrest.get(key);

    if (!prev) {
      existing.push(e);
      byCrest.set(key, e);
      added++;
      continue;
    }

    const prevFt = Number(prev.ft);
    const newFt = Number(e.ft);

    if (!Number.isFinite(prevFt) || (Number.isFinite(newFt) && newFt > prevFt)) {
      prev.t = e.t;
      prev.ft = e.ft;
      prev.type = e.type;
      prev.kind = e.kind;
      prev.crest = e.crest;
      updated++;
    }
  }

  existing.sort((a, b) => new Date(a.t) - new Date(b.t));
  cache.events = existing;

  const newestT = series[series.length - 1]?.t;
  if (newestT) cache.lastProcessedISO = new Date(newestT).toISOString();

  saveJSON(CACHE_PATH, cache);

  console.log(`Fetched USGS points:         ${series.length}`);
  console.log(`NOAA predicted HIGH crests:  ${predictedHighs.length}`);
  console.log(`Crest-anchored events built: ${crestHighs.length}`);
  console.log(`Events added:               ${added}`);
  console.log(`Events updated:             ${updated}`);
  console.log(`New lastProcessedISO:       ${cache.lastProcessedISO}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
