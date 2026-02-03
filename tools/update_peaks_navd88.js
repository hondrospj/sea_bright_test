#!/usr/bin/env node
/**
 * Crest-anchored NAVD88 "high tide events" builder for USGS 01407600 (param 72279)
 * - Uses NOAA CO-OPS predicted HIGH tide crest times (interval=hilo, type=H) as the "tide clock"
 * - For each predicted HIGH tide crest:
 *    - Search observed USGS IV points within ±2 hours and take the MAX
 *    - BUT: if there are ZERO observed points within ±1 hour of the crest, SKIP that crest entirely
 *
 * Writes to: data/pea:contentReference[oaicite:20]{index=20}*   node tools/update_peaks_navd88.js
 *     -> incremental update from lastProcessedISO (with buffer) to now
 *
 *   node tools/update_peaks_navd88.js --backfill-year=2000
 *     -> backfill exactly that calendar year (UTC)
 *
 *   node tools/update_peaks_navd88.js --backfill-from=2000 --backfill-to=2026
 *     -> backfill inclusive year range (UTC)
 */

"use strict";

const fs = require("fs");
const path = require("path");

const CACHE_PATH = path.join(__dirname, "..", "data", "peaks_navd88.json");

const SITE = "01407600";     // USGS Sea Bright, Shrewsbury River
const PARAM = "72279";       // tidal elevation, NOS-averaged, NAVD88

// NOAA tide-clock (predicted highs/lows) — used ONLY for crest times
const NOAA_STATION = "8531804"; // Sea Bright, NJ

const PEAK_MIN_SEP_MINUTES = 300;

// Incremental overlap so boundary crests don't get missed
const BUFFER_HOURS = 12;

// Crest anchoring rules
const CREST_WINDOW_HOURS = 2;      // search max within ±2h of predicted crest
const REQUIRE_WITHIN_HOURS = 1;    // if NO obs points within ±1h, skip that crest entirely

const METHOD = "crest_anchored_highs_v1";

function die(msg) { console.error(msg); process.exit(1); }

function loadJSON(p) {
  if (!fs.existsSync(p)) die(`Missing cache file: ${p}`);
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function saveJSON(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function isoNow() { return new Date().toISOString(); }

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

function roundFt(x) { return Math.round(x * 1000) / 1000; }

function yyyymmddUTC(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

function addDaysUTC(d, days) {
  return new Date(d.getTime() + days * 86400 * 1000);
}

function startOfUTCDate(d) {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 0, 0, 0));
}

function parseNOAATimeToISO_UTC(t) {
  // We request time_zone=gmt so interpret as UTC and append Z.
  // "YYYY-MM-DD HH:MM" -> "YYYY-MM-DDTHH:MM:00Z"
  return t.replace(" ", "T") + ":00Z";
}

function classifyNAVD(ft, T) {
  let type = "Below";
  if (ft >= T.majorLow) type = "Major";
  else if (ft >= T.moderateLow) type = "Moderate";
  else if (ft >= T.minorLow) type = "Minor";
  return type;
}

/* =========================
USGS IV fetch (15-min-ish)
========================= */
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

  const res = await fetch(url, { headers: { "User-Agent": "sea-bright-peaks-cache/1.0" } });
  if (!res.ok) throw new Error(`USGS IV fetch failed: ${res.status} ${res.statusText}`);

  const j = await res.json();

  const ts = j?.value?.timeSeries?.[0];
  const values = ts?.values?.[0]?.value || [];

  const out = [];
  for (const v of values) {
    const t = v?.dateTime;
    const ft = Number(v?.value);
    if (!t) continue;
    if (!Number.isFinite(ft)) continue;
    const ms = new Date(t).getTime();
    if (!Number.isFinite(ms)) continue;
    out.push({ t: new Date(ms).toISOString(), ft });
  }

  out.sort((a, b) => new Date(a.t) - new Date(b.t));
  return out;
}

/* =========================
NOAA hilo predictions (highs only)
========================= */
async function fetchNOAAHiloHighs({ station, beginYMD, endYMD, datum="MLLW" }) {
  const url = new URL("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter");
  url.searchParams.set("product","predictions");
  url.searchParams.set("application","cupajoe.live");
  url.searchParams.set("begin_date", beginYMD);
  url.searchParams.set("end_date", endYMD);
  url.searchParams.set("datum", datum);
  url.searchParams.set("station", station);
  url.searchParams.set("time_zone","gmt");
  url.searchParams.set("units","english");
  url.searchParams.set("interval","hilo");
  url.searchParams.set("format","json");

  const res = await fetch(url.toString(), { cache:"no-store" });
  if (!res.ok) throw new Error("NOAA hilo predictions failed " + res.status);
  const j = await res.json();

  const preds = j?.predictions || [];
  return preds
    .filter(p => String(p.type || "").toUpperCase() === "H")
    .map(p => ({
      t: new Date(parseNOAATimeToISO_UTC(String(p.t))).toISOString()
    }))
    .filter(p => p.t)
    .sort((a,b)=> new Date(a.t) - new Date(b.t));
}

/* =========================
Build crest-anchored events
========================= */
function buildCrestAnchoredHighEvents({ series, predictedHighs, thresholdsNAVD88 }) {
  if (!Array.isArray(series) || !series.length) return [];
  if (!Array.isArray(predictedHighs) || !predictedHighs.length) return [];

  const w2 = CREST_WINDOW_HOURS * 3600 * 1000;
  const w1 = REQUIRE_WITHIN_HOURS * 3600 * 1000;

  const pts = [...series].sort((a, b) => new Date(a.t) - new Date(b.t));

  const out = [];
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
      t: new Date(best.t).toISOString(),
      ft: roundFt(ft),
      type: classifyNAVD(ft, thresholdsNAVD88),
      crest: new Date(crestISO).toISOString(),
      kind: "CrestHigh"
    });
  }

  return out;
}

function dedupeByCrestKeepMax(events) {
  const m = new Map(); // crestISO -> event
  for (const e of (events || [])) {
    const k = e?.crest || e?.t;
    if (!k) continue;
    if (!m.has(k) || (Number(e.ft) > Number(m.get(k).ft))) m.set(k, e);
  }
  return [...m.values()].sort((a,b)=> new Date(a.t) - new Date(b.t));
}

async function main() {
  const cache = loadJSON(CACHE_PATH);

  cache.site = SITE;
  cache.parameterCd = PARAM;
  cache.datum = "NAVD88";
  cache.peakMinSepMinutes = cache.peakMinSepMinutes || PEAK_MIN_SEP_MINUTES;

  const THRESH_NAVD88 = cache?.thresholdsNAVD88 || null;
  if (!THRESH_NAVD88) {
    die(
      "Missing NAVD88 thresholds. Add thresholdsNAVD88 to data/peaks_navd88.json, e.g.\n" +
      '  "thresholdsNAVD88": {"minorLow": 3.10, "moderateLow": 4.10, "majorLow": 5.10}\n'
    );
  }

  if (cache.method !== METHOD) {
    console.log(`Method changed (${cache.method || "none"} -> ${METHOD}). Clearing events for clean rebuild.`);
    cache.method = METHOD;
    cache.events = [];
  } else {
    cache.method = METHOD;
    cache.events = Array.isArray(cache.events) ? cache.events : [];
  }

  const backfillYear = parseArg("--backfill-year");
  const backfillFrom = parseArg("--backfill-from");
  const backfillTo   = parseArg("--backfill-to");

  let startISO, endISO;

  if (backfillYear) {
    const y = Number(backfillYear);
    if (!Number.isFinite(y) || y < 1900 || y > 3000) die("Invalid --backfill-year=YYYY");
    startISO = new Date(Date.UTC(y, 0, 1, 0, 0, 0)).toISOString();
    endISO   = new Date(Date.UTC(y + 1, 0, 1, 0, 0, 0)).toISOString();
    console.log(`Backfill year ${y}: ${startISO} → ${endISO}`);
  } else if (backfillFrom && backfillTo) {
    const y1 = Number(backfillFrom);
    const y2 = Number(backfillTo);
    if (!Number.isFinite(y1) || !Number.isFinite(y2)) die("Invalid --backfill-from / --backfill-to (must be years)");
    const lo = Math.min(y1, y2);
    const hi = Math.max(y1, y2);
    if (lo < 1900 || hi > 3000) die("Backfill range out of bounds.");
    startISO = new Date(Date.UTC(lo, 0, 1, 0, 0, 0)).toISOString();
    endISO   = new Date(Date.UTC(hi + 1, 0, 1, 0, 0, 0)).toISOString();
    console.log(`Backfill years ${lo}–${hi}: ${startISO} → ${endISO}`);
  } else {
    const last = clampISO(cache.lastProcessedISO || "2000-01-01T00:00:00Z");
    if (!last) die("Cache lastProcessedISO is invalid ISO.");
    startISO = addHoursISO(last, -BUFFER_HOURS);
    endISO   = isoNow();
    console.log(`Incremental: ${startISO} → ${endISO}`);
  }

  // Build predicted crest list (NOAA hilo highs)
  const startD = startOfUTCDate(new Date(startISO));
  const endD   = startOfUTCDate(new Date(endISO));

  const beginYMD = yyyymmddUTC(startD);
  const endYMD   = yyyymmddUTC(endD);

  console.log(`Fetching NOAA hilo highs: ${beginYMD} → ${endYMD} (station ${NOAA_STATION})`);
  const predictedHighs = await fetchNOAAHiloHighs({
    station: NOAA_STATION,
    beginYMD,
    endYMD,
    datum: "MLLW"
  });

  console.log(`Fetching USGS IV points: ${startISO} → ${endISO} (site ${SITE})`);
  const series = await fetchUSGSIV({ startISO, endISO });

  console.log(`Building crest-anchored events...`);
  const newEvents = buildCrestAnchoredHighEvents({
    series,
    predictedHighs,
    thresholdsNAVD88: THRESH_NAVD88
  });

  console.log(`New events: ${newEvents.length}`);

  cache.events = dedupeByCrestKeepMax([...(cache.events || []), ...newEvents]);
  cache.lastProcessedISO = endISO;

  saveJSON(CACHE_PATH, cache);

  console.log(`Saved ${cache.events.length} total events → ${CACHE_PATH}`);
}

main().catch((e) => die(String(e && (e.stack || e.message || e))));
