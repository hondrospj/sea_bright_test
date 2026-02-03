#!/usr/bin/env node
/**
 * PETSS station forecast updater (MLLW)
 * - Default: scrape NOMADS latest petss.YYYYMMDD and pick best cycle tarball
 * - Override: set PETSS_TARBALL_URL to a direct .tar.gz URL (your provided pattern)
 *
 * Outputs:
 *   data/petss_forecast.json
 *   data/petss_forecast.csv
 *   data/petss_meta.json
 */

const fs = require("fs");
const path = require("path");
const https = require("https");
const zlib = require("zlib");
const tar = require("tar");

const OUT_DIR = path.join(__dirname, "..", "data");
const OUT_JSON = path.join(OUT_DIR, "petss_forecast.json");
const OUT_CSV  = path.join(OUT_DIR, "petss_forecast.csv");
const OUT_META = path.join(OUT_DIR, "petss_meta.json");

const STID = String(process.env.PETSS_STID || "8531804");   // Sea Bright
const DATUM = String(process.env.PETSS_DATUM || "MLLW");    // matches your dashboard
const NOMADS_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/petss/prod/";
const USER_AGENT = "petss-forecast-updater-seabright";

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { "User-Agent": USER_AGENT } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return resolve(fetchText(res.headers.location));
      }
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      let data = "";
      res.setEncoding("utf8");
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => resolve(data));
    }).on("error", reject);
  });
}

function downloadFile(url, outPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(outPath);
    https.get(url, { headers: { "User-Agent": USER_AGENT } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        file.close(() => fs.unlinkSync(outPath));
        return resolve(downloadFile(res.headers.location, outPath));
      }
      if (res.statusCode !== 200) {
        file.close(() => fs.unlinkSync(outPath));
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      res.pipe(file);
      file.on("finish", () => file.close(resolve));
    }).on("error", (err) => {
      try { file.close(() => fs.unlinkSync(outPath)); } catch (_) {}
      reject(err);
    });
  });
}

function listLatestProdDir(html) {
  const re = /petss\.(\d{8})\/?/g;
  const dates = [];
  let m;
  while ((m = re.exec(html)) !== null) dates.push(m[1]);
  if (!dates.length) throw new Error("Could not find petss.YYYYMMDD directories in NOMADS listing.");
  dates.sort();
  const latest = dates[dates.length - 1];
  return `petss.${latest}/`;
}

function chooseCycleTarball(html) {
  const preferred = ["t18z", "t12z", "t06z", "t00z"];
  for (const cyc of preferred) {
    const name = `petss.${cyc}.csv.tar.gz`;
    if (html.includes(name)) return name;
  }
  const m = html.match(/petss\.t\d{2}z\.csv\.tar\.gz/g);
  if (m && m.length) return m.sort().pop();
  throw new Error("Could not find any petss.t??z.csv.tar.gz tarball in run dir listing.");
}

function findFileRecursive(rootDir, filename) {
  const stack = [rootDir];
  while (stack.length) {
    const d = stack.pop();
    const ents = fs.readdirSync(d, { withFileTypes: true });
    for (const e of ents) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) stack.push(p);
      else if (e.isFile() && e.name === filename) return p;
    }
  }
  return null;
}

function parseNomadsStationCsv(text, stid) {
  const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);

  let headerIdx = -1;
  for (let i = 0; i < lines.length; i++) {
    const h = lines[i].trim().toUpperCase();
    if (h.includes("TIME") && h.includes("TWL")) { headerIdx = i; break; }
  }
  if (headerIdx === -1) throw new Error(`Could not find NOMADS header row with TIME/TWL for STID=${stid}`);

  const header = lines[headerIdx].split(",").map((s) => s.trim().toUpperCase());
  const idxTIME = header.indexOf("TIME");
  const idxTWL = header.indexOf("TWL");
  const idxTIDE = header.indexOf("TIDE");
  const idxSURGE = header.indexOf("SURGE");

  if (idxTIME === -1 || idxTWL === -1) throw new Error(`Header missing TIME or TWL for STID=${stid}`);

  function parseNum(s) {
    const v = Number(String(s).trim());
    if (!Number.isFinite(v)) return null;
    if (Math.abs(v - 9999) < 1e-6) return null;
    return v;
  }

  function parseTimeYYYYMMDDHHMM(s) {
    const t = String(s).trim();
    if (!/^\d{12}$/.test(t)) return null;
    const Y = Number(t.slice(0, 4));
    const M = Number(t.slice(4, 6));
    const D = Number(t.slice(6, 8));
    const h = Number(t.slice(8, 10));
    const m = Number(t.slice(10, 12));
    const dt = new Date(Date.UTC(Y, M - 1, D, h, m, 0));
    if (Number.isNaN(dt.getTime())) return null;
    return dt;
  }

  const rows = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const parts = lines[i].split(",");
    if (parts.length < header.length) continue;

    const dt = parseTimeYYYYMMDDHHMM(parts[idxTIME]);
    const twl = parseNum(parts[idxTWL]);
    if (!dt || twl === null) continue;

    const tide = (idxTIDE !== -1) ? parseNum(parts[idxTIDE]) : null;
    const surge = (idxSURGE !== -1) ? parseNum(parts[idxSURGE]) : null;

    rows.push({
      t: dt.toISOString(),
      twl,
      tide,
      surge
    });
  }

  rows.sort((a, b) => new Date(a.t) - new Date(b.t));
  return rows;
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const overrideUrl = process.env.PETSS_TARBALL_URL ? String(process.env.PETSS_TARBALL_URL) : null;

  let tarUrlUsed = null;

  if (overrideUrl) {
    tarUrlUsed = overrideUrl;
    console.log(`Using PETSS_TARBALL_URL override: ${tarUrlUsed}`);
  } else {
    const prodHtml = await fetchText(NOMADS_BASE);
    const latestDir = listLatestProdDir(prodHtml);
    const runUrl = NOMADS_BASE + latestDir;

    const runHtml = await fetchText(runUrl);
    const tarName = chooseCycleTarball(runHtml);

    tarUrlUsed = runUrl + tarName;
    console.log(`Auto-selected: ${tarUrlUsed}`);
  }

  const tmpTar = path.join(OUT_DIR, `petss_${Date.now()}.tar.gz`);
  const tmpExtract = path.join(OUT_DIR, `petss_extract_${Date.now()}`);

  await downloadFile(tarUrlUsed, tmpTar);
  fs.mkdirSync(tmpExtract, { recursive: true });

  // Extract tar.gz
  await tar.x({
    file: tmpTar,
    cwd: tmpExtract,
    gzip: true
  });

  // Find the station CSV (often STID.csv or similar)
  const wanted = `${STID}.csv`;
  const csvPath = findFileRecursive(tmpExtract, wanted);
  if (!csvPath) {
    throw new Error(`Could not find ${wanted} inside PETSS tarball.`);
  }

  const csvText = fs.readFileSync(csvPath, "utf8");
  fs.writeFileSync(OUT_CSV, csvText, "utf8");

  const rows = parseNomadsStationCsv(csvText, STID);

  // JSON format your index.html can normalize (it supports json.points)
  const out = {
    station: STID,
    datum: DATUM,
    fetched_utc: new Date().toISOString(),
    source: tarUrlUsed,
    points: rows.map(r => ({
      t: r.t,
      fcst: r.twl,
      tide: r.tide,
      surge: r.surge
    }))
  };

  fs.writeFileSync(OUT_JSON, JSON.stringify(out, null, 2) + "\n", "utf8");

  const meta = {
    station: STID,
    datum: DATUM,
    fetched_utc: out.fetched_utc,
    source: tarUrlUsed,
    n_points: out.points.length,
    t0: out.points[0]?.t || null,
    t1: out.points[out.points.length - 1]?.t || null
  };
  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2) + "\n", "utf8");

  // Cleanup
  try { fs.unlinkSync(tmpTar); } catch (_) {}
  try { fs.rmSync(tmpExtract, { recursive: true, force: true }); } catch (_) {}

  console.log(`Wrote ${OUT_JSON}`);
  console.log(`Wrote ${OUT_CSV}`);
  console.log(`Wrote ${OUT_META}`);
  console.log(`Points: ${out.points.length}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
