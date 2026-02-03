/**
 * Fetch NOAA/PETSS forecast (MLLW) for Sea Bright area.
 * Default station: 8531680 (Sandy Hook, NJ)
 *
 * Writes:
 *  - data/seabright_petss_forecast_mllw.json
 */

import fs from "node:fs";
import path from "node:path";

// =============
// Sea Bright defaults
// =============
const PLACE_NAME = "Sea Bright, NJ";
const SITE_SLUG = "seabright";

// PETSS station id (NOAA CO-OPS): Sandy Hook (near Sea Bright)
const DEFAULT_PETSS_STID = "8531680";

// Output
const DEFAULT_OUT = `data/${SITE_SLUG}_petss_forecast_mllw.json`;

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

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

async function fetchPetssJson({ stid }) {
  // This is the same endpoint pattern you were using; only the STID changes.
  // If your original script used a different PETSS endpoint, keep it—
  // just swap stid default + output naming.
  const url =
    `https://api.water.noaa.gov/nwps/v1/reaches/${stid}/forecast?product=petss`;

  const res = await fetch(url, {
    headers: { "User-Agent": `${PLACE_NAME} PETSS Updater` },
  });

  // Some PETSS endpoints return 404 depending on product availability;
  // treat it as "no forecast" instead of hard-failing.
  if (!res.ok) {
    return { ok: false, status: res.status, statusText: res.statusText, url };
  }

  const json = await res.json();
  return { ok: true, json, url };
}

function normalizePoints(json) {
  // Normalize to {t, v} points if your dashboard expects that.
  // Many NOAA forecast JSON blobs nest time series differently.
  // This is a safe "best guess" that works if it’s already {t,v} or {time,value}.
  const raw =
    json?.points ||
    json?.data ||
    json?.timeseries ||
    json?.timeSeries ||
    [];

  if (!Array.isArray(raw)) return [];

  const pts = raw
    .map((d) => ({
      t: d?.t || d?.time || d?.dateTime || d?.validTime || null,
      v:
        d?.v != null
          ? Number(d.v)
          : d?.value != null
          ? Number(d.value)
          : d?.waterLevel != null
          ? Number(d.waterLevel)
          : null,
    }))
    .filter((p) => p.t && Number.isFinite(p.v));

  // sort
  pts.sort((a, b) => new Date(a.t).getTime() - new Date(b.t).getTime());
  return pts;
}

async function main() {
  const argv = parseArgs();

  const stid = argv.stid || DEFAULT_PETSS_STID;
  const out = argv.out || DEFAULT_OUT;

  const fetched_utc = new Date().toISOString();

  const r = await fetchPetssJson({ stid });

  if (!r.ok) {
    const payload = {
      place: PLACE_NAME,
      site_slug: SITE_SLUG,
      petss_stid: stid,
      fetched_utc,
      ok: false,
      error: `${r.status} ${r.statusText}`,
      urlUsed: r.url,
      points: [],
    };
    writeJson(out, payload);
    console.log(`[warn] PETSS unavailable; wrote empty payload -> ${out}`);
    return;
  }

  const json = r.json;
  const points = normalizePoints(json);

  const issued_utc =
    json?.model_time_utc ||
    json?.issued_utc ||
    json?.fetched_utc ||
    json?.issuedTime ||
    null;

  const payload = {
    place: PLACE_NAME,
    site_slug: SITE_SLUG,
    petss_stid: stid,
    datum: "MLLW",
    issued_utc,
    fetched_utc,
    ok: true,
    urlUsed: r.url,
    points,
    raw: json,
  };

  writeJson(out, payload);
  console.log(`[ok] wrote ${out} (${points.length} points)`);
}

main().catch((e) => {
  console.error("[fatal]", e);
  process.exit(1);
});
