#!/usr/bin/env python3
"""
Sentinel-2 Quarterly Cloudless Mosaic — NDVI GeoTIFF Downloader  (v2 — fixed)
==============================================================================
Downloads cloud-free NDVI composites (FLOAT32 GeoTIFF, 10 m resolution)
from the Sentinel-2 L2A Quarterly Cloudless Mosaic collection on CDSE.

Key fixes vs v1
---------------
1. Bands are requested as raw float (no scaling in evalscript) and NDVI is
   computed in Python — eliminating all evalscript DN-scaling ambiguity.
2. Single-output response only ("default") — avoids multi-response dict
   key mismatches that caused the wrong array to be saved.
3. Hard clamp to [-1, 1] with NaN propagation — non-physical values from
   integer overflow, divide-by-zero, or no-data sentinels (-32768) are
   cleaned up before writing.
4. Verbose per-tile diagnostics so bad data is caught immediately.

Compositing (server-side, ESA/Sinergise):
  • 3-month stack of L2A scenes per pixel.
  • SCL-based invalid pixel removal (clouds, shadows, snow …).
  • 25th-percentile of valid observations selected per band.
  • Quarters: Q1=Jan–Mar, Q2=Apr–Jun, Q3=Jul–Sep, Q4=Oct–Dec.

Requirements:
    pip install sentinelhub rasterio numpy

Credentials (OAuth client from CDSE dashboard):
    export CDSE_CLIENT_ID="…"
    export CDSE_CLIENT_SECRET="…"

Usage:
    python download_ndvi.py --year 2024 --quarter Q3
    python download_ndvi.py --year 2024 --all-quarters
    python download_ndvi.py --year 2015 --year 2025 --all-quarters   # loop externally
    python download_ndvi.py          # auto-selects last completed quarter
"""

import os
import sys
import argparse
import datetime
from pathlib import Path

import numpy as np

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────
CLIENT_ID     = os.environ.get("CDSE_CLIENT_ID",     "YOUR_CLIENT_ID_HERE")
CLIENT_SECRET = os.environ.get("CDSE_CLIENT_SECRET", "YOUR_CLIENT_SECRET_HERE")

AOI_BBOX = {
    "west":  18.50000,  # ←

    "south": 48.77910,  # ↓

    "east":  18.83029,  # →

    "north": 49.01070,  # ↑
}

OUTPUT_DIR    = Path("./ndvi_output")
COLLECTION_ID = "5460de54-082e-473a-b6ea-d5cbe3c17cca"   # 10 m quarterly mosaic
RESOLUTION_M  = 10

QUARTERS = {
    "Q1": ("01-01", "03-31"),
    "Q2": ("04-01", "06-30"),
    "Q3": ("07-01", "09-30"),
    "Q4": ("10-01", "12-31"),
}

# ──────────────────────────────────────────────────────────────────────────────
# Evalscript — returns raw B04 and B08 as FLOAT32 (no DN scaling here)
#
# Why raw bands and not NDVI directly?
#   The mosaic stores values as DN = reflectance × 10000 (INT16 under the hood).
#   Sentinel Hub auto-converts to float before passing to evaluatePixel, BUT the
#   exact scaling path has changed across collection versions (2015–2016 tiles vs
#   later tiles behave differently).  Computing NDVI in Python from the two
#   returned band values is unambiguous: we can inspect B04/B08 individually,
#   detect sentinel no-data values (-32768 → float -3.2768 after /10000), and
#   mask them explicitly.
#
# dataMask is output separately so we can apply it cleanly in Python.
# ──────────────────────────────────────────────────────────────────────────────
EVALSCRIPT = """
//VERSION=3
function setup() {
    return {
        input: ["B04", "B08", "dataMask"],
        output: {
            id: "default",
            bands: 3,
            sampleType: "FLOAT32"
        }
    };
}

function evaluatePixel(samples) {
    // Band 0 = B04 (Red),  Band 1 = B08 (NIR),  Band 2 = dataMask
    // Values come in as DN (reflectance * 10000). We do NOT divide here —
    // we pass raw DN so Python can inspect them and detect -32768 no-data.
    return [samples.B04, samples.B08, samples.dataMask];
}
"""


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def build_config():
    try:
        from sentinelhub import SHConfig
    except ImportError:
        sys.exit("ERROR: sentinelhub not installed.  Run:  pip install sentinelhub")

    config = SHConfig()
    config.sh_client_id     = CLIENT_ID
    config.sh_client_secret = CLIENT_SECRET
    config.sh_token_url = (
        "https://identity.dataspace.copernicus.eu"
        "/auth/realms/CDSE/protocol/openid-connect/token"
    )
    config.sh_base_url = "https://sh.dataspace.copernicus.eu"

    if CLIENT_ID == "YOUR_CLIENT_ID_HERE":
        sys.exit(
            "ERROR: No credentials.\n"
            "  Set CDSE_CLIENT_ID + CDSE_CLIENT_SECRET env vars, or edit CONFIG.\n"
            "  Get OAuth client: https://shapps.dataspace.copernicus.eu/"
            "dashboard/#/account/settings"
        )
    return config


def quarter_interval(year: int, quarter: str):
    s, e = QUARTERS[quarter]
    return f"{year}-{s}", f"{year}-{e}"


def compute_ndvi_safe(b04_dn: np.ndarray, b08_dn: np.ndarray,
                      datamask: np.ndarray) -> np.ndarray:
    """
    Compute NDVI from raw DN arrays with full safety checks.

    Rules applied:
      1. dataMask == 0  →  NaN   (no valid observation)
      2. DN == -32768   →  NaN   (mosaic no-data sentinel, stored as float -32768)
      3. B04 + B08 == 0 →  NaN   (divide-by-zero guard)
      4. Result clipped to [-1, 1]  (physical range; values outside indicate
         sensor artefacts in early archive years)
    """
    ndvi = np.full(b04_dn.shape, np.nan, dtype=np.float32)

    # Convert DN float back — sentinel value from INT16 no-data
    NO_DATA_SENTINEL = -32768.0   # what -32768 looks like as float DN

    valid = (
        (datamask > 0.5) &                      # valid observation exists
        (b04_dn > NO_DATA_SENTINEL + 1) &       # not a no-data sentinel
        (b08_dn > NO_DATA_SENTINEL + 1) &
        ((b04_dn + b08_dn) != 0.0)              # no divide-by-zero
    )

    r = b04_dn[valid]
    n = b08_dn[valid]
    ndvi[valid] = (n - r) / (n + r)

    # Hard clamp — values outside [-1,1] are physically impossible
    # (they arise from integer overflow or residual artefacts in 2015-2016 tiles)
    out_of_range = valid & ((ndvi < -1.0) | (ndvi > 1.0))
    ndvi[out_of_range] = np.nan

    return ndvi


def save_geotiff(ndvi_array: np.ndarray, bbox, out_path: Path):
    try:
        import rasterio
        from rasterio.transform import from_bounds
        from rasterio.crs import CRS as RioCRS
    except ImportError:
        sys.exit("ERROR: rasterio not installed.  Run:  pip install rasterio")

    rows, cols = ndvi_array.shape
    transform = from_bounds(
        bbox.min_x, bbox.min_y, bbox.max_x, bbox.max_y,
        cols, rows
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(
        out_path, "w",
        driver="GTiff",
        height=rows, width=cols,
        count=1,
        dtype=np.float32,
        crs=RioCRS.from_epsg(4326),
        transform=transform,
        nodata=np.nan,
        compress="lzw",
        predictor=3,      # floating-point predictor
        tiled=True,
        blockxsize=256, blockysize=256,
    ) as dst:
        dst.write(ndvi_array.astype(np.float32), 1)
        dst.update_tags(
            DESCRIPTION="Sentinel-2 Quarterly Cloudless Mosaic NDVI",
            COLLECTION_ID=COLLECTION_ID,
            RESOLUTION_M=str(RESOLUTION_M),
        )

    size_mb = out_path.stat().st_size / 1e6
    print(f"  → Saved: {out_path}  ({cols}×{rows} px, {size_mb:.1f} MB)")


def download_ndvi(year: int, quarter: str, config) -> Path | None:
    try:
        from sentinelhub import (
            BBox, CRS, DataCollection, MimeType,
            SentinelHubRequest, bbox_to_dimensions,
        )
    except ImportError:
        sys.exit("ERROR: sentinelhub not installed.  Run:  pip install sentinelhub")

    start_date, end_date = quarter_interval(year, quarter)
    print(f"\n{'='*64}")
    print(f"  Quarter : {year} {quarter}  ({start_date} → {end_date})")

    bbox = BBox(
        bbox=(AOI_BBOX["west"], AOI_BBOX["south"],
              AOI_BBOX["east"], AOI_BBOX["north"]),
        crs=CRS.WGS84,
    )
    img_size = bbox_to_dimensions(bbox, resolution=RESOLUTION_M)
    print(f"  Image   : {img_size[0]} × {img_size[1]} px @ {RESOLUTION_M} m")

    mosaic = DataCollection.define_byoc(collection_id=COLLECTION_ID)

    request = SentinelHubRequest(
        evalscript=EVALSCRIPT,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=mosaic,
                time_interval=(start_date, end_date),
            )
        ],
        responses=[
            SentinelHubRequest.output_response("default", MimeType.TIFF),
        ],
        bbox=bbox,
        size=img_size,
        config=config,
    )

    print("  Fetching …")
    raw = request.get_data()

    # ── Unpack ───────────────────────────────────────────────────────────────
    # get_data() always returns a list of length 1 for a single time-slot.
    # With a single-output response the item is a numpy array of shape
    # (height, width, bands).
    if not raw:
        print("  ERROR: empty response — skipping.")
        return None

    item = raw[0]

    # Handle both possible return types robustly
    if isinstance(item, dict):
        # Multi-output path (shouldn't happen here, but be safe)
        key = next(iter(item))
        arr = item[key]
        print(f"  Note: got dict response, key='{key}'")
    elif isinstance(item, np.ndarray):
        arr = item
    else:
        print(f"  ERROR: unexpected response type {type(item)} — skipping.")
        return None

    # arr shape: (rows, cols, 3)  bands: [B04, B08, dataMask]
    if arr.ndim != 3 or arr.shape[2] < 3:
        print(f"  ERROR: unexpected array shape {arr.shape} — skipping.")
        return None

    b04      = arr[:, :, 0].astype(np.float32)
    b08      = arr[:, :, 1].astype(np.float32)
    datamask = arr[:, :, 2]

    # ── Diagnostics on raw bands ─────────────────────────────────────────────
    valid_mask = datamask > 0.5
    n_total   = arr.shape[0] * arr.shape[1]
    n_valid   = int(valid_mask.sum())
    n_nodata  = n_total - n_valid
    print(f"  dataMask: {n_valid}/{n_total} valid pixels "
          f"({100*n_valid/n_total:.1f}%), {n_nodata} no-data")

    if n_valid == 0:
        print("  WARNING: No valid pixels — mosaic not available for this quarter/area.")
        print("           Saving all-NaN tile so inventory remains complete.")
        ndvi = np.full((arr.shape[0], arr.shape[1]), np.nan, dtype=np.float32)
    else:
        # Spot-check raw DN for sentinel values
        b04_v = b04[valid_mask]
        b08_v = b08[valid_mask]
        print(f"  B04 DN  : min={b04_v.min():.0f}  max={b04_v.max():.0f}  "
              f"mean={b04_v.mean():.0f}")
        print(f"  B08 DN  : min={b08_v.min():.0f}  max={b08_v.max():.0f}  "
              f"mean={b08_v.mean():.0f}")

        ndvi = compute_ndvi_safe(b04, b08, datamask)

        ndvi_v = ndvi[~np.isnan(ndvi)]
        n_clamped = n_valid - len(ndvi_v)
        print(f"  NDVI    : min={ndvi_v.min():.4f}  mean={ndvi_v.mean():.4f}  "
              f"max={ndvi_v.max():.4f}")
        if n_clamped > 0:
            print(f"  Clamped : {n_clamped} out-of-range pixels set to NaN "
                  f"({100*n_clamped/n_total:.2f}%)")

    # ── Save ─────────────────────────────────────────────────────────────────
    out_path = OUTPUT_DIR / f"ndvi_{year}_{quarter}.tif"
    save_geotiff(ndvi, bbox, out_path)
    return out_path


# ──────────────────────────────────────────────────────────────────────────────
# Post-download validation helper  (run against already-downloaded files)
# ──────────────────────────────────────────────────────────────────────────────

def validate_existing(directory: Path):
    """
    Re-scan all ndvi_*.tif files in a directory and report any anomalies.
    Useful for checking the batch you already downloaded.
    """
    try:
        import rasterio
    except ImportError:
        sys.exit("ERROR: rasterio not installed.")

    tifs = sorted(directory.glob("ndvi_*.tif"))
    if not tifs:
        print(f"No ndvi_*.tif files found in {directory}")
        return

    print(f"\nValidating {len(tifs)} files in {directory}\n")
    print(f"{'File':<30}  {'Valid%':>7}  {'Min':>8}  {'Mean':>8}  {'Max':>8}  Status")
    print("-" * 75)

    problems = []
    for f in tifs:
        with rasterio.open(f) as src:
            arr = src.read(1).astype(np.float32)

        total = arr.size
        valid = arr[~np.isnan(arr)]
        pct   = 100 * len(valid) / total

        if len(valid) == 0:
            status = "ALL NaN"
            problems.append((f.name, status))
        else:
            mn, mn_val, mx = valid.min(), valid.mean(), valid.max()
            bad = ((valid < -1) | (valid > 1)).sum()
            if bad > 0:
                status = f"!! {bad} out-of-range pixels !!"
                problems.append((f.name, status))
            elif pct < 50:
                status = f"low coverage ({pct:.0f}%)"
            else:
                status = "OK"
            print(f"{f.name:<30}  {pct:>6.1f}%  {mn:>8.4f}  {mn_val:>8.4f}  "
                  f"{mx:>8.4f}  {status}")
            continue

        print(f"{f.name:<30}  {pct:>6.1f}%  {'—':>8}  {'—':>8}  {'—':>8}  {status}")

    if problems:
        print(f"\n⚠  {len(problems)} problematic file(s):")
        for name, reason in problems:
            print(f"   {name}: {reason}")
    else:
        print("\n✓  All files look clean.")


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Download Sentinel-2 Quarterly Cloudless Mosaic NDVI as GeoTIFF",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--year",  type=int, default=datetime.date.today().year)
    p.add_argument("--quarter", choices=["Q1","Q2","Q3","Q4"])
    p.add_argument("--all-quarters", action="store_true")
    p.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    p.add_argument(
        "--validate-only", action="store_true",
        help="Do not download; just validate existing GeoTIFFs in --output-dir"
    )
    return p.parse_args()


def main():
    args = parse_args()
    global OUTPUT_DIR
    OUTPUT_DIR = args.output_dir

    if args.validate_only:
        validate_existing(OUTPUT_DIR)
        return

    config = build_config()

    if args.all_quarters:
        quarters = list(QUARTERS.keys())
    elif args.quarter:
        quarters = [args.quarter]
    else:
        today = datetime.date.today()
        m = today.month
        if   m <= 3:  q, y = "Q4", args.year - 1
        elif m <= 6:  q, y = "Q1", args.year
        elif m <= 9:  q, y = "Q2", args.year
        else:         q, y = "Q3", args.year
        quarters, args.year = [q], y
        print(f"No quarter specified — defaulting to last completed: {args.year} {q}")

    saved = []
    for q in quarters:
        path = download_ndvi(year=args.year, quarter=q, config=config)
        if path:
            saved.append(path)

    print(f"\n{'='*64}")
    print(f"Done. {len(saved)} file(s) in {OUTPUT_DIR.resolve()}")
    for p in saved:
        print(f"  {p.name}")

    print("""
─────────────────────────────────────────────────────────────
GeoTIFF spec:
  CRS    : EPSG:4326 (WGS84)
  Values : FLOAT32 NDVI in [-1 .. 1], NaN = no data
  Compress: LZW + floating-point predictor (predictor=3)
  Tiled  : 256×256 blocks

To validate your existing downloads without re-downloading:
  python download_ndvi.py --validate-only --output-dir ./ndvi_output
─────────────────────────────────────────────────────────────
""")


if __name__ == "__main__":
    main()