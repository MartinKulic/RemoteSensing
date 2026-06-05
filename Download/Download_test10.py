#!/usr/bin/env python3
"""
Sentinel-2 Quarterly Cloudless Mosaic — NDVI GeoTIFF Downloader
================================================================
Downloads cloud-free NDVI composites (FLOAT32 GeoTIFF, 10 m resolution)
from the Sentinel-2 L2A Quarterly Cloudless Mosaic collection hosted on
Copernicus Data Space Ecosystem (CDSE).

Compositing methodology (done server-side by ESA/Sinergise):
  • For each pixel a 3-month stack of L2A observations is built.
  • Invalid pixels (clouds, shadows, snow, etc.) are removed using SCL.
  • The 25th percentile of the remaining valid observations is selected
    per band → this is the same product visible in Copernicus Browser.
  • Available quarters: Jan–Mar, Apr–Jun, Jul–Sep, Oct–Dec.

Requirements:
    pip install sentinelhub rasterio numpy

Credentials:
    Set CDSE_CLIENT_ID and CDSE_CLIENT_SECRET as environment variables,
    or edit the CONFIG section below.

Usage examples:
    # Latest available quarter for all 4 seasonal quarters of 2024
    python download_ndvi.py --year 2024 --all-quarters

    # Single quarter
    python download_ndvi.py --year 2024 --quarter Q2

    # Custom date range (must span ≥1 full quarter boundary)
    python download_ndvi.py --start 2024-04-01 --end 2024-06-30
"""

import os
import sys
import argparse
import datetime
from pathlib import Path

import numpy as np

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG — edit or set env vars
# ──────────────────────────────────────────────────────────────────────────────
CLIENT_ID     = os.environ.get("CDSE_CLIENT_ID",     "YOUR_CLIENT_ID_HERE")
CLIENT_SECRET = os.environ.get("CDSE_CLIENT_SECRET", "YOUR_CLIENT_SECRET_HERE")

# Area of interest (WGS84)
AOI_BBOX = {
    "west":  18.51635,
    "south": 48.78376,
    "east":  18.80255,
    #"north": 49.04104,
    "north": 49.01,
}

# Output directory
OUTPUT_DIR = Path("./ndvi_output")

# Sentinel-2 Quarterly Cloudless Mosaic collection (10 m)
COLLECTION_ID = "5460de54-082e-473a-b6ea-d5cbe3c17cca"

# Quarterly windows — use the first day of the quarter to address the mosaic
QUARTERS = {
    "Q1": ("01-01", "03-31"),   # Jan–Mar
    "Q2": ("04-01", "06-30"),   # Apr–Jun
    "Q3": ("07-01", "09-30"),   # Jul–Sep
    "Q4": ("10-01", "12-31"),   # Oct–Dec
}

# Resolution in metres (the mosaic native resolution is 10 m)
RESOLUTION_M = 10

# ──────────────────────────────────────────────────────────────────────────────
# Evalscript — returns raw FLOAT32 NDVI values in [-1, 1]
# dataMask band (band 2) marks valid pixels: 1 = valid, 0 = no-data
# ──────────────────────────────────────────────────────────────────────────────
EVALSCRIPT_NDVI = """
//VERSION=3
function setup() {
    return {
        input: ["B04", "B08", "dataMask"],
        output: [
            {
                id: "ndvi",
                bands: 1,
                sampleType: "FLOAT32"
            },
            {
                id: "dataMask",
                bands: 1,
                sampleType: "UINT8"
            }
        ]
    };
}

function evaluatePixel(samples) {
    // DN values are stored as reflectance * 10000 in the mosaic
    let factor = 1.0 / 10000.0;
    let red = factor * samples.B04;
    let nir = factor * samples.B08;

    // Guard against division by zero
    let ndvi = (nir + red) > 0.0 ? (nir - red) / (nir + red) : 0.0;

    // If no valid observation was recorded the mosaic stores -32768;
    // dataMask == 0 flags those pixels — we propagate NaN.
    let ndviOut = samples.dataMask === 1 ? ndvi : NaN;

    return {
        ndvi: [ndviOut],
        dataMask: [samples.dataMask]
    };
}
"""

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def build_config():
    """Build and return an SHConfig pointed at CDSE."""
    try:
        from sentinelhub import SHConfig
    except ImportError:
        sys.exit("ERROR: sentinelhub not installed — run:  pip install sentinelhub")

    config = SHConfig()
    config.sh_client_id     = CLIENT_ID
    config.sh_client_secret = CLIENT_SECRET
    config.sh_token_url     = (
        "https://identity.dataspace.copernicus.eu"
        "/auth/realms/CDSE/protocol/openid-connect/token"
    )
    config.sh_base_url = "https://sh.dataspace.copernicus.eu"

    if CLIENT_ID == "YOUR_CLIENT_ID_HERE":
        sys.exit(
            "ERROR: No credentials set.\n"
            "  Set CDSE_CLIENT_ID and CDSE_CLIENT_SECRET env vars, or edit CONFIG in the script.\n"
            "  Get credentials at: https://shapps.dataspace.copernicus.eu/dashboard/#/account/settings"
        )
    return config


def quarter_interval(year: int, quarter: str):
    """Return (start_date, end_date) strings for a given year and quarter."""
    start_mm_dd, end_mm_dd = QUARTERS[quarter]
    return f"{year}-{start_mm_dd}", f"{year}-{end_mm_dd}"


def save_geotiff(ndvi_array: np.ndarray, bbox, out_path: Path, nodata: float = np.nan):
    """
    Write a single-band FLOAT32 GeoTIFF with proper georeferencing.

    Parameters
    ----------
    ndvi_array : 2-D numpy array  shape (rows, cols)
    bbox       : sentinelhub BBox
    out_path   : output file path
    nodata     : value used for no-data pixels
    """
    try:
        import rasterio
        from rasterio.transform import from_bounds
        from rasterio.crs import CRS as RioCRS
    except ImportError:
        sys.exit("ERROR: rasterio not installed — run:  pip install rasterio")

    rows, cols = ndvi_array.shape

    # from_bounds(west, south, east, north, width, height)
    transform = from_bounds(
        bbox.min_x, bbox.min_y, bbox.max_x, bbox.max_y,
        cols, rows
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(
        out_path,
        mode="w",
        driver="GTiff",
        height=rows,
        width=cols,
        count=1,
        dtype=np.float32,
        crs=RioCRS.from_epsg(4326),   # WGS84
        transform=transform,
        nodata=nodata,
        compress="lzw",
        predictor=3,   # floating-point predictor — improves FLOAT32 compression
        tiled=True,
        blockxsize=256,
        blockysize=256,
    ) as dst:
        dst.write(ndvi_array.astype(np.float32), 1)
        dst.update_tags(
            DESCRIPTION="Sentinel-2 Quarterly Cloudless Mosaic NDVI",
            COLLECTION_ID=COLLECTION_ID,
            RESOLUTION_M=str(RESOLUTION_M),
        )

    print(f"  → Saved: {out_path}  ({cols} × {rows} px, {out_path.stat().st_size / 1e6:.1f} MB)")


def download_ndvi(year: int, quarter: str, config):
    """
    Download NDVI for one quarter and save it as a GeoTIFF.

    Returns the output file path.
    """
    try:
        from sentinelhub import (
            BBox, CRS, DataCollection,
            MimeType, SentinelHubRequest,
            bbox_to_dimensions,
        )
    except ImportError:
        sys.exit("ERROR: sentinelhub not installed — run:  pip install sentinelhub")

    start_date, end_date = quarter_interval(year, quarter)
    print(f"\n{'='*60}")
    print(f"  Quarter : {year} {quarter}  ({start_date}  →  {end_date})")

    # ── BBox & image size ─────────────────────────────────────────────────────
    bbox = BBox(
        bbox=(AOI_BBOX["west"], AOI_BBOX["south"], AOI_BBOX["east"], AOI_BBOX["north"]),
        crs=CRS.WGS84,
    )
    img_size = bbox_to_dimensions(bbox, resolution=RESOLUTION_M)
    print(f"  Image   : {img_size[0]} × {img_size[1]} px  @ {RESOLUTION_M} m")

    # ── Collection ────────────────────────────────────────────────────────────
    mosaic_collection = DataCollection.define_byoc(collection_id=COLLECTION_ID)

    # ── Request ───────────────────────────────────────────────────────────────
    request = SentinelHubRequest(
        evalscript=EVALSCRIPT_NDVI,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=mosaic_collection,
                # The quarterly mosaic has one "scene" per quarter.
                # Use the entire quarter window so the mosaic tile is matched.
                time_interval=(start_date, end_date),
            )
        ],
        responses=[
            SentinelHubRequest.output_response("ndvi",     MimeType.TIFF),
            SentinelHubRequest.output_response("dataMask", MimeType.TIFF),
        ],
        bbox=bbox,
        size=img_size,
        config=config,
    )

    print("  Fetching data from Copernicus Data Space …")
    data = request.get_data()

    # ── Unpack response ───────────────────────────────────────────────────────
    # get_data() returns a list; each item is a dict of band arrays
    if not data or not isinstance(data[0], dict):
        print("  WARNING: Unexpected response format. Dumping raw result.")
        print(data)
        return None

    result      = data[0]
    ndvi_arr    = result.get("ndvi.tif")
    datamask    = result.get("dataMask.tif")

    if ndvi_arr is None:
        # Fall-back: single-output response is returned as a bare array
        ndvi_arr = list(result.values())[0]
        datamask = None

    # Shape is (rows, cols, 1) → squeeze to (rows, cols)
    ndvi_arr = np.squeeze(ndvi_arr)

    # Apply dataMask so no-data pixels become NaN
    if datamask is not None:
        mask = np.squeeze(datamask).astype(bool)
        ndvi_arr[~mask] = np.nan

    # Sanity check
    valid = ndvi_arr[~np.isnan(ndvi_arr)]
    if valid.size > 0:
        print(f"  NDVI    : min={valid.min():.4f}  mean={valid.mean():.4f}  max={valid.max():.4f}")
        coverage = 100.0 * valid.size / ndvi_arr.size
        print(f"  Coverage: {coverage:.1f}% valid pixels")
    else:
        print("  WARNING: All pixels are NaN — no valid observations for this quarter/area.")

    # ── Save ──────────────────────────────────────────────────────────────────
    out_path = OUTPUT_DIR / f"ndvi_{year}_{quarter}.tif"
    save_geotiff(ndvi_arr, bbox, out_path)

    return out_path


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Sentinel-2 Quarterly Cloudless Mosaic NDVI as GeoTIFF",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--year", type=int, default=datetime.date.today().year,
        help="Year to download (default: current year)"
    )
    parser.add_argument(
        "--quarter", choices=["Q1", "Q2", "Q3", "Q4"],
        help="Single quarter to download (Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec)"
    )
    parser.add_argument(
        "--all-quarters", action="store_true",
        help="Download all four quarters for the given year"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=OUTPUT_DIR,
        help="Directory where GeoTIFFs will be saved (default: ./ndvi_output)"
    )
    parser.add_argument(
        "--list-quarters", action="store_true",
        help="Print available quarter date ranges and exit"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    global OUTPUT_DIR
    OUTPUT_DIR = args.output_dir

    if args.list_quarters:
        print("Available quarters (year is user-supplied):")
        for q, (s, e) in QUARTERS.items():
            print(f"  {q}: {args.year}-{s}  →  {args.year}-{e}")
        return

    config = build_config()

    # Determine which quarters to fetch
    if args.all_quarters:
        quarters_to_fetch = list(QUARTERS.keys())
    elif args.quarter:
        quarters_to_fetch = [args.quarter]
    else:
        # Default: most recently completed quarter
        today = datetime.date.today()
        month = today.month
        if   month <= 3:  q = "Q4"; y = args.year - 1
        elif month <= 6:  q = "Q1"; y = args.year
        elif month <= 9:  q = "Q2"; y = args.year
        else:             q = "Q3"; y = args.year
        quarters_to_fetch = [q]
        args.year = y
        print(f"No quarter specified — defaulting to most recently completed quarter: {y} {q}")

    saved = []
    for q in quarters_to_fetch:
        path = download_ndvi(year=args.year, quarter=q, config=config)
        if path:
            saved.append(path)

    print(f"\n{'='*60}")
    print(f"Done. {len(saved)} file(s) written to:  {OUTPUT_DIR.resolve()}")
    for p in saved:
        print(f"  {p.name}")

    print("""
─────────────────────────────────────────────────────────────
GeoTIFF properties:
  • CRS    : EPSG:4326 (WGS84)
  • Values : FLOAT32 NDVI in [-1 .. 1], NaN = no data
  • Compression: LZW with floating-point predictor
  • Resolution : 10 m (native mosaic resolution)

Opening in QGIS:
  Drag the .tif file into QGIS. Set band rendering to
  Singleband pseudocolor, use a diverging ramp centred on 0,
  or import the NDVI colour ramp from the Copernicus script.

Opening in Python:
  import rasterio, numpy as np
  with rasterio.open("ndvi_output/ndvi_2024_Q2.tif") as src:
      ndvi = src.read(1)          # shape (rows, cols), float32
      nodata = src.nodata         # np.nan
      transform = src.transform   # affine georeferencing
─────────────────────────────────────────────────────────────
""")


if __name__ == "__main__":
    main()