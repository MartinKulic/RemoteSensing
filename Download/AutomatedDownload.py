from sentinelhub import BBox, CRS
from Download_app import Worker
from pathlib import Path



bbox = BBox(
    bbox=(18.50000, 48.77910, 18.83029, 49.01070),
    crs=CRS.WGS84,
)

START_YEAR = 2015
END_YEAR = 2025

QUARTER = 3

BANDS =  ["B04", "B08", "observations"]
OUT_DIR = Path("./Quarter_mosaics")

inventory_file_path = OUT_DIR / "inventory.csv"
w = Worker(START_YEAR, QUARTER, bbox, OUT_DIR, BANDS, Path("./Secret"))

inventory_file_path.parent.mkdir(parents=True, exist_ok=True)
with open(inventory_file_path, "w") as f:
  f.write("Year; Quarter; Band; Path;\n")


for year in range(START_YEAR, END_YEAR+1):
    w.year = year
    out_files = w.work()
    with open(inventory_file_path, "a") as f:
        for b, p in zip(BANDS, out_files):
            f.write(f"{year};{QUARTER};{b};{p};\n")