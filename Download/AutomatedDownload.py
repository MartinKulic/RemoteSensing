from sentinelhub import BBox, CRS
from Download_app import Worker
from pathlib import Path



bbox = BBox(
    bbox=(4938500.0,
          2882200.0,
          5013450.0,
          2957180.0),
    crs=CRS(3035),
)
bbox = Worker.align_bbox_to_disturbance(bbox)

START_YEAR = 2015
END_YEAR = 2025

QUARTER = 3

BANDS =  ["B02", "B03", "B04", "B08", ]
OUT_DIR = Path("../Qarterly_mosaics_30m_VelkaFatra")

inventory_file_path = OUT_DIR / "inventory.csv"
w = Worker(START_YEAR, QUARTER, bbox, OUT_DIR, BANDS, Path("./Secret"))

inventory_file_path.parent.mkdir(parents=True, exist_ok=True)
with open(inventory_file_path, "w") as f:
  f.write("Year;Quarter;Band;Path;\n")


for year in range(START_YEAR, END_YEAR+1):
    w.year = year
    out_files = w.work()
    with open(inventory_file_path, "a") as f:
        for b, p in zip(BANDS, out_files):
            f.write(f"{year};{QUARTER};{b};{p};\n")