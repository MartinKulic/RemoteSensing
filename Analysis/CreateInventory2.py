import xarray as xr
import os
import re
import pandas as pd

CSV_OUTPUT_PATH = "/home/martin/repositories/RemoteSensing/Download/ndvi_output/inventory.csv"
BASE_INPUT_DIR = "/home/martin/repositories/RemoteSensing/Download/ndvi_output"
condent_of_base_dir = os.listdir(BASE_INPUT_DIR)

map = {"Q1":1, "Q2":4, "Q3":7, "Q4":10}


out_db = pd.DataFrame(columns=["full_name", "year", "month", "path"])
out_db.set_index("full_name")

for content_name in condent_of_base_dir:
    path = os.path.join(BASE_INPUT_DIR, content_name)
    print(path)
    if (not path.endswith(".tif")):
        continue

    match = re.findall(r"_(?P<year>\d*)_(?P<quarter>Q\d)\.", content_name)
    match_list = match[0]
    year = match_list[0]
    quarter = match_list[1]

    month = map[quarter]

    out_full_path = os.path.join(BASE_INPUT_DIR, content_name)

    out_db.loc[len(out_db)] = [content_name, year, month, out_full_path]

out_db.to_csv(CSV_OUTPUT_PATH)