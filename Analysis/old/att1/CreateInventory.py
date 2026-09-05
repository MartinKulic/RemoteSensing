# cut observations to specified location and creates CSV which organizes
import xarray as xr
import os
import re
import pandas as pd

lat_min, lat_max = 49.04104, 48.78376
lon_min, lon_max = 18.51635, 18.80255
# so res shape is 100x100
long_add = 0.0050
lat_add = 0.0200


def proces_one_nc(base_path, name, out_file, out_table):
    full_path = os.path.join(base_path, name)
    nc = xr.open_dataset(full_path)
    nc_croped = nc.sel(
        lat=slice(lat_min + lat_add, lat_max - lat_add),
        lon=slice(lon_min - long_add, lon_max + long_add)
    )

    out_full_path = os.path.join(out_file, name)
    nc_croped.to_netcdf(out_full_path)
    nc_croped.close()
    nc.close()

    match = re.findall(r"_(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<rest>\d{4})_", name)
    match_list = match[0]
    year = match_list[0]
    month = match_list[1]
    day = match_list[2]

    # year = match.group("year")
    # month = match.group("month")
    # day = match.group("day")

    # out_table[name] = [year, month, day, full_path]
    out_table.loc[len(out_table)] = [name, year, month, day, out_full_path]





base_input_dir = "/run/media/martin/KINGSTON/NDVI/ndvi_europe/"
condent_of_base_dir = os.listdir(base_input_dir)

out_db = pd.DataFrame(columns=["full_name", "year", "month", "day", "path"])
out_db.set_index("full_name")


OUTPUT_DIR = "/run/media/martin/KINGSTON/NDVI/test_area"
CSV_OUTPUT_PATH = "/run/media/martin/KINGSTON/NDVI/test_area/log.csv"
for content_name in condent_of_base_dir:
    path = os.path.join(base_input_dir, content_name)
    print(path)
    if (not path.endswith(".nc")):
        continue
    proces_one_nc(base_input_dir, content_name, OUTPUT_DIR, out_db)

out_db.to_csv(CSV_OUTPUT_PATH)