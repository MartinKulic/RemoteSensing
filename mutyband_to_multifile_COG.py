import os

import numpy as np
import pandas as pd
import rasterio
from rasterio import MemoryFile
from rio_cogeo import cog_profiles, cog_translate

INPUT_GTIFF = "/home/main/Downloads/slovakia/annual_disturbances_1985_2023_slovakia.tif"
INPUT_GTIFF_TIME_SPAND = (1985, 2023)
OUTPUT_DIRECTORY = "/home/main/repositories/RemoteSensing/MachineLearning/dataset/layered_disturbance"


def save_cog_gtiff(data: np.ndarray, profile: dict, out_path: str, desc: str = "NoDescription"):
    # easy way
    # self.save_gtiff(data, profile, out_path)
    # cog_translate(str(out_path), str(out_path), cog_profiles.get("deflate"))
    # ----------------------

    with MemoryFile() as memfile:
        with memfile.open(**profile) as mem:
            mem.write(data, 1)
            mem.update_tags(
                DESCRIPTION=desc
            )

            dst_profile = cog_profiles.get("deflate")
            # dst_profile.update({"blockxsize": 256, "blockysize": 256})

            # Translate to COG on disk
            cog_translate(
                mem,
                str(out_path),
                dst_profile,
                nodata=0,
                use_cog_driver=True,
                in_memory=False
            )


with rasterio.open(INPUT_GTIFF) as src:
    src_array = src.read()
    profile = src.profile

#1985 - 2023
# 15 16 17 18 19 20 21 22 23
#  1  2  3  4  5  6  7  8  9
print(src_array.shape)
relevant_years = src_array[-(INPUT_GTIFF_TIME_SPAND[1]-2015+1):,:,:]
print(relevant_years.shape)

inventory_log_np = pd.DataFrame(columns=["Year", "Quarter", "Path"])
quarter = 3

in_year_profile = profile.copy()
in_year_profile.update(
    dtype=rasterio.uint8,
    count=1,
    nodata=0
)

for i in range(1,INPUT_GTIFF_TIME_SPAND[1]-2015+1):
    cog_out = os.path.join(OUTPUT_DIRECTORY, f"{INPUT_GTIFF_TIME_SPAND[1]-i+1}_{quarter}_EUdisturbance_COG.tiff")
    save_cog_gtiff(src_array[-i,:,:], in_year_profile, cog_out, "EUdisturbance_COG")
    inventory_log_np.loc[len(inventory_log_np)] = [INPUT_GTIFF_TIME_SPAND[1]-i+1, quarter, cog_out]

inventory_log_np.to_csv(os.path.join(OUTPUT_DIRECTORY, "inventory.csv"))

