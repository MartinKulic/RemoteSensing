from pathlib import Path
import pandas as pd
import numpy as np
import rasterio

import argparse

class NDVI_Calculator:

    def __init__(self, in_inventory_path : Path, out_dir : Path):
        self.out_dir = out_dir

        self.inventory = pd.read_csv(in_inventory_path, sep=';', header=0)
        self.inventory = self.inventory.set_index(["Year", "Quarter", "Band"])

    def Read_Band(self, year, quarter, band) -> np.ndarray:
        with rasterio.open(self.inventory.loc[year, quarter, band]["Path"], "r") as src:
            band = np.array(src.read())

            if len(band.shape) > 2:
                if band.shape[0] == 1:
                    band = band[0, :, :]
                else:
                    raise BaseException(f"Cannot work with array of shape {band.shape}\n"
                                        f"   Incompatible shape encounterad at year {year} band {band} file {self.inventory.loc[year, band]["Path"]}")
        return band

    def Calculate_ndvi(self,year, quarter):
        # year_bands = inventory[ (inventory["Year"] == year) ]

        red = self.Read_Band(year, quarter, "B04")
        nir = self.Read_Band(year, quarter, "B08")
        observations = self.Read_Band(year, quarter, "observations")

        red[red == -32768] = np.nan
        nir[nir == -32768] = np.nan

        red[red < 0] = 0
        nir[nir < 0] = 0  # because not harmonized data

        ndvi = (nir - red) / (nir + red)
        ndvi[red == -32768] = np.nan
        ndvi[nir == -32768] = np.nan
        ndvi[observations < 1] = np.nan

        ndvi_clipped = np.clip(ndvi, -1, 1)  # just in case

        return ndvi_clipped, observations

    def Calculate_all(self):
        out_inventory_path = self.out_dir / "ndvi_inventory.csv"
        out_inventory_path.parent.mkdir(parents=True, exist_ok=True)

        # get coords, transform ...
        with rasterio.open(self.inventory.iloc[0]["Path"], "r") as src:
            profile = src.profile
        profile.update(count=2, dtype="float32", predctor=15)

        with open(out_inventory_path, "w") as f:
            f.write("Year;Quarter;Path;\n")

        # for year in self.inventory.index.unique(level="Year"):
        #     quarters = self.inventory.loc[year].index.unique(level="Quarter")
        #     for quarter in quarters:
        for (year, quarter), group_df in self.inventory.groupby(level=["Year", "Quarter"]):

                print(f"Calculating NDVI for {year} quarter {quarter}")
                ndvi, observations = self.Calculate_ndvi(year, quarter)

                print(f"   {np.nanmin(ndvi)} - {np.nanmax(ndvi)}   N/A-s {np.sum(np.isnan(ndvi))}")

                raster = np.array([ndvi, observations])
                cur_tiff_path = self.out_dir / f"{year}_{quarter}_NDVI.tif"

                with rasterio.open(cur_tiff_path, "w", **profile) as dst:
                    dst.update_tags(
                        DESCRIPTION=f"NDVI quarterly composite and number of available observation - year {year}")
                    dst.write(raster)
                print(f"Saving geotiff {cur_tiff_path} \n")

                with open(out_inventory_path, "a") as f:
                    f.write(f"{year};{quarter};{cur_tiff_path.resolve()};\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='NDVI Calculator',
        description='Calculate NDVI and creates 2 band geotiff from downloaded red, nir and observation\nGeotifs shold be organized in inventery table with year, quarter and path colums',
        epilog='')
    parser.add_argument('-i', '--inventory_path', required=True, type=Path, help='Path to inventory file')
    parser.add_argument('-o', '--out_dir', required=True, type=Path, help='Path to output directory')

    args = parser.parse_args()
    inventory_path = args.inventory_path
    out_dir = args.out_dir

    worker = NDVI_Calculator(inventory_path, out_dir)
    worker.Calculate_all()