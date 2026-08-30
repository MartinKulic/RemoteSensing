from sentinelhub import (
            SHConfig, BBox, CRS, DataCollection, MimeType,
            SentinelHubRequest, bbox_to_dimensions,
        )

import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS as RioCRS
import numpy as np

from pathlib import Path
import argparse


class Worker:
    AVAILABLE_BANDS=["B02", "B03", "B04", "B08", "observations", "dataMask"]
    QUARTERS = [
        # API takes yyyy-mm-dd
        ("01-01", "01-2"), #("01-01", "03-31")
        ("04-01", "04-2"), #("04-01", "06-30")
        ("07-01", "07-2"), #("07-01", "09-30")
        ("10-01", "10-2"), #("10-01", "12-31")
    ]
    RESOLUTION    = 10
    COLLECTION_ID = "5460de54-082e-473a-b6ea-d5cbe3c17cca"

    def __init__(self, year, quarter, bbox, path_out, bands, path_secret = "./Secret"):
        self.output_dir    = path_out
        self.year = year
        self.quarter = quarter
        self.bbox = bbox
        self.client_id, self.client_secret = self.__get_secret(path_secret)
        self.config = Worker.build_config(self.client_id, self.client_secret)
        self.bands = bands

        self.img_size = bbox_to_dimensions(bbox, resolution=Worker.RESOLUTION)
        print(f"        Image   : {self.img_size[0]} × {self.img_size[1]} px @ {Worker.RESOLUTION} m")

        self.eval_script = self.__build_Evalscript()
        self.colection = DataCollection.define_byoc(collection_id=Worker.COLLECTION_ID)


    '''
    B04          - Red                              - DN => FV*1000
    B08          - NIR                              - DN => FV*1000
    observations - The number of valid observations
    dataMask     - The mask of data/no data pixels
    
    For each pixel, for each band (B02, B03, B04, B08): Take the value of the first quartile and multiply it by 10000 (to get a ‘digital number’). This is an output value.
    
    For each pixel, for each band (B02, B03, B04, B08): If there are no valid observations, output the value -32768, which represents no data
    For the observations band, output the value 0, which also represents no data
    '''
    def __build_Evalscript(self):
        # input_bands_str = ", ".join([f'"{b}"' for b in Bands])
        # return_values_str = ", ".join([f"samples.{b}" for b in Bands])

        bands = self.bands
        EVALSCRIPT = f"""
        //VERSION=3
        function setup() {{
            return {{
                input: [{", ".join([f'"{b}"' for b in bands])}],
                output: {{
                    id: "default",
                    bands: {len(bands)},
                    sampleType: "FLOAT32"
                }}
            }};
        }}
        
        function evaluatePixel(samples) {{
            return [{", ".join([f"samples.{b}" for b in bands])}];
        }}
        """
        return EVALSCRIPT


    @staticmethod
    def get_time_interval(year: int, quarter: int):
        if quarter < 0 or quarter > 4:
            raise ValueError(f"Quarter must be from 1 to 4 not {quarter}")

        start, end = Worker.QUARTERS[quarter-1]
        return f"{year}-{start}", f"{year}-{end}"

    @staticmethod
    def __get_secret(path_to_secret):
        with open(path_to_secret, 'r', encoding='utf-8') as file:
            file.readline()
            client_id = file.readline().strip()
            file.readline()
            client_secret = file.readline().strip()
        return client_id, client_secret

    def build_config(CLIENT_ID:str, CLIENT_SECRET:str):
        config = SHConfig()

        config.sh_client_id = CLIENT_ID
        config.sh_client_secret = CLIENT_SECRET
        config.sh_token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        config.sh_base_url = "https://sh.dataspace.copernicus.eu"

        return config

    def __download(self, start_date, end_date):

        print(f"\n{'='*64}")
        print(f"  Quarter : {self.year} {self.quarter}  ({start_date} → {end_date})")


        request = SentinelHubRequest(
            evalscript=self.eval_script,
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=self.colection,
                    time_interval=(start_date, end_date),
                )
            ],
            responses=[
                SentinelHubRequest.output_response("default", MimeType.TIFF),
            ],
            bbox=self.bbox,
            size=self.img_size,
            config=self.config,

        )


        print("  Fetching …")
        raw = request.get_data()

        return raw

    def __save_geotiff(self, pixels, out_path, dtype = np.float32, no_data=-32768):
        rows, cols = pixels.shape
        transform = from_bounds(
            self.bbox.min_x, self.bbox.min_y, self.bbox.max_x, self.bbox.max_y,
            cols, rows
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with rasterio.open(
                out_path,
                mode = "w",
                driver="GTiff",
                height=rows,
                width=cols,
                count=1,
                dtype=dtype,
                crs=RioCRS.from_epsg(4326),
                transform=transform,
                nodata=no_data,
                compress="lzw",
                predctor=3,
                tiled=True,
        ) as dst:
            dst.write(pixels.astype(dtype), 1)
            dst.update_tags(
                DESCRIPTION="Sentinel-2 Quarterly Cloudless Mosaic NDVI",
                COLLECTION_ID=Worker.COLLECTION_ID,
                RESOLUTION_M=str(Worker.RESOLUTION),
            )

        size_mb = out_path.stat().st_size / 1e6
        print(f"  → Saved: {out_path}  ({cols}×{rows} px, {size_mb:.1f} MB)")

    def work(self):
        out_paths = []
        start_date, end_date = Worker.get_time_interval(self.year, self.quarter)

        raw = self.__download( start_date, end_date )
        raw_arr = np.array(raw)

        if(raw_arr.shape[0] < 1):
            raise BaseException(f"No observations in given range {start_date} - {end_date}")
        if(raw_arr.shape[0] > 1):
            raise BaseException(f"More observation ({raw_arr.shape[0]}) in selected time {start_date} - {end_date}. This should not happen. Check manually available data. [-y {self.year} -q {self
            .quarter}]")

        # for i in range( raw_arr.shape[0] ):
        #     cur_obs = raw_arr[i]
        #     for i, b in enumerate(self.bands):
        #         out_path = self.output_dir / f"{year}_Q{quarter}_{b}{"" if raw_arr.shape[0] == 1 else i}.tif"
        #         self.__save_geotiff(cur_obs[:, :, i], out_path)
        #         out_paths.append(out_path)
        cur_obs = raw_arr[0]
        for i, b in enumerate(self.bands):
            out_path = self.output_dir / f"{self.year}_Q{self.quarter}_{b}{"" if raw_arr.shape[0] == 1 else i}.tif"
            self.__save_geotiff(cur_obs[:, :, i], out_path)
            out_paths.append(out_path.resolve())
        return out_paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='QuarterlyDownload',
        description='Download quarterly products from Sentinel hub',
        epilog='Text at the bottom of help')

    parser.add_argument('-y', '--year', required=True, type=int, help='Year - time of data collection')
    parser.add_argument('-q', '--quarter', required=True, type=int, help='Quarter of year of data collection',
                        choices=[1, 2, 3, 4])
    parser.add_argument('-x', '--bbox', type=float, nargs=4,
                        default=(18.50000, 48.77910, 18.83029, 49.01070),
                        metavar=('WEST', 'SOUTH', 'EAST', 'NORTH'),
                        help="Area of interest in format west #←   south #↓   east #→   north #↑")
    parser.add_argument('-o', '--output_dir', default="./out", type=Path, help="Where should tiffs be saved")
    parser.add_argument('-s', '--secret', default="./Secret", type=Path, help="Path to file where are oauth id ind secret located in specific format")
    parser.add_argument('-b', '--bands', nargs='+', type=str, choices=Worker.AVAILABLE_BANDS,
                        default=["B04", "B08", "observations", "dataMask"],
                        help="Bands to download")

    args = parser.parse_args()
    year = args.year
    quarter = args.quarter
    west, south, east, north = args.bbox
    out_path = args.output_dir
    secret_path = args.secret
    bands = args.bands

    # AOI_BBOX = {
    #     "west": west,  # ←
    #     "south": south,  # ↓
    #     "east": east,  # →
    #     "north": north,  # ↑
    # }

    bbox = BBox(
        bbox=(west, south,
              east, north),
        crs=CRS.WGS84,
    )

    worker = Worker(year, quarter, bbox, out_path, bands, secret_path )
    worker.work()
