# import pandas as pd
# from pathlib import Path
#
# import rasterio
# from rasterio.windows import Window
#
# SOURCE_DISTURBANCE_PATH = Path("/home/main/Downloads/slovakia/annual_disturbances_1985_2023_slovakia.tif")
# INVENTORY_BANDS_CSV=Path("/home/main/repositories/RemoteSensing/Qarterly_mosaics_30m_VelkaFatra/inventory.csv")
# FOREST_MASK_TIF=Path("/home/main/repositories/RemoteSensing/MachineLearning/dataset/forest_mask_slovakia.tif")
#
# disturbance_inv = pd.read_csv(INVENTORY_BANDS_CSV, sep=";")
#
# # 1. Get the target bounds (in geographic coordinates)
# with rasterio.open(disturbance_inv.iloc[0]['Path']) as src:
#     AIO = src.bounds  # (left, bottom, right, top) in AIO's CRS
#
#
# def crop_and_save(path_to_target, AIO_bounds, output_path):
#
#     with rasterio.open(path_to_target) as src:
#         AIO_window = rasterio.windows.from_bounds(
#             AIO_bounds.left,
#             AIO_bounds.bottom,
#             AIO_bounds.right,
#             AIO_bounds.top,
#             transform=src.transform
#         )
#         target_mask = src.read(window=AIO_window)
#         profile = src.profile
#         profile['width'] = int(AIO_window.width)
#         profile['height'] = int(AIO_window.height)
#         profile['transform'] = src.window_transform(AIO_window)
#
#     with rasterio.open(output_path, "w", **profile) as dst:
#         dst.write(target_mask)
#
#
# crop_and_save(FOREST_MASK_TIF, AIO, "/home/main/repositories/RemoteSensing/MachineLearning/dataset/cropped_forest_mask_slovakia.tif")
# crop_and_save(SOURCE_DISTURBANCE_PATH, AIO, "/home/main/repositories/RemoteSensing/MachineLearning/dataset/cropped_annual_disturbances_1985_2023_slovakia.tif")


import pandas as pd
from pathlib import Path
import rasterio
from rasterio.windows import Window

SOURCE_DISTURBANCE_PATH = Path(
    "/home/main/Downloads/slovakia/"
    "annual_disturbances_1985_2023_slovakia.tif"
)

FOREST_MASK_TIF = Path(
    "/home/main/repositories/RemoteSensing/"
    "MachineLearning/dataset/forest_mask_slovakia.tif"
)

INVENTORY_BANDS_CSV = Path(
    "/home/main/repositories/RemoteSensing/"
    "Qarterly_mosaics_30m_VelkaFatra/inventory.csv"
)

OUTPUT_DIR = Path(
    "/home/main/repositories/RemoteSensing/"
    "MachineLearning/dataset"
)

inventory = pd.read_csv(
    INVENTORY_BANDS_CSV,
    sep=";"
)

sentinel_path = Path(inventory.iloc[0]["Path"])


def crop_to_sentinel(source_path, sentinel_path, output_path):

    with rasterio.open(sentinel_path) as sentinel, \
         rasterio.open(source_path) as src:

        if src.crs != sentinel.crs:
            raise ValueError("CRS mismatch")

        if src.res != sentinel.res:
            raise ValueError("Resolution mismatch")

        col_float = (
            sentinel.bounds.left - src.bounds.left
        ) / src.res[0]

        row_float = (
            src.bounds.top - sentinel.bounds.top
        ) / src.res[1]

        col = round(col_float)
        row = round(row_float)

        if (
            abs(col_float - col) > 1e-6
            or abs(row_float - row) > 1e-6
        ):
            raise ValueError(
                "Pixel grids are not aligned. "
                "Redownload Sentinel using the reference grid."
            )

        window = Window(
            col_off=col,
            row_off=row,
            width=sentinel.width,
            height=sentinel.height
        )

        if (
            col < 0 or row < 0
            or col + sentinel.width > src.width
            or row + sentinel.height > src.height
        ):
            raise ValueError("Sentinel extent outside source raster")

        data = src.read(window=window)

        profile = src.profile.copy()
        profile.update(
            width=sentinel.width,
            height=sentinel.height,
            transform=src.window_transform(window)
        )

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with rasterio.open(
            output_path, "w", **profile
        ) as dst:
            dst.write(data)


crop_to_sentinel(
    SOURCE_DISTURBANCE_PATH,
    sentinel_path,
    OUTPUT_DIR / "cropped_annual_disturbances_1985_2023_slovakia.tif"
)

crop_to_sentinel(
    FOREST_MASK_TIF,
    sentinel_path,
    OUTPUT_DIR / "cropped_forest_mask_slovakia.tif"
)