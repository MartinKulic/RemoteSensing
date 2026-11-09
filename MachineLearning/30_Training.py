from os import path

import numpy as np
import pandas as pd
import torch
import rasterio as rio
from torch.utils.data import Dataset, DataLoader
from torch.utils.data.dataset import _T_co

TRAIN_PATH = "/home/main/repositories/RemoteSensing/MachineLearning/dataset/train.csv"
VALID_PATH = "/home/main/repositories/RemoteSensing/MachineLearning/dataset/valid.csv"
SPECTRAIL_INVENTORY = "/home/main/repositories/RemoteSensing/Qarterly_mosaics_30m_VelkaFatra/inventory.csv"


class QuarterBandsDataset(Dataset):
    def __init__(self, spectral_bands_inventory:str, data_set:str) -> None:
        super().__init__()

        self.bands_inventory = pd.read_csv(spectral_bands_inventory, sep=';')
        band_names = sorted(self.bands_inventory["Band"].unique())
        years = sorted(self.bands_inventory["Year"].unique())
        self.min_band_year = self.bands_inventory["Year"].min()
        self.bands_inventory = self.bands_inventory.set_index(["Year","Band"]).sort_index()

        self.data_set = pd.read_csv(data_set, sep=';')

        self.bands = []


        for y in years:
            bands_in_y=[]
            for band in band_names:
                with rio.open(self.bands_inventory.loc[(y,band)]["Path"], "r") as src:
                    tiff = src.read(1)
                    bands_in_y.append(tiff)
            self.bands.append(bands_in_y)
        self.bands = np.array(self.bands)



    def __len__(self):
        return len(self.data_set)

    def __getitem__(self, index) -> _T_co:
        observation = self.data_set.iloc[index]

        year = observation["Year"]
        posX = observation["PosX"]
        posY = observation["PosY"]

        bands_to_ret=[]

        year_to_index = int(year)-self.min_band_year

        for y in range(year_to_index - 2, year_to_index + 2):
            bands_to_ret.append(self.bands[y, : , posX, posY])
        # for y in range(year_to_index-2, year_to_index+2):
        #     bands_in_year_paths = self.bands_inventory[self.bands_inventory["Year"]==year].sort_values(by="Band")["Path"]
        #     bands_in_year = []
        #     for path in bands_in_year_paths:
        #         with rio.open(path, "r") as src:
        #             tiff = src.read(1)
        #             bands_in_year.append(tiff[observation["PosX"], observation["PosY"]])
        #
        #     bands.append(bands_in_year)

        bands_to_ret = np.array(bands_to_ret)
        return QuarterBandsDataset.digitalN_to_reflectance( bands_to_ret ), observation["Label"]

    @staticmethod
    def digitalN_to_reflectance(values:np.ndarray) -> np.ndarray:
        return values/1000


# class QuarterBandsDataset(Dataset):
#     def __init__(self, spectral_bands_inventory: str, data_set: str) -> None:
#         super().__init__()
#
#         # 1. Load CSVs
#         self.bands_inventory = pd.read_csv(spectral_bands_inventory, sep=';')
#         self.data_set = pd.read_csv(data_set, sep=';')
#
#         # 2. Get sorted unique keys
#         self.band_names = sorted(self.bands_inventory["Band"].unique())
#         self.years = sorted(self.bands_inventory["Year"].unique())
#
#         # 3. Preload all GeoTIFFs into RAM at startup
#         print("Preloading all GeoTIFFs into memory... Please wait.")
#         self.preloaded_bands = {}
#
#         # Temporary index for easy loading loop
#         load_inventory = self.bands_inventory.set_index(["Year", "Band"])
#
#         for y in self.years:
#             for band in self.band_names:
#                 try:
#                     path = load_inventory.loc[(y, band), "Path"]
#                     with rio.open(path, "r") as src:
#                         # Read the full 2D image layer into RAM as a NumPy array
#                         self.preloaded_bands[(y, band)] = src.read(1)
#                 except KeyError:
#                     # Handle missing year/band combinations if your inventory isn't perfectly complete
#                     print(f"Warning: Missing file path for Year {y}, Band {band}")
#                     self.preloaded_bands[(y, band)] = None
#
#         print("Preloading complete! Ready for training.")
#
#     def __len__(self):
#         return len(self.data_set)
#
#     def __getitem__(self, index):
#         observation = self.data_set.iloc[index]
#         target_year = int(observation["Year"])
#
#         # Extract coordinate indexes
#         pos_x = int(observation["PosX"])
#         pos_y = int(observation["PosY"])
#
#         bands_to_ret = []
#
#         # 4. Loop through the 4-year temporal window
#         for y in range(target_year - 2, target_year + 2):
#             bands_in_year = []
#
#             for band in self.band_names:
#                 # Retrieve the preloaded 2D array from RAM
#                 img_array = self.preloaded_bands.get((y, band), None)
#
#                 if img_array is not None:
#                     # Fast RAM-based pixel retrieval
#                     pixel_value = img_array[pos_x, pos_y]
#                     bands_in_year.append(pixel_value)
#                 else:
#                     # Fallback padding if the year/band combination was completely missing
#                     bands_in_year.append(0.0)
#
#             bands_to_ret.append(bands_in_year)
#
#         # 5. Build the final PyTorch Tensors
#         # Shape: [4, number_of_bands]
#         features = torch.tensor(bands_to_ret, dtype=torch.float32)
#         label = torch.tensor(observation["Label"])
#
#         return features, label

dsTest = QuarterBandsDataset(SPECTRAIL_INVENTORY, TRAIN_PATH)
print(dsTest.__getitem__(41016))

dataloaderTest = DataLoader(dsTest, batch_size=64, shuffle=True)
train_features, train_labels = next(iter(dataloaderTest))
bands = train_features[0]
label = train_labels[0]

print(bands)
print(label)
