import copy
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from torch import nn
import rasterio as rio
from torch.utils.data import Dataset, DataLoader

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

    def __getitem__(self, index):
        observation = self.data_set.iloc[index]

        year = observation["Year"]
        posX = observation["PosX"]
        posY = observation["PosY"]

        bands_to_ret=[]

        year_to_index = int(year)-self.min_band_year

        #bands_to_ret = self.bands[year_to_index - 2: year_to_index + 2,: posX, posY]

        for y in range(year_to_index - 2, year_to_index + 2):
            bands_to_ret.append(self.bands[y, : , posX, posY]) # for point at [posX, posY] [year, band]
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
        bands_tenser = torch.tensor(
            QuarterBandsDataset.digitalN_to_reflectance(bands_to_ret),
            dtype=torch.float32
        )

        label_tensor = torch.tensor(1 if (observation["Label"] == "Poz") else 0, dtype=torch.float32)

        return bands_tenser, label_tensor

    @staticmethod
    def digitalN_to_reflectance(values:np.ndarray) -> np.ndarray:
        return values/1000




class MyGRU(nn.Module):
    HIDDEN_SIZE = 32
    NUM_LAYER = 1
    DROPOUT = 0.0

    def __init__(self, loss_fn):
        super().__init__()

        self.gru = nn.GRU(input_size=4, hidden_size=MyGRU.HIDDEN_SIZE, num_layers=MyGRU.NUM_LAYER, dropout=MyGRU.DROPOUT, batch_first=True)
        self.head = nn.Linear(MyGRU.HIDDEN_SIZE, 1)

        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.loss_fn = loss_fn
        self.prediction_handle = lambda output: (torch.sigmoid(output) > 0.5).int()

    def forward(self, x):
        _, h = self.gru(x)
        return self.head(h[-1]).squeeze(-1)

    def train_one_epoch(self, optimizer, epoch:int, data_loader:DataLoader):
        # total_loss = 0.0
        # correct = 0
        # total = 0

        running_loss = 0.0
        last_loss = 0.0
        running_acc = 0.0
        last_acc = 0.0

        self.train()
        for i, (x, y) in enumerate(data_loader):
            x, y = x.to(self.device), y.to(self.device)

            optimalizer.zero_grad()

            logits = self(x)

            loss = self.loss_fn(logits, y)
            loss.backward()
            optimizer.step()

            # total_loss += loss.item()
            # predictions = self.prediction_handle(logits)
            # correct += predictions.eq(y.view_as(predictions)).sum().item()
            # total += data_loader.batch_size

            running_loss += loss.item()

            pred = self.prediction_handle(logits)

            correct = pred.eq(y.view_as(pred)).sum().item()
            running_acc += correct / len(y) * 100
            if i % 100 == 99:
                last_loss = running_loss / 100
                last_acc = running_acc / 100
                running_loss = 0.0
                running_acc = 0.0
                print("*", end="")
                # print(
                #     f"\nTrain Epoch {epoch + 1}: batch {i} - {(i * data_loader.batch_size)}/{len(data_loader.dataset)}, loss: {last_loss:.4f}, acc: {last_acc:.4f}%")


        # return total_loss / total, correct / total * 100
        return last_loss, last_acc



    def evaluate(self, loader):
        self.eval()

        running_vloss = 0.0
        correct = 0
        trues = []
        predictions = []

        with torch.no_grad():
            for i, (data, target) in enumerate(loader):
                data = data.to(self.device)
                target = target.to(self.device)

                output = self(data)

                vloss = self.loss_fn(output, target)
                running_vloss += vloss.item()

                pred = self.prediction_handle(output)

                predictions.extend(pred.cpu().numpy())
                trues.extend(target.cpu().numpy())
                correct += pred.eq(target.view_as(pred)).sum().item()

                if(i%100==99):
                    print("*", end="")

        avg_vloss = running_vloss / len(loader)
        return avg_vloss, correct, predictions, trues


#-----------------------------------------------

if __name__ == "__main__":
    EPOCHS = 30
    curTime = datetime.now()

    BASE_DIR = Path("/home/main/repositories/RemoteSensing/MachineLearning/") / f"Model_{curTime.strftime("%Y%m%d%H%M%S")}"
    BEST_MODEL_SAVE = BASE_DIR / "BestModel.pt"
    PROGRESS_SAVE = BASE_DIR / "Progress.csv"

    BASE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Base directory: {BASE_DIR}")

    dsTrain = QuarterBandsDataset(SPECTRAIL_INVENTORY, TRAIN_PATH)
    dataloaderTrain = DataLoader(dsTrain, batch_size=64, shuffle=True)
    dsValid = QuarterBandsDataset(SPECTRAIL_INVENTORY, VALID_PATH)
    dataloaderValid = DataLoader(dsValid, batch_size=64, shuffle=False)

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(device)


    loss_fn = nn.BCEWithLogitsLoss()
    model = MyGRU(loss_fn).to(device)
    optimalizer = torch.optim.Adam(model.parameters(), lr=0.002)


    best_valid = -1

    print("\nEvaluating...")
    # avg_vloss, vcorrect, vpreds, vtrues = model.evaluate(dataloaderValid)
    # vacc = vcorrect / len(vtrues) * 100
    # print(f'\nEpoch 0/{EPOCHS} validation_acc={vacc:.3f} validation_loss={vacc:.4f}')

    for epoch in range(1, EPOCHS+1):
        print(f"\nepoch {epoch}")

        last_train_loss, last_train_acc = model.train_one_epoch(optimalizer, epoch, dataloaderTrain)

        print("\nEvaluating...")
        avg_vloss, vcorrect, vpreds, vtrues = model.evaluate(dataloaderValid)
        vacc = vcorrect / len(vtrues) * 100


        print(f'\nEpoch {epoch:02d}/{EPOCHS}  train_acc={last_train_acc:.3f}  '
              f'validation_acc={vacc:.3f}  train_loss={last_train_loss:.4f}  validation_loss={avg_vloss:.4f}')

        row = pd.DataFrame([{'epoch': epoch, 'train_loss': last_train_loss, 'train_accuracy': last_train_acc,
                             'validation_loss': avg_vloss, 'validation_accuracy': vacc}])
        row.to_csv(PROGRESS_SAVE, mode='a' if epoch > 1 else 'w',
                   header=(epoch == 1), index=False)

        if vacc > best_valid:
            best_valid = vacc
            torch.save({'model_state_dict': copy.deepcopy(model.state_dict())},
                       BEST_MODEL_SAVE)

            print(f"Better model saved to {BEST_MODEL_SAVE}")


