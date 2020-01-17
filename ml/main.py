import sys
if sys.version_info[1] <= 5:
    pass
    # -*- coding: future_fstrings -*-

import torch
from torch.utils.data import DataLoader
from tqdm import tqdm
import tensorboardX
import numpy as np

from dataset import OMOP_Base
from model import Net

if __name__ == '__main__':
    # model_path = sys.argv[1]
    model_path = "model_small.pt"
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    print(device)
    ds = OMOP_Base(None, path="synpuf_cdm.samples.pkl")

    model = Net(len(ds[0][0])).to(device)
    model.load(model_path)
    for param in model.parameters():
        param.requires_grad = False
        param.retain_grad()

    feature_importance = np.zeros((len(ds), len(ds[0][0])), dtype=np.float)
    for i, batch in enumerate(tqdm(ds)):
        features, labels = batch

        features.requires_grad = True
        features = features.to(device)
        labels = labels.to(device)

        output = model(features)

        if labels[0]:
            loss = (output - 0).pow(2)*1000
        else:
            loss = (output - 1).pow(2)*1000

        loss.backward()

        feature_importance[i] = features.grad.numpy()

    print(feature_importance.max())


