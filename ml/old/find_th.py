import torch
from torch.utils.data import DataLoader
from tqdm import tqdm
import tensorboardX
import numpy as np
import pickle
import matplotlib.pyplot as plt

from dataset import OMOP_Samples
from model import *

epsilon = 1e-16


def f1_score(input: torch.tensor, target: torch.tensor):
    pred = input
    assert (pred.min() >= 0 and pred.max() <= 1)
    tp = torch.sum(pred * target)
    fp = torch.sum((1 - target) * pred)
    fn = torch.sum(target * (1 - pred))
    tn = torch.sum((1 - pred) * (1 - target))

    soft_f1_class1 = 2 * tp / (2 * tp + fn + fp + epsilon)
    soft_f1_class0 = 2 * tn / (2 * tn + fn + fp + epsilon)
    double_f1 = 0.5 * (soft_f1_class1 + soft_f1_class0)

    p = tp / (tp + fp + epsilon)
    r = tp / (tp + fn + epsilon)
    f1 = 2 * p * r / (p + r + epsilon)
    assert (not torch.isinf(f1) or not torch.isnan(f1))
    return f1, p, r, double_f1


if __name__ == '__main__':

    model_path = "minimal_fc100fc_LR5e-04_BS1000_L-f1-wmse-r0,03_Ep1000.pt"
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    ds = OMOP_Samples("synpuf_cdm.minimal.pkl")
    dl = DataLoader(ds, batch_size=len(ds), shuffle=False, pin_memory=True, num_workers=8)

    # '''
    model = N_FC(input_size=len(ds[0][0]),
                 layers=[[100],
                         [1],
                         ])

    model2 = N_FC(input_size=len(ds[0][0]),
                  layers=[[100],
                          [1],
                          ])
    # '''

    # model = SVM(len(ds[0][0]))
    model.load(model_path, device=device)
    model2.load(model_path, device=device)

    labels = None
    output = None
    with torch.no_grad():
        for batch in tqdm(dl, desc="pred"):
            features, labels = batch
            output = (model(features) + 1) / 2
            output2 = (model2(features) + 1) / 2

        print((output - output2).abs().sum())

        print(output.min(), output.max())

        x = np.arange(0, 1, 0.01)
        f_l = []
        p_l = []
        r_l = []
        for i in tqdm(x):
            pred = (output > i).float()

            f1, p, r, _ = f1_score(pred, labels)
            print(i, f1)
            f_l.append(f1)
            p_l.append(p)
            r_l.append(r)

        plt.plot(x, f_l)
        plt.plot(x, p_l)
        plt.plot(x, r_l)
        plt.show()
