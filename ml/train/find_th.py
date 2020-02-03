import torch
from torch.utils.data import DataLoader
import numpy as np
import sys
import os
import matplotlib.pyplot as plt
from tqdm import tqdm

from dataset import PreparedOMOP

sys.path.append(os.pardir)
import model as m

epsilon = 1e-16


def f1_score(input: torch.tensor, target: torch.tensor):
    pred = input  # F.hardtanh((input - 0.5).pow(-1), min_val=0, max_val=1)
    if not (pred.min() >= 0 and pred.max() <= 1):
        print(pred.min(), pred.max(), input)
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


def main(path):
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    dss = PreparedOMOP("../output/dataset_pos/")
    trainset, testset = dss.get_datasets()
    testloader = DataLoader(testset, batch_size=len(testset), shuffle=True, pin_memory=True, num_workers=0)

    l = torch.tensor([i[1] for i in trainset])
    f_l = []
    for _ in range(1000):
        z = torch.rand(len(l)).round()
        f1, _, _, _ = f1_score(z, l)
        f_l.append(f1)

    i = len(dss.disease_lut)
    print(f"N_features {i}")
    #model = m.FC_N_FC(i, 870).to(device)
    model = m.SVM(i).to(device)

    model.load(path)

    preds_cat = torch.empty(0)
    labels_cat = torch.empty(0)
    out_cat = torch.empty(0)
    for batch in testloader:
        features, labels_cpu = batch
        features = features.to(device)

        output = model(features)
        output = (output + 1) / 2

        preds_cat = torch.cat([preds_cat, output.detach().cpu().argmax(dim=1).float().view(-1)])
        labels_cat = torch.cat([labels_cat, labels_cpu.float().view(-1)])
        out_cat = torch.cat([out_cat, output[:, 1].detach().cpu().view(-1)])

    mean_f = -(out_cat * (labels_cat - 1)).mean()
    mean_t = (out_cat * labels_cat).mean()
    var_f = (out_cat * (labels_cat - 1) * -1).var()
    var_t = (out_cat * labels_cat).var()
    th = (mean_f + mean_t) / 2
    print(f"{mean_f.item()}+-{var_f.item()}, {mean_t.item()}+-{var_t.item()}, {th.item()}")

    f1_l = []
    p_l = []
    r_l = []
    x = np.arange(0, 1, 0.0001)
    base = np.ones(len(x)) * np.mean(f_l)
    for i in tqdm(x):
        f1, p, r, df1 = f1_score((out_cat > i).float(), labels_cat)
        f1_l.append(f1)
        p_l.append(p)
        r_l.append(r)

    plt.plot(x, f1_l, label='F1')
    plt.plot(x, p_l, label='P')
    plt.plot(x, r_l, label='R')
    plt.plot(x, base, label="Random Baseline")
    plt.title(path.split('/')[-1] + f"\nmax F1: {max(f1_l)}")
    plt.legend()
    plt.show()


if __name__ == '__main__':
    # for file in os.listdir("../output/models"):
    #     if file.startswith("PosData_MFC870"):
    #         main(os.path.join("../output/models", file))
    main("../output/models/PosData_SVMDropout_AdamW_LR1e-05_EP5000__03022020_004037.pt")
