import torch
from torch.optim import SGD, AdamW, Adam
from torch.optim.lr_scheduler import OneCycleLR
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader
import tensorboardX
import numpy as np
import sys
import os
import datetime
import math

from dataset import PreparedOMOP

sys.path.append(os.pardir)
import model as m

N_EPOCHS = 5000
MAX_LR = 1e-5

epsilon = 1e-16

name = f"PosData_FC_FC100Dropout_Adam_LR{MAX_LR:.0e}_EP{N_EPOCHS}"
name += f"__{datetime.datetime.today().strftime('%d%m%Y_%H%M%S')}"


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


class wMSELoss:
    def __init__(self, weight):
        self.weight = weight

    def __call__(self, pred: torch.tensor, target: torch.tensor):
        p_w = target * self.weight
        n_w = (target + 1) % 2 * (1 - self.weight)
        w = p_w + n_w
        lps = (pred - target).pow(2) * w
        return lps.mean()


def wl1(input, target, w=0.998):
    input = input.view(-1)
    target = target.view(-1)
    return ((input - target).abs() * (target * w - (target - 1) * (1 - w))).mean()


def wmse(input, target, w=0.998, p=2):
    input = input.view(-1)
    target = target.view(-1)
    return ((input - target).pow(p) * (target * w - (target - 1) * (1 - w))).mean()


if __name__ == '__main__':
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    dss = PreparedOMOP("../output/dataset_pos/")
    trainset, testset = dss.get_datasets()
    trainloader = DataLoader(trainset, batch_size=len(trainset), shuffle=True, pin_memory=True,
                             num_workers=8)
    testloader = DataLoader(testset, batch_size=len(testset), shuffle=True, pin_memory=True, num_workers=0)

    i = len(dss.disease_lut)
    print(f"N_features {i}")
    model = m.SVM(i).to(device)
    # model = m.FC_N_FC(i, i).to(device)
    # model = m.FC_FC(i, 100).to(device)

    # optim = SGD(model.parameters(), lr=MAX_LR, momentum=0.9, weight_decay=1e-7)
    optim = Adam(model.parameters(), lr=MAX_LR)
    # lr_sched = OneCycleLR(optim, max_lr=MAX_LR, epochs=N_EPOCHS, steps_per_epoch=len(trainloader))

    w = trainset.n_pos() / len(trainset)
    print(f"w: {w}")
    loss_CEL = nn.CrossEntropyLoss(weight=torch.tensor([w, 1 - w]))
    # loss_wMSE = wMSELoss(weight=w)
    # loss_MSE = nn.MSELoss()
    # loss_BCE = nn.BCELoss()

    log = tensorboardX.SummaryWriter(f"runs/{name}")

    for epoch in range(N_EPOCHS):
        print(epoch)
        mean_loss = []
        preds_cat = torch.empty(0)
        labels_cat = torch.empty(0)
        out_cat = torch.empty(0)
        for batch in trainloader:
            features, labels_cpu = batch
            features = features.to(device)
            labels = labels_cpu.to(device)

            output = model(features)
            output = (output + 1) / 2
            # output = F.softmax(output, dim=1)

            preds_cat = torch.cat([preds_cat, output.detach().cpu().argmax(dim=1).float().view(-1)])
            labels_cat = torch.cat([labels_cat, labels_cpu.float().view(-1)])
            out_cat = torch.cat([out_cat, output[:, 1].detach().cpu().view(-1)])

            if torch.isnan(output).int().sum():
                print(list(model.parameters()))

            loss = 0.0
            loss += loss_CEL(output, labels.view(-1).long())
            # loss += wl1(output[:, 1], labels, w=1.0-w) * 0.5
            # loss += loss_wMSE(output, labels)
            # loss += loss_BCE(output, labels)
            # loss += loss_MSE(output, labels)

            mean_loss.append(loss.item())
            loss.backward()
            optim.step()
            # lr_sched.step()

        mean_f = -(out_cat * (labels_cat - 1)).mean()
        mean_t = (out_cat * labels_cat).mean()
        f1_t, p_t, r_t, df1_t = f1_score(preds_cat, labels_cat)
        log.add_scalars("mean in pred", {"train_t": mean_t, "train_f": mean_f}, global_step=epoch)

        print(f"Train F1 {f1_t:.5f}, P {p_t:.5f}, R {r_t:.5f}, dF1 {df1_t:.5f}, "
              f"N_pos {preds_cat.sum().item():5.0f}, "
              f"L1 {(preds_cat-labels_cat).abs().mean()}, loss {np.mean(mean_loss)}")

        preds_cat = torch.empty(0)
        labels_cat = torch.empty(0)
        out_cat = torch.empty(0)
        for batch in testloader:
            features, labels_cpu = batch
            features = features.to(device)
            labels = labels_cpu.to(device)

            output = model(features)
            output = (output + 1) / 2
            # output = F.softmax(output, dim=1)

            preds_cat = torch.cat([preds_cat, output.detach().cpu().argmax(dim=1).float().view(-1)])
            labels_cat = torch.cat([labels_cat, labels_cpu.float().view(-1)])
            out_cat = torch.cat([out_cat, output[:, 1].detach().cpu().view(-1)])

        mean_f = -(out_cat * (labels_cat - 1)).mean()
        mean_t = (out_cat * labels_cat).mean()
        f1, p, r, df1 = f1_score(preds_cat, labels_cat)

        print(f"Test F1 {f1:.5f}, P {p:.5f}, R {r:.5f}, dF1 {df1:.5f}, "
              f"N_pos {preds_cat.sum().item():5.0f}, "
              f"L1 {(preds_cat-labels_cat).abs().mean()}")

        log.add_scalars("mean in pred", {"test_t": mean_t, "test_f": mean_f}, global_step=epoch)
        log.add_scalar("Loss", np.mean(mean_loss), global_step=epoch)
        # log.add_scalar("LR", lr_sched.get_lr(), global_step=epoch)
        log.add_scalars("Precission", {"test": p, "train": p_t}, global_step=epoch)
        log.add_scalars("Recall", {"test": r, "train": r_t}, global_step=epoch)
        log.add_scalars("F1-Score", {"test": f1, "train": f1_t}, global_step=epoch)
        log.add_scalars("Double F1-Score", {"test": df1, "train": df1_t}, global_step=epoch)

    model.save(f"../output/models/{name}.pt")
    log.close()
