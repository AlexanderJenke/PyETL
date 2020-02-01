import torch
from torch.optim import SGD, Adam
from torch.optim.lr_scheduler import OneCycleLR
from torch.nn import MSELoss
from torch.utils.data import DataLoader
from tqdm import tqdm
import tensorboardX
import numpy as np

from dataset import PreparedOMOP
import sys, os
sys.path.append(os.pardir)
import model

N_EPOCHS = 1000
MAX_LR = 1e-6
BATCH_SIZE = 100

epsilon = 1e-16

name = f"SVM_LR{MAX_LR:.0e}_BS{BATCH_SIZE}_EP{N_EPOCHS}"


def f1_score(input: torch.tensor, target: torch.tensor):
    pred = torch.sigmoid((input - 0.5) * 10)
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


if __name__ == '__main__':
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    dss = PreparedOMOP("../output/dataset/")
    trainset, testset = dss.get_datasets()
    trainloader = DataLoader(trainset, batch_size=BATCH_SIZE, shuffle=True, pin_memory=True, num_workers=8)
    testloader = DataLoader(testset, batch_size=len(testset), shuffle=True, pin_memory=True, num_workers=8)

    i = len(dss.disease_lut)

    model = model.SVM(i).to(device)

    optim = SGD(model.parameters(), lr=MAX_LR, momentum=0.9, weight_decay=1e-7)
    lr_sched = OneCycleLR(optim, max_lr=MAX_LR, epochs=N_EPOCHS, steps_per_epoch=len(trainloader))
    loss_wMSE = wMSELoss(weight=0.98)
    loss_MSE = MSELoss()

    log = tensorboardX.SummaryWriter(f"runs/{name}")

    for epoch in range(N_EPOCHS):
        mean_loss = []
        preds_cat = torch.empty((0, 1))
        labels_cat = torch.empty((0, 1))
        for batch in tqdm(trainloader, desc=f"Train ep{str(epoch).zfill(3)}"):
            features, labels_cpu = batch
            features = features.to(device)
            labels = labels_cpu.to(device)

            output = (model(features) + 1) / 2
            preds_cat = torch.cat([preds_cat, output.detach().cpu()])
            labels_cat = torch.cat([labels_cat, labels_cpu])

            f1, p, r, df1 = f1_score(output, labels)
            wmse = loss_wMSE(output, labels)
            mse = loss_MSE(output, labels)

            loss = (torch.tensor(1) - f1) + wmse + (torch.tensor(1) - r) * 0.01

            mean_loss.append(loss.item())
            loss.backward()
            optim.step()
            lr_sched.step()

        f1, p, r, df1 = f1_score(preds_cat, labels_cat)

        log.add_scalar("Train Precission", p, global_step=epoch)
        log.add_scalar("Train Recall", r, global_step=epoch)
        log.add_scalar("Train F1-Score", f1, global_step=epoch)
        log.add_scalar("Train Double F1-Score", df1, global_step=epoch)
        print(f"Train F1-Score: {f1:.5f}, {p:.5f}, {r:.5f}, {(preds_cat> 0.5).float().sum().item():.0f} {df1:.5f}")


        mean_loss = []
        preds_cat = torch.empty((0, 1))
        labels_cat = torch.empty((0, 1))
        for batch in tqdm(testloader, desc=f"Test ep{str(epoch).zfill(3)}"):
            features, labels_cpu = batch
            features = features.to(device)
            labels = labels_cpu.to(device)

            output = (model(features) + 1) / 2
            preds_cat = torch.cat([preds_cat, output.detach().cpu()])
            labels_cat = torch.cat([labels_cat, labels_cpu])

            f1, p, r, df1 = f1_score(output, labels)
            wmse = loss_wMSE(output, labels)
            mse = loss_MSE(output, labels)

            loss = (torch.tensor(1) - f1) + wmse + (torch.tensor(1) - r) * 0.01

            mean_loss.append(loss.item())
            loss.backward()
            optim.step()
            lr_sched.step()

        f1, p, r, df1 = f1_score(preds_cat, labels_cat)

        log.add_scalar("Test Precission", p, global_step=epoch)
        log.add_scalar("Test Recall", r, global_step=epoch)
        log.add_scalar("Test F1-Score", f1, global_step=epoch)
        log.add_scalar("Test Double F1-Score", df1, global_step=epoch)
        print(f"Test F1-Score: {f1:.5f}, {p:.5f}, {r:.5f}, {(preds_cat> 0.5).float().sum().item():.0f} {df1:.5f}")

        log.add_scalar("Loss", np.mean(mean_loss), global_step=epoch)
        log.add_scalar("LR", lr_sched.get_lr(), global_step=epoch)

    model.save(f"{name}.pt")
    log.close()
