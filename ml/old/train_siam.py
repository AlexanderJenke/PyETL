import torch
from torch.optim import SGD, Adam
from torch.optim.lr_scheduler import OneCycleLR
from torch.nn import MSELoss
from torch.utils.data import DataLoader
from tqdm import tqdm
import tensorboardX
import numpy as np
import math

from dataset import OMOP_Base
from model import *

N_EPOCHS = 100
MAX_LR = 1e-3
BATCH_SIZE = 100  # 0

epsilon = 1e-16


def f1loss(input: torch.tensor, target: torch.tensor):
    return torch.tensor(1) - f1_score(input, target)[0]


def f1_score(input: torch.tensor, target: torch.tensor):
    pred = input  # .round()
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

    trainset = OMOP_Base(None, path="synpuf_cdm.samples.pkl")
    trainloader = DataLoader(trainset, batch_size=BATCH_SIZE, shuffle=True, pin_memory=True, num_workers=8)

    i = len(trainset[0][0])

    # model = N_FC(input_size=i,
    #              layers=[[200],
    #                      [100, 1],
    #                      # [i // 2, i // 4],
    #                      # [i // 8, i // 16],
    #                      # [i // 32, 1],
    #                      ]
    #              ).to(device)
    # model = Net_small(len(trainset[0][0])).to(device)
    # model = Net(len(trainset[0][0])).to(device)
    # model = SVM(len(trainset[0][0])).to(device)

    model_feat = N_FC(input_size=i,
                      layers=[[i // 2, 200]]).to(device)

    model_cls = N_FC(input_size=200,
                     layers=[[100], [1]]).to(device)

    optim_feat = Adam(model_feat.parameters(), lr=MAX_LR)
    lr_sched_feat = OneCycleLR(optim_feat, max_lr=MAX_LR, epochs=N_EPOCHS, steps_per_epoch=len(trainloader))

    optim_cls = Adam(model_cls.parameters(), lr=MAX_LR)
    lr_sched_cls = OneCycleLR(optim_cls, max_lr=MAX_LR, epochs=N_EPOCHS, steps_per_epoch=math.ceil(len(trainloader)/10))

    loss_f1 = f1loss
    loss_f1_score = f1_score
    loss_wMSE = wMSELoss(weight=0.98)
    loss_MSE = MSELoss()

    log = tensorboardX.SummaryWriter()

    fp, tp, fn = 0, 0, 0

    for epoch in range(N_EPOCHS):
        mean_loss = []
        preds_cat = torch.empty((0, 1))
        labels_cat = torch.empty((0, 1))
        bps_c = 0
        loss_bps = 0
        for batch in tqdm(trainloader, desc=f"Train ep{str(epoch).zfill(3)}"):
            features, labels_cpu = batch
            features = features.to(device)
            labels = labels_cpu.to(device)

            output_feat = model_feat(features)
            output = (model_cls(output_feat) + 1) / 2

            preds_cat = torch.cat([preds_cat, output.detach().cpu()])
            labels_cat = torch.cat([labels_cat, labels_cpu])
            # feat loss
            loss_feat = torch.tensor(0).float()

            for i in range(len(output_feat)):
                for j in range(len(output_feat)):
                    if labels_cpu[i] == labels_cpu[j]:
                        loss_feat += loss_MSE(output_feat[i], output_feat[j])
                    else:
                        loss_feat -= loss_MSE(output_feat[i], output_feat[j])

            loss_feat.backward(retain_graph=True)
            optim_feat.step()
            lr_sched_feat.step()

            # cls loss
            bps_c += 1
            f1, p, r, df1 = loss_f1_score(output, labels)
            wmse = loss_wMSE(output, labels)
            loss_cls = (torch.tensor(1) - p) + (torch.tensor(1) - r) / 100 + wmse
            loss_bps += loss_cls
            mean_loss.append(loss_cls.item())

            if not bps_c % 10:
                loss_bps.backward()
                optim_cls.step()
                loss_bps = 0
                lr_sched_cls.step()

        # do step on remaining batches
        if bps_c % 10:
            loss_bps.backward()
            optim_cls.step()

        f1, p, r, df1 = f1_score(preds_cat, labels_cat)

        log.add_scalar("Precission", p, global_step=epoch)
        log.add_scalar("Recall", r, global_step=epoch)
        log.add_scalar("F1-Score", f1, global_step=epoch)
        log.add_scalar("Double F1-Score", df1, global_step=epoch)

        print(f"F1-Score: {f1:.5f}, {p:.5f}, {r:.5f}, {preds_cat.sum().item():.0f} {df1:.5f}")

        log.add_scalar("Loss", np.mean(mean_loss), global_step=epoch)
        log.add_scalar("LR", lr_sched_cls.get_lr(), global_step=epoch)

    model_feat.save("siam_feat.pt")
    model_cls.save("siam_cls.pt")
    log.close()
