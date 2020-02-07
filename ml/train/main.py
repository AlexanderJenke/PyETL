import torch
from torch.optim import AdamW
from torch.optim.lr_scheduler import OneCycleLR, StepLR
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader
import tensorboardX
import numpy as np
import sys
import os
import datetime

from dataset import PreparedOMOP

sys.path.append(os.pardir)
import model as m

N_EPOCHS = 20000
MAX_LR = 1e-4
BS = 0.1
p = 0.001
fw = 1.0

epsilon = 1e-16

name = f"PosData-F5_Dropout{p}SVM_AdamW_LR{MAX_LR:.0e}_Sched5k_EP{N_EPOCHS}_BS*{BS}_W*{fw}"
name += f"__{datetime.datetime.today().strftime('%d%m%Y_%H%M%S')}"


def f1_score(input: torch.tensor, target: torch.tensor):
    """ calculate F1-Score on input and target """
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


if __name__ == '__main__':
    """ train a nn to predict decubitus """

    # define device to calculate on [cpu, cuda]
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    # load datasets
    dss = PreparedOMOP("../output/dataset_pos_f5/")
    trainset, testset = dss.get_datasets()
    trainloader = DataLoader(trainset, batch_size=int(len(trainset) * BS), shuffle=True, pin_memory=True,
                             num_workers=8)
    testloader = DataLoader(testset, batch_size=len(testset), shuffle=True, pin_memory=True, num_workers=0)

    # init model
    i = len(dss.disease_lut)
    print(f"N_features {i}")
    model = m.SVM(i).to(device)

    # init optimizer and learn rate scheduler
    optim = AdamW(model.parameters(), lr=MAX_LR)
    lr_sched = StepLR(optim, step_size=5000, gamma=0.1)

    # calculate positive to negative label ratio
    w = trainset.n_pos() / len(trainset)
    w *= fw
    print(f"w: {w}")

    # init CrossEntropyLoss with the calculated ratio
    loss_CEL = nn.CrossEntropyLoss(weight=torch.tensor([w, 1-w]))

    # initialize tensorboardX log to visualize the training
    log = tensorboardX.SummaryWriter(f"runs/{name}")

    # train multiple epochs
    for epoch in range(N_EPOCHS):
        print(epoch)

        # TRAINING
        # prepare variables to collect data
        mean_loss = []
        preds_cat = torch.empty(0)
        labels_cat = torch.empty(0)
        out_cat = torch.empty(0)

        # iterate over all train samples (multiple samples are bundled into batches)
        for batch in trainloader:
            # prepare batch data
            features, labels_cpu = batch
            features = features.to(device)
            labels = labels_cpu.to(device)

            # calculate prediction using the model
            features = F.dropout(features, p=p)
            output = model(features)
            output = (output + 1) / 2

            # store data for later scoring (f1 score, number of declared risks ...)
            preds_cat = torch.cat([preds_cat, output.detach().cpu().argmax(dim=1).float().view(-1)])
            labels_cat = torch.cat([labels_cat, labels_cpu.float().view(-1)])
            out_cat = torch.cat([out_cat, output[:, 1].detach().cpu().view(-1)])

            # print debuging output if model parameters crash into nan (caused by over-/underflowing floats)
            if torch.isnan(output).int().sum():
                print(list(model.parameters()))

            # calculate loss
            loss = 0.0
            loss += loss_CEL(output, labels.view(-1).long())

            # save loss to later log the loss value
            mean_loss.append(loss.item())

            # backpropagate loss and make a step to improve the model
            loss.backward()
            optim.step()

        # trigger a step in the learn rate scheduler to adapt the learnrate
        lr_sched.step()

        # calculate and log scores
        mean_f = -(out_cat * (labels_cat - 1)).mean()
        mean_t = (out_cat * labels_cat).mean()
        f1_t, p_t, r_t, df1_t = f1_score(preds_cat, labels_cat)
        log.add_scalars("mean in pred", {"train_t": mean_t, "train_f": mean_f}, global_step=epoch)

        print(f"Train F1 {f1_t:.5f}, P {p_t:.5f}, R {r_t:.5f}, dF1 {df1_t:.5f}, "
              f"N_pos {preds_cat.sum().item():5.0f}, "
              f"L1 {(preds_cat-labels_cat).abs().mean()}, loss {np.mean(mean_loss)}")

        # EVALUATION
        # prepare variables to collect data
        preds_cat = torch.empty(0)
        labels_cat = torch.empty(0)
        out_cat = torch.empty(0)

        # iterate over all test samples (all samples are bundled into on batch)
        for batch in testloader:
            # prepare batch data
            features, labels_cpu = batch
            features = features.to(device)
            labels = labels_cpu.to(device)

            # calculate prediction using the model
            output = model(features)
            output = (output + 1) / 2

            # store data for later scoring (f1 score, number of declared risks ...)
            preds_cat = torch.cat([preds_cat, output.detach().cpu().argmax(dim=1).float().view(-1)])
            labels_cat = torch.cat([labels_cat, labels_cpu.float().view(-1)])
            out_cat = torch.cat([out_cat, output[:, 1].detach().cpu().view(-1)])

        # calculate and log scores
        mean_f = -(out_cat * (labels_cat - 1)).mean()
        mean_t = (out_cat * labels_cat).mean()
        f1, p, r, df1 = f1_score(preds_cat, labels_cat)

        print(f"Test F1 {f1:.5f}, P {p:.5f}, R {r:.5f}, dF1 {df1:.5f}, "
              f"N_pos {preds_cat.sum().item():5.0f}, "
              f"L1 {(preds_cat-labels_cat).abs().mean()}")

        log.add_scalars("mean in pred", {"test_t": mean_t, "test_f": mean_f}, global_step=epoch)
        log.add_scalar("Loss", np.mean(mean_loss), global_step=epoch)
        log.add_scalar("LR", lr_sched.get_lr(), global_step=epoch)
        log.add_scalars("Precission", {"test": p, "train": p_t}, global_step=epoch)
        log.add_scalars("Recall", {"test": r, "train": r_t}, global_step=epoch)
        log.add_scalars("F1-Score", {"test": f1, "train": f1_t}, global_step=epoch)
        log.add_scalars("Double F1-Score", {"test": df1, "train": df1_t}, global_step=epoch)

    # save trained model
    model.save(f"../output/models/{name}.pt")
    log.close()
