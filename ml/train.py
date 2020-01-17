import sys
if sys.version_info[1] <= 5:
    pass
    # -*- coding: future_fstrings -*-

import torch
from torch.optim import SGD
from torch.optim.lr_scheduler import OneCycleLR
from torch.nn import MSELoss
from torch.utils.data import DataLoader
from tqdm import tqdm
import tensorboardX
import numpy as np

from dataset import OMOP_Base
from model import Net, SVM

N_EPOCHS = 100
MAX_LR = 1e-1
BATCH_SIZE = 100

if __name__ == '__main__':
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    trainset = OMOP_Base(None, path="synpuf_cdm.samples.pkl")
    trainloader = DataLoader(trainset, batch_size=BATCH_SIZE, shuffle=True, pin_memory=True, num_workers=8)

    model = SVM(len(trainset[0][0])).to(device)  # Net

    optim = SGD(model.parameters(), lr=MAX_LR, momentum=0.9)
    lr_sched = OneCycleLR(optim, max_lr=MAX_LR, epochs=N_EPOCHS, steps_per_epoch=len(trainloader))
    loss_fn = MSELoss()

    log = tensorboardX.SummaryWriter()

    for epoch in range(N_EPOCHS):
        mean_loss = []
        for batch in tqdm(trainloader, desc="Train"):
            features, labels = batch
            features = features.to(device)
            labels = labels.to(device)

            output = model(features)

            loss = loss_fn(output, labels)
            mean_loss.append(loss.item())

            loss.backward()
            optim.step()
            lr_sched.step()

        log.add_scalar("Loss", np.mean(mean_loss), global_step=epoch)
        log.add_scalar("LR", lr_sched.get_lr(), global_step=epoch)

    model.save("svm.pt")
