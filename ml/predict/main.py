import torch
from torch.utils.data import DataLoader
from tqdm import tqdm
import tensorboardX
import numpy as np
import pickle
import sys
import os

sys.path.append(os.pardir)
import model as m
from dataset import PreparedOMOP


if __name__ == '__main__':
    batch_size = 1  # 000

    # model_path = sys.argv[1]
    model_path = "../output/models/PosData_SVMDropout_Adam_LR1e-05_EP5000__03022020_002945.pt"
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    # device = 'cpu'
    dss = PreparedOMOP("../output/dataset_pos/")
    trainset, ds = dss.get_datasets()
    features_lut = dss.disease_lut

    model = m.SVM(len(ds[0][0]))
    model.load(model_path, device=device)
    model.to(device)

    for param in model.parameters():
        param.requires_grad = False

    j = 0
    feature_importance = np.zeros((len(ds), len(ds[0][0])), dtype=np.float)
    for i, batch in enumerate(tqdm(ds)):
        features, labels = batch

        if not labels.item():
            #continue
            pass

        features = features.to(device)
        labels = labels.to(device)

        features.requires_grad = True

        output = model(features)[1:]

        # output.data = torch.FloatTensor([1]).to(device)
        output.backward(torch.tensor([-((labels[0]-0.5) * 2)]))

        feature_importance[j * batch_size:
                           j * batch_size + features.shape[0]] = features.grad.cpu().numpy() * (features != 0).float().numpy()
        j += 1

    feature_importance = feature_importance[:j]

    importance = {}
    for i, v in enumerate(feature_importance.mean(axis=0)):
        importance[i] = v

    print(sum(importance.values()))

    a = feature_importance.mean(axis=0)
    np.save("a.npy", a)

    for id in sorted(importance, key=lambda x: abs(importance[x]), reverse=True):
        #if importance[id] < 0:
        print(importance[id], features_lut[id])
