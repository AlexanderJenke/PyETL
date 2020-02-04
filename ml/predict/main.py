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
from dataset import NewestOMOP

if __name__ == '__main__':

    model_path = sys.argv[1]

    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    dss = NewestOMOP("../output/dataset_pos_f5/disease_lut.pkl")

    model = m.SVM(len(dss[0][1]))
    model.load(model_path, device=device)
    model.to(device)

    ids_d = {dss.disease_lut[i]['cid']: dss.disease_lut[i]['name'] for i in dss.disease_lut}
    name_lut = [ids_d[i] for i in sorted(ids_d, key=lambda x: x)]

    for param in model.parameters():
        param.requires_grad = False

    for patient in dss:
        pid, features = patient
        features = features.to(device)
        features.requires_grad = True

        output = model(features.view(1, -1))
        output.backward(torch.tensor([[1.0, -1.0]]))

        importance = features.grad.cpu().numpy() * (features != 0).float().numpy()

        importance_d = {i: v for i, v in enumerate(importance)}

        top_ten = sorted(importance_d, key=lambda x: importance_d[x], reverse=False)[:10]  # abs()
        if output[0, 1].item() > 0.2072:
            for i in top_ten:
                print(pid, output[0, 1].item(), features[i].item(), importance_d[i], name_lut[i], i)

    """
    importance = {}
    for i, v in enumerate(feature_importance.mean(axis=0)):
        importance[i] = v

    print(sum(importance.values()))

    a = feature_importance.mean(axis=0)
    np.save("a.npy", a)

    for id in sorted(importance, key=lambda x: abs(importance[x]), reverse=True):
        # if importance[id] < 0:
        print(importance[id], features_lut[id])
    """
