
import torch
from torch.utils.data import DataLoader
from tqdm import tqdm
import tensorboardX
import numpy as np
import pickle

from dataset import OMOP_Base
from model import *

if __name__ == '__main__':
    batch_size = 1  # 000

    with open("synpuf_cdm.db.pkl", 'rb') as file:
        db_data = pickle.load(file)
    patient_data = db_data['data']
    alphabet = db_data['alphabet']
    features_map = sorted(list(set(np.concatenate([list(patient['datapoints_d'].keys())
                                                   for patient in patient_data if len(patient['datapoints_d'])]))))

    features_lut = ["Age", "Is Male", "Is Female"]
    for feature_id in features_map:
        features_lut.append(alphabet[feature_id][0])

    # model_path = sys.argv[1]
    model_path = "svm_57f1.pt"
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    # device = 'cpu'
    print(device)
    ds = OMOP_Base(None, path="synpuf_cdm.samples.pkl")
    dl = DataLoader(ds, batch_size=batch_size, shuffle=False, pin_memory=True, num_workers=8)

    model = SVM(len(ds[0][0]))
    model.load(model_path, device=device)
    model.to(device)

    for param in model.parameters():
        param.requires_grad = False

    feature_importance = np.zeros((len(ds), len(ds[0][0])), dtype=np.float)
    for i, batch in enumerate(tqdm(dl)):
        features, labels = batch

        features = features.to(device)
        labels = labels.to(device)

        features.requires_grad = True

        output = model(features).mean()

        output.data = torch.FloatTensor([-1 if labels.item() else 1]).to(device)
        output.backward()

        feature_importance[i * batch_size: i * batch_size + features.shape[0]] = features.grad.cpu().numpy()

    importance = {}
    for i, v in enumerate(feature_importance.mean(axis=0)):
        importance[i] = v

    for id in sorted(importance, key=lambda x: importance[x], reverse=True):
        print(features_lut[id])
