import torch
from torch.utils.data import DataLoader
from tqdm import tqdm
import tensorboardX
import numpy as np
import pickle

from dataset import OMOP_Samples
from model import *

if __name__ == '__main__':
    batch_size = 1  # 000

    # model_path = sys.argv[1]
    model_path = "minimal_svm_LR1e-3_BS500_L-f1-wmse-r0,02_Ep3000.pt"
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    # device = 'cpu'
    ds = OMOP_Samples("synpuf_cdm.minimal.pkl")

    features_lut = ["Is Male", "Is Female", "Age"] + [ds.alphabet[concept_id][0] for concept_id in
                                                      sorted(ds.features_lut, key=lambda x: ds.features_lut[x])]

    '''
    model = N_FC(input_size=len(ds[0][0]),
                 layers=[[100],
                         [1]])
    # '''

    model = SVM(len(ds[0][0]))
    model.load(model_path, device=device)
    model.to(device)

    for param in model.parameters():
        param.requires_grad = False

    j = 0
    feature_importance = np.zeros((len(ds), len(ds[0][0])), dtype=np.float)
    for i, batch in enumerate(tqdm(ds)):
        features, labels = batch

        if not labels.item():
            continue
            pass

        features = features.to(device)
        labels = labels.to(device)

        features.requires_grad = True

        output = model(features)

        #output.data = torch.FloatTensor([1]).to(device)
        output.backward(torch.tensor([-1.0]))

        feature_importance[j * batch_size:
                           j * batch_size + features.shape[0]] = features.grad.cpu().numpy()
        j += 1

    feature_importance = feature_importance[:j]

    importance = {}
    for i, v in enumerate(feature_importance.mean(axis=0)):
        importance[i] = v



    print(sum(importance.values()))

    a = feature_importance.mean(axis=0)
    np.save("a.npy", a)

    for id in sorted(importance, key=lambda x: abs(importance[x]), reverse=True):
        # if features_lut[id][1] in ['Procedure', 'Conditions']: #, 'Conditions', 'Procedure', 'Measurement']:
        print(importance[id], features_lut[id])
