import torch
import pickle
import os
import numpy as np


class PreparedOMOP:
    def __init__(self, directory):
        with open(os.path.join(directory, "alphabet.pkl"), 'rb') as f:
            self.alphabet = pickle.load(f)

        test_patients = []
        for file in [f for f in os.listdir(os.path.join(directory, "test")) if f.endswith(".pkl")]:
            with open(os.path.join(directory, "test", file), 'rb') as f:
                test_patients += pickle.load(f)

        train_patients = []
        for file in [f for f in os.listdir(os.path.join(directory, "train")) if f.endswith(".pkl")]:
            with open(os.path.join(directory, "train", file), 'rb') as f:
                train_patients += pickle.load(f)

        ids = sorted(set(
            np.concatenate([[k for k in p[0].keys() if p[0][k] != 0] for p in test_patients + train_patients if p[1]])))

        self.disease_lut = {i: {"cid": j,
                                "vocabulary": self.alphabet[j][2],
                                "domain": self.alphabet[j][1],
                                "name": self.alphabet[j][0]} for i, j in enumerate(ids)}
        with open(os.path.join(directory, "disease_lut.pkl"), 'wb') as file:
            pickle.dump(self.disease_lut, file)

        self.test_data = []
        self.train_data = []

        for feature_d, label in test_patients:
            feature_l = [feature_d[id] if id in feature_d else 0 for i, id in enumerate(ids)]
            self.test_data.append((feature_l, label))

        for feature_d, label in train_patients:
            feature_l = [feature_d[id] if id in feature_d else 0 for i, id in enumerate(ids)]
            self.train_data.append((feature_l, label))

    def get_datasets(self):
        return self.Dataset(self.train_data), self.Dataset(self.test_data)

    class Dataset(torch.utils.data.Dataset):
        def __init__(self, data):
            self.data = data

        def __getitem__(self, item):
            return torch.tensor(self.data[item][0],
                                dtype=torch.float32), torch.tensor([self.data[item][1]],
                                                                   dtype=torch.float32)

        def __len__(self):
            return len(self.data)

        def n_pos(self):
            return sum([1 for d in self.data if d[1]])


if __name__ == '__main__':
    dss = PreparedOMOP("../output/dataset_pos_f5/")

    trainset, testset = dss.get_datasets()

    print(f"trainset: len {len(trainset)} n_pos {trainset.n_pos()}")
    print(f"testset:  len {len(testset)} n_pos {testset.n_pos()}")
    print(f"N Features: {len(dss.disease_lut)}")
