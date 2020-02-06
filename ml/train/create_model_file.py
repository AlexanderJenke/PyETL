import sys
import pickle
import torch

if __name__ == '__main__':
    name, state_dict_file, threshold, disease_lut_file = sys.argv[1:5]

    state_dict = torch.load(state_dict_file)

    with open(disease_lut_file, "rb") as file:
        disease_lut = pickle.load(file)

    d = {"state_dict": state_dict,
         "disease_lut": disease_lut,
         "threshold": float(threshold)}
    with open(f"../output/{name}.pkl", "wb") as file:
        pickle.dump(d, file)
