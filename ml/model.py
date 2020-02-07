import torch
import torch.nn as nn
import torch.nn.functional as F


class SaveLoad(nn.Module):
    """class implementing save and load functions for other models to inherit """
    def save(self, path):
        """saves the models state_dict
        :param path: path to generated file
        """
        torch.save(self.state_dict(), path)

    def load(self, path, device='cpu'):
        """ loads a state dict into a model
        :param path: file containing state dict
        :param device: device the model is on
        """
        # resize model according to file
        state_dict = torch.load(path, map_location=device)
        for key in state_dict:
            # automatically resize the models layer according to the state dict
            if key.endswith("weight"):
                self.__getattr__(key.split('.')[0]).__init__(state_dict[key].shape[1], state_dict[key].shape[0])

        # load params into the model
        self.load_state_dict(state_dict)


class SVM(SaveLoad):
    """ SVM for decubitus risk prediction
    inherits save and load funktionality from SaveLoad class
    """
    def __init__(self, input_size):
        """ initialize the model"""
        super(SVM, self).__init__()
        self.fc = nn.Linear(input_size, 2)

    def forward(self, x):
        """propagate data through the nn"""
        return F.softsign(self.fc(x))
