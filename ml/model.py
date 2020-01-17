import torch
import torch.nn as nn
import torch.nn.functional as F


class SaveLoad(nn.Module):
    def save(self, path):
        torch.save(self.state_dict(), path)

    def load(self, path, device='cpu'):
        # resize model according to file
        state_dict = torch.load(path, map_location=device)
        for key in state_dict:
            if key.endswith("weight"):
                self.__getattr__(key.split('.')[0]).__init__(state_dict[key].shape[1], state_dict[key].shape[0])
                print(key.split('.')[0], state_dict[key].shape)

        # load params
        self.load_state_dict(state_dict)


class SVM(SaveLoad):
    def __init__(self, input_size):
        super(SVM, self).__init__()
        self.fc = nn.Linear(input_size, 1)

    def forward(self, x):
        return self.fc(x)


class Net(SaveLoad):
    def __init__(self, input_size):
        super(Net, self).__init__()

        self.fc1 = nn.Linear(input_size, input_size * 2)
        self.fc2 = nn.Linear(input_size * 2, input_size * 2)

        self.fc3 = nn.Linear(input_size * 2, input_size * 2)
        self.fc4 = nn.Linear(input_size * 2, input_size * 2)

        self.dropout = nn.Dropout()

        self.fc5 = nn.Linear(input_size * 2, input_size)
        self.fc6 = nn.Linear(input_size, 1)

    def forward(self, x):
        x = F.leaky_relu(self.fc2(self.fc1(x)))
        x = F.leaky_relu(self.fc4(self.fc3(x)))
        x = self.dropout(x)
        x = F.leaky_relu(self.fc6(self.fc5(x)))
        return x
