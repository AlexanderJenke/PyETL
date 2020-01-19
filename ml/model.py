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
        return F.softsign(self.fc(x))


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


class Net_small(SaveLoad):
    def __init__(self, input_size):
        super(Net_small, self).__init__()

        self.fc1 = nn.Linear(input_size, input_size // 2)
        self.fc2 = nn.Linear(input_size // 2, input_size // 4)

        self.fc3 = nn.Linear(input_size // 4, input_size // 8)
        self.fc4 = nn.Linear(input_size // 8, input_size // 16)

        self.dropout = nn.Dropout()

        self.fc5 = nn.Linear(input_size // 16, input_size // 32)
        self.fc6 = nn.Linear(input_size // 32, 1)

    def forward(self, x):
        x = F.leaky_relu(self.fc2(self.fc1(x)))
        x = F.leaky_relu(self.fc4(self.fc3(x)))
        x = self.dropout(x)
        x = F.relu(self.fc6(self.fc5(x)))
        return x


class N_FC(SaveLoad):
    def __init__(self, input_size, layers, activation=F.softsign):
        super(N_FC, self).__init__()
        self.activation = activation
        self.layers = []
        last_size = input_size

        for blocks in layers:
            block = []
            for layer in blocks:
                block.append(nn.Linear(last_size, layer))
                last_size = layer
            self.layers.append(block)

    def parameters(self, recurse=True):

        for blocks in self.layers:
            for layer in blocks:
                for name, param in layer.named_parameters(recurse=recurse):
                    yield param

    def forward(self, x):
        for blocks in self.layers:
            for layer in blocks:
                x = layer(x)
            x = self.activation(x)
        return x
