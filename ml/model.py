import torch
import torch.nn as nn
import torch.nn.functional as F


class Net(nn.Module):
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
        x = torch.sigmoid(self.fc6(self.fc5(x)))
        return x

    def save(self, path):
        torch.save(self.state_dict(), path)

    def load(self, path):
        self.load_state_dict(torch.load(path))
