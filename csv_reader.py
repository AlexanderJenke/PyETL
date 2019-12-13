import csv


class InEK_File:
    def __init__(self, csv_path):
        with open(csv_path, 'r') as f:
            data = list(csv.reader(f, delimiter=";"))
        self.colums = data[0]
        self.rows = data[1:]

    def get_colums(self, list_of_colums: list):
        return [{colum: row[self.colums.index(colum)] for colum in list_of_colums} for row in self.rows]


if __name__ == '__main__':
    fall = InEK_File("Data/FALL.csv")
    print(fall.colums)
    for row in fall.rows:
        print(row)

    for row in fall.get_colums(['ik', 'geburtsjahr', 'geschlecht', 'aufnahmedatum', 'aufnahmeanlass', 'aufnahmegrund']):
        print(row)
