import OMOP
import csv_reader

db = OMOP.DB()
data = csv_reader.InEK_File("Data/FALL.csv")

sel_colums = ['patientennummer',
              'patienten_nummer',
              ]

for row in data.get_colums(sel_colums):
    assert(row['patientennummer'] == row['patienten_nummer'])
