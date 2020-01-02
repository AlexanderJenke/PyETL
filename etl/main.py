import pandas as pd
from sys import argv
import os

GENDER_D = {"m" : "8507", "w" : "8532", "nan" : "8551"}


if __name__ == "__main__":
    csv_dir = argv[1]
    
    fall_csv = os.path.join(csv_dir, "FALL.csv")
    icd_csv = os.path.join(csv_dir,  "ICD.csv")
    messungen_csv = os.path.join(csv_dir, "MESSUNGEN.csv")
    labor_csv = os.path.join(csv_dir, "LABOR.csv")
    ops_csv = os.path.join(csv_dir, "OPS.csv")

    fall_pd = pd.read_csv(fall_csv, delimiter=";")
    icd_pd = pd.read_csv(icd_csv, delimiter=";")
    messungen_csv = pd.read_csv(messungen_csv, delimiter=";")
    labor_csv = pd.read_csv(labor_csv, delimiter=";")
    ops_csv = pd.read_csv(ops_csv, delimiter=";")

    person_d = {}
    patient_ids = fall_pd["patienten_nummer"].unique()
    print(patient_ids)
    for id in patient_ids:
        df = fall_pd[fall_pd.patienten_nummer.isin([id])]
        df = df.sort_values(by=["aufnahmedatum"])
        print(GENDER_D[str(df.iloc[0]["geschlecht"])])
        location = {}
        location["city"] = df.wohnort 
        location["zip"] = df.plz
        last_record = df.iloc[-1]
        person = Person(person_id=id, 
                gender_concept_id=GENDER_ID[last_record["geschlecht"]], 
                year_of_birth=last_record["geburtsjahr"])
        

        
