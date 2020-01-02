import pandas as pd
from sys import argv
import os
from Classes import Person
from Database import OMOP

if __name__ == "__main__":
    csv_dir = argv[1]

    fall_csv = os.path.join(csv_dir, "FALL.csv")
    icd_csv = os.path.join(csv_dir, "ICD.csv")
    messungen_csv = os.path.join(csv_dir, "MESSUNGEN.csv")
    labor_csv = os.path.join(csv_dir, "LABOR.csv")
    ops_csv = os.path.join(csv_dir, "OPS.csv")

    fall_pd = pd.read_csv(fall_csv, delimiter=";")
    icd_pd = pd.read_csv(icd_csv, delimiter=";")
    messungen_csv = pd.read_csv(messungen_csv, delimiter=";")
    labor_csv = pd.read_csv(labor_csv, delimiter=";")
    ops_csv = pd.read_csv(ops_csv, delimiter=";")

    omop = OMOP()

    for id in fall_pd["patienten_nummer"].unique():
        df = fall_pd[fall_pd.patienten_nummer.isin([id])]
        df = df.sort_values(by=["aufnahmedatum"])
        last_record = df.iloc[-1]

        location = {}
        location["city"] = df.wohnort
        location["zip"] = df.plz

        person = Person(person_id=id,
                        gender_concept_id=omop.GENDER_LUT[last_record["geschlecht"]],
                        year_of_birth=last_record["geburtsjahr"],
                        race_concept_id="8552",  # Unknown
                        ethnicity_concept_id="38003564",  # Non-Hispanic
                        location_id=location,
                        )
