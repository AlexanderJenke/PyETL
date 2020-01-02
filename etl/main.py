import pandas as pd
from sys import argv
import os
from Classes import Person
from Database import OMOP

if __name__ == "__main__":
    csv_dir = argv[1]  # Path to csv files

    # load csv files
    fall_csv = os.path.join(csv_dir, "FALL.csv")
    icd_csv = os.path.join(csv_dir, "ICD.csv")
    messungen_csv = os.path.join(csv_dir, "MESSUNGEN.csv")
    labor_csv = os.path.join(csv_dir, "LABOR.csv")
    ops_csv = os.path.join(csv_dir, "OPS.csv")

    fall_pd = pd.read_csv(fall_csv, delimiter=";")
    icd_pd = pd.read_csv(icd_csv, delimiter=";")
    messungen_pd = pd.read_csv(messungen_csv, delimiter=";")
    labor_pd = pd.read_csv(labor_csv, delimiter=";")
    ops_pd = pd.read_csv(ops_csv, delimiter=";")

    # init database connector
    omop = OMOP()

    # add one patient after another
    for id in fall_pd["patienten_nummer"].unique():

        #  FALL.csv
        fall_df = fall_pd[fall_pd.patienten_nummer.isin([id])]
        fall_df = fall_df.sort_values(by=["aufnahmedatum"])
        last_record = fall_df.iloc[-1]

        location = {"city": last_record.wohnort, "zip": last_record.plz}
        person = Person(person_id=str(id),
                        gender_concept_id=omop.GENDER_LUT[last_record["geschlecht"]],
                        year_of_birth=str(last_record["geburtsjahr"]),
                        month_of_birth=str(last_record["geburtsmonat"]),
                        race_concept_id="8552",  # Unknown
                        ethnicity_concept_id="38003564",  # Non-Hispanic
                        location=location,
                        )
