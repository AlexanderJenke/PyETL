import pandas as pd
from sys import argv
import os
from Classes import Person

GENDER_D = {"m": "8507", "w": "8532", "nan": "8551"}

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

    for id in fall_pd["patienten_nummer"].unique():
        df = fall_pd[fall_pd.patienten_nummer.isin([id])]
        df = df.sort_values(by=["aufnahmedatum"])
        last_record = df.iloc[-1]
        location = {"city": df.wohnort, "zip": df.plz}
        person = Person(person_id=str(id),
                        gender_concept_id=GENDER_D[last_record["geschlecht"]],
                        year_of_birth=str(last_record["geburtsjahr"]),
                        month_of_birth=str(last_record["geburtsmonat"]),
                        race_concept_id="8552",  # Unknown
                        ethnicity_concept_id="38003564",  # Non-Hispanic
                        location=location,
                        )
        measurements = []
        kennzeichen_list = [k for k in df["kh_internes_kennzeichen"]]
        df = labor_csv[labor_csv.kh_internes_kennzeichen.isin(kennzeichen_list)]
        measurements = []
        for i, row in df.iterrows(): 
            measurements.append({"measurement_concept_id" : str(row["LOINC"]), 
                                   "measurement_date" : str(row["timestamp"][:10]), 
                                   "measurement_datetime" : str(row["timestamp"]), 
                                   "measurement_type_concept_id" : str(row["LOINC"]),
                                   "value_as_number" : str(row["value"]),
                                   "range_low" : str(row["low"]),
                                   "range_high" : str(row["high"])})
        person.add_measurements(measurements)
        print(person.insert_into_db())
