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
    omop = OMOP(do_commits=False)  # TODO Activate commits

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

        internal_ids = [key for key in fall_df["kh_internes_kennzeichen"]]  # associated internal ids

        # LABOR.csv
        labor_df = labor_pd[labor_pd.kh_internes_kennzeichen.isin(internal_ids)]

        for i, row in labor_df.iterrows():

            # no LOINC version given -> expecting only one concept_id -> selecting first one
            concept_id = omop.LOINC_LUT[str(row["LOINC"])]['concept_ids'][0][0]
            domain_id = omop.LOINC_LUT[str(row["LOINC"])]['domain_id']

            if domain_id == "Measurement":

                # adding optional information if given
                optional = {}
                if str(row["low"]) != "nan":
                    optional['range_low'] = str(row["low"])
                if str(row["high"]) != "nan":
                    optional['range_high'] = str(row["high"])

                person.add_measurement(measurement_concept_id=str(concept_id),
                                       measurement_date=str(row["timestamp"][:10]),
                                       measurement_datetime=str(row["timestamp"]),
                                       measurement_type_concept_id=str(concept_id),
                                       value_as_number=str(row["value"]),
                                       unit_source_value=str(row['unit']),
                                       **optional
                                       )

            if domain_id == "Observation":
                # Not Handled
                raise NotImplementedError("'Observation' in the LABOR.csv is not supported!")

            if domain_id == "Meas Value":
                # Not Handled
                raise NotImplementedError("'Meas Value' in the LABOR.csv is not supported!")

        # MESSUNGEN.csv
        messungen_df = messungen_pd[messungen_pd.kh_internes_kennzeichen.isin(internal_ids)]

        for i, row in messungen_df.iterrows():

            # no LOINC version given -> expecting only one concept_id -> selecting first one
            concept_id = omop.LOINC_LUT[str(row["LOINC"])]['concept_ids'][0][0]
            domain_id = omop.LOINC_LUT[str(row["LOINC"])]['domain_id']

            if domain_id == "Measurement":
                person.add_measurement(measurement_concept_id=str(concept_id),
                                       measurement_date=str(row["timestamp"][:10]),
                                       measurement_datetime=str(row["timestamp"]),
                                       measurement_type_concept_id=str(concept_id),
                                       value_as_number=str(row["value"]),
                                       unit_source_value=str(row['unit']),
                                       )

            if domain_id == "Observation":
                # Not Handled
                raise NotImplementedError("Observation in the MESSUNGEN.csv is not supported!")

            if domain_id == "Meas Value":
                # Not Handled
                raise NotImplementedError("'Meas Value' in the MESSUNGEN.csv is not supported!")
