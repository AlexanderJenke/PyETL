import pandas as pd
from sys import argv
import os
from Classes import Person
from Database import OMOP

from tqdm import tqdm

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
    for id in tqdm(fall_pd["patienten_nummer"].unique()):

        #  FALL.csv ----------------------------------------------------------------------------------------------------
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

        for i, row in fall_df.iterrows():
            person.add_visit(visit_occurrence_id=str(row['kh_internes_kennzeichen']),
                             visit_concept_id="0000",  # TODO
                             visit_start_date=str(row["aufnahmedatum"])[:8],
                             visit_end_date=str(row["entlassungsdatum"])[:8],
                             visit_type_concept_id="32023",  # TODO 'Visit derived from encounter on medical facility claim', ok?
                             visit_source_value=str(row['aufnahmeanlass']),
                             admitting_source_value=str(row['aufnahmegrund']),
                             discharge_to_source_value=str(row['entlassungsgrund']),
                             )

        internal_ids = [key for key in fall_df["kh_internes_kennzeichen"]]  # associated internal ids

        # LABOR.csv ----------------------------------------------------------------------------------------------------
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
                                       measurement_type_concept_id="44818702",  # TODO 'Lab Result', ok?
                                       value_as_number=str(row["value"]),
                                       measurement_source_concept_id=str(concept_id),
                                       measurement_source_value=str(row["LOINC"]),
                                       unit_source_value=str(row['unit']),
                                       **optional
                                       )

            if domain_id == "Observation":
                # Not Handled
                raise NotImplementedError("'Observation' in the LABOR.csv is not supported!")

            if domain_id == "Meas Value":
                # Not Handled
                raise NotImplementedError("'Meas Value' in the LABOR.csv is not supported!")

        # MESSUNGEN.csv ------------------------------------------------------------------------------------------------
        messungen_df = messungen_pd[messungen_pd.kh_internes_kennzeichen.isin(internal_ids)]

        for i, row in messungen_df.iterrows():

            # no LOINC version given -> expecting only one concept_id -> selecting first one
            concept_id = omop.LOINC_LUT[str(row["LOINC"])]['concept_ids'][0][0]
            domain_id = omop.LOINC_LUT[str(row["LOINC"])]['domain_id']

            if domain_id == "Measurement":
                person.add_measurement(measurement_concept_id=str(concept_id),
                                       measurement_date=str(row["timestamp"][:10]),
                                       measurement_datetime=str(row["timestamp"]),
                                       measurement_type_concept_id="44818701",  # TODO 'From physical examination', ok?
                                       measurement_source_concept_id=str(concept_id),
                                       measurement_source_value=str(row["LOINC"]),
                                       value_as_number=str(row["value"]),
                                       unit_source_value=str(row['unit']),
                                       )

            if domain_id == "Observation":
                # Not Handled
                raise NotImplementedError("Observation in the MESSUNGEN.csv is not supported!")

            if domain_id == "Meas Value":
                # Not Handled
                raise NotImplementedError("'Meas Value' in the MESSUNGEN.csv is not supported!")

        # ICD.csv ------------------------------------------------------------------------------------------------------
        icd_df = icd_pd[icd_pd.kh_internes_kennzeichen.isin(internal_ids)]

        for i, row in icd_df.iterrows():

            icd_version = int(row['icd_version'])
            domain_id = omop.ICD10GM_LUT[str(row["icd_kode"])]['domain_id']

            # get correct concept_id according to icd_version
            concept_id = None
            for id, start, end in omop.ICD10GM_LUT[str(row['icd_kode'])]['concept_ids']:
                if end.year >= icd_version >= start.year:
                    concept_id = id
            assert (concept_id is not None)

            if domain_id == "Observation":

                # adding optional information if given
                optional = {}
                if str(row["diagnosensicherheit"]) != "nan":
                    optional['qualifier_source_value'] = str(row["diagnosensicherheit"])
                # TODO was ist mit lokalisation & sekundaer_kode

                person.add_observation(observation_concept_id=str(omop.ICD10GM2SNOMED[concept_id]),
                                       observation_date="1999-01-01",  # TODO date ist requiered abder nicht angegeben
                                       observation_type_concept_id="0000",  # TODO welche type concept id?
                                       observation_source_concept_id=str(concept_id),
                                       observation_source_value=str(row['icd_kode']),
                                       **optional
                                       )

            if domain_id == "Condition":
                # TODO was ist mit diagnosensicherheit, lokalisation & sekundaer_kode

                person.add_condition(condition_concept_id=str(omop.ICD10GM2SNOMED[concept_id]),
                                     condition_start_date="1999-01-01",  # TODO date ist requiered abder nicht angegeben
                                     condition_start_datetime="1999-01-01 00:00:00",  # TODO date ist requiered abder nicht angegeben
                                     condition_type_concept_id=omop.CONDITION_TYPE_LUT[str(row['diagnoseart'])],
                                     condition_source_concept_id=str(concept_id),
                                     condition_source_value=str(row['icd_kode']),

                                     )

            if domain_id == "Measurement":
                # Not Handled
                raise NotImplementedError("'Measurement' in the ICD.csv is not supported!")

            if domain_id == "Procedure":
                # Not Handled
                raise NotImplementedError("'Procedure' in the ICD.csv is not supported!")

        # OPS.csv ------------------------------------------------------------------------------------------------------
        # TODO

        # insert into database -----------------------------------------------------------------------------------------
        res = person.insert_into_db()
        for sql in res:
            pass
            #print(sql)
            omop.insert(sql)
        # omop.commit()  # TODO enable commit of whole person
