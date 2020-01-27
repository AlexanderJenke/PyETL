import pandas as pd
# from sys import argv
import os
from Classes import Person
from Database import OMOP
from optparse import OptionParser

from tqdm import tqdm
from sys import stderr

CSV_DELIMITER = ";"


def get_opts_and_args():
    parser = OptionParser()
    parser.add_option("--db_host", dest="db_host", default="localhost")
    parser.add_option("--db_port", dest="db_port", default="5432")
    return parser.parse_args()


if __name__ == "__main__":
    opts, args = get_opts_and_args()
    csv_dir = args[0]  # Path to csv files

    # load csv files
    fab_csv = os.path.join(csv_dir, "FAB.csv")
    fall_csv = os.path.join(csv_dir, "FALL.csv")
    icd_csv = os.path.join(csv_dir, "ICD.csv")
    messungen_csv = os.path.join(csv_dir, "MESSUNGEN.csv")
    labor_csv = os.path.join(csv_dir, "LABOR.csv")
    ops_csv = os.path.join(csv_dir, "OPS.csv")

    fab_pd = pd.read_csv(fab_csv, delimiter=CSV_DELIMITER)
    fall_pd = pd.read_csv(fall_csv, delimiter=CSV_DELIMITER)
    icd_pd = pd.read_csv(icd_csv, delimiter=CSV_DELIMITER)
    messungen_pd = pd.read_csv(messungen_csv, delimiter=CSV_DELIMITER)
    labor_pd = pd.read_csv(labor_csv, delimiter=CSV_DELIMITER)
    ops_pd = pd.read_csv(ops_csv, delimiter=CSV_DELIMITER)

    # init database connector
    omop = OMOP(do_commits=False)

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
                        gender_source_value=last_record["geschlecht"],
                        location=location,
                        )

        for i, row in fall_df.iterrows():  # create new visit for every row in FALL.csv
            person.add_visit(visit_occurrence_id=str(row['kh_internes_kennzeichen']),
                             visit_concept_id=omop.VISIT_TYPE_LUT[str(row['aufnahmeanlass'])],
                             visit_start_date=str(row["aufnahmedatum"])[:8],
                             visit_end_date=str(row["entlassungsdatum"])[:8],
                             visit_type_concept_id="44818518",  # Visit derived from EHR record
                             visit_source_value=str(row['aufnahmeanlass']),
                             admitting_source_value=str(row['aufnahmegrund']),
                             discharge_to_source_value=str(row['entlassungsgrund']),
                             care_site_name=
                             fab_pd[fab_pd['KH-internes-Kennzeichen'] == int(row['kh_internes_kennzeichen'])]['FAB'].iloc[-1]
                             )

        internal_ids = [key for key in fall_df["kh_internes_kennzeichen"]]  # associated internal ids for patient

        # LABOR.csv ----------------------------------------------------------------------------------------------------
        labor_df = labor_pd[labor_pd.kh_internes_kennzeichen.isin(internal_ids)]

        for i, row in labor_df.iterrows():
            domain_id = omop.LOINC_LUT.get(str(row["LOINC"]))['domain_id']
            concept_id = omop.get_valid_concept_id(LUT=omop.LOINC_LUT,
                                                   code=str(row['LOINC']))

            if domain_id == "Measurement":
                # adding optional information if given
                optional = {}
                if str(row["low"]) != "nan":
                    optional['range_low'] = str(row["low"])
                if str(row["high"]) != "nan":
                    optional['range_high'] = str(row["high"])

                person.add_measurement(measurement_concept_id=str(concept_id),
                                       measurement_date=str(row["timestamp"])[:10],
                                       measurement_datetime=str(row["timestamp"]),
                                       measurement_type_concept_id="44818702",  # Lab Result
                                       value_as_number=str(row["value"]),
                                       measurement_source_concept_id=str(concept_id),
                                       measurement_source_value=str(row["LOINC"]),
                                       unit_source_value=str(row['unit']),
                                       **optional
                                       )

            elif domain_id == "Observation":
                # Not Handled
                raise NotImplementedError("'Observation' in the LABOR.csv is not supported!")

            elif domain_id == "Meas Value":
                # Not Handled
                raise NotImplementedError("'Meas Value' in the LABOR.csv is not supported!")

            elif domain_id == "KeyNotFound":
                print(f"WARNING: Skipping row in LABOR.csv! \n{row}", file=stderr)

            else:
                raise NotImplementedError(f"'{domain_id}' in the LABOR.csv is not supported!")

        # MESSUNGEN.csv ------------------------------------------------------------------------------------------------
        messungen_df = messungen_pd[messungen_pd.kh_internes_kennzeichen.isin(internal_ids)]

        for i, row in messungen_df.iterrows():
            domain_id = omop.LOINC_LUT.get(str(row["LOINC"]))['domain_id']
            concept_id = omop.get_valid_concept_id(LUT=omop.LOINC_LUT,
                                                   code=str(row['LOINC']))

            if domain_id == "Measurement":
                person.add_measurement(measurement_concept_id=str(concept_id),
                                       measurement_date=str(row["timestamp"])[:10],
                                       measurement_datetime=str(row["timestamp"]),
                                       measurement_type_concept_id="44818701",  # From physical examination
                                       measurement_source_concept_id=str(concept_id),
                                       measurement_source_value=str(row["LOINC"]),
                                       value_as_number=str(row["value"]),
                                       unit_source_value=str(row['unit']),
                                       )

            elif domain_id == "Observation":
                # Not Handled
                raise NotImplementedError("Observation in the MESSUNGEN.csv is not supported!")

            elif domain_id == "Meas Value":
                # Not Handled
                raise NotImplementedError("'Meas Value' in the MESSUNGEN.csv is not supported!")

            elif domain_id == "KeyNotFound":
                print(f"WARNING: Skipping row in MESSUNGEN.csv! \n{row}", file=stderr)

            else:
                raise NotImplementedError(f"'{domain_id}' in the MESSUNGEN.csv is not supported!")

        # ICD.csv ------------------------------------------------------------------------------------------------------
        icd_df = icd_pd[icd_pd.kh_internes_kennzeichen.isin(internal_ids)]

        for i, row in icd_df.iterrows():
            icd_version = int(row['icd_version'])
            domain_id = omop.ICD10GM_LUT.get(str(row["icd_kode"]))['domain_id']
            concept_id = omop.get_valid_concept_id(LUT=omop.ICD10GM_LUT,
                                                   code=str(row['icd_kode']),
                                                   code_version=icd_version)

            fall_aufnahmedatum = str(fall_pd[fall_pd.kh_internes_kennzeichen.isin(
                [row['kh_internes_kennzeichen']])].iloc[0]['aufnahmedatum'])

            if domain_id == "Observation":

                # adding optional information if given
                optional = {}
                if str(row["diagnosensicherheit"]) != "nan":
                    optional['qualifier_source_value'] = str(row["diagnosensicherheit"])
                    # TODO add diagnosensicherheit

                # add lokalisation
                if str(row["lokalisation"]) != "nan":
                    person.add_observation(observation_concept_id=omop.LOCALISATION_LUT.get(str(row['lokalisation'])),
                                           observation_date=fall_aufnahmedatum[:8],
                                           observation_type_concept_id="38000280",  # Observation recorded from EHR
                                           observation_source_value=str(row['lokalisation']),
                                           value_as_string=str(row['lokalisation']),
                                           )

                person.add_observation(observation_concept_id=str(omop.ICD10GM2SNOMED[concept_id]),
                                       observation_date=fall_aufnahmedatum[:8],
                                       observation_type_concept_id="38000280",  # Observation recorded from EHR
                                       observation_source_concept_id=str(concept_id),
                                       observation_source_value=str(row['icd_kode']),
                                       **optional
                                       )
                #  add sekundaer_kode
                if str(row["sekundaer_kode"]) != "nan":
                    concept_id = omop.get_valid_concept_id(LUT=omop.ICD10GM_LUT,
                                                           code=str(row['sekundaer_kode'])[:-1],
                                                           # remove '*' at the end of code
                                                           code_version=icd_version)
                    # adding optional information if given
                    optional = {}
                    if str(row["diagnosensicherheit"]) != "nan":
                        optional['qualifier_source_value'] = str(row["diagnosensicherheit"])
                    person.add_observation(observation_concept_id=str(omop.ICD10GM2SNOMED[concept_id]),
                                           observation_date=fall_aufnahmedatum[:8],
                                           observation_type_concept_id="38000280",  # Observation recorded from EHR
                                           observation_source_concept_id=str(concept_id),
                                           observation_source_value=str(row['sekundaer_kode']),
                                           **optional
                                           )
                    # add lokalisation
                    if str(row["lokalisation"]) != "nan":
                        person.add_observation(
                            observation_concept_id=omop.LOCALISATION_LUT.get(str(row['lokalisation'])),
                            observation_date=fall_aufnahmedatum[:8],
                            observation_type_concept_id="38000280",  # Observation recorded from EHR
                            observation_source_value=str(row['lokalisation']),
                            value_as_string=str(row['lokalisation']),
                        )

                    # TODO add FACT_RELATIONSHIPs


            elif domain_id == "Condition":
                # TODO add diagnosensicherheit
                datetime = f"{fall_aufnahmedatum[0:4]}-{fall_aufnahmedatum[4:6]}-{fall_aufnahmedatum[6:8]} " \
                           f"{fall_aufnahmedatum[8:10]}:{fall_aufnahmedatum[10:12]}:00"

                # add lokalisation
                if str(row["lokalisation"]) != "nan":
                    person.add_observation(observation_concept_id=omop.LOCALISATION_LUT.get(str(row['lokalisation'])),
                                           observation_date=fall_aufnahmedatum[:8],
                                           observation_type_concept_id="38000280",  # Observation recorded from EHR
                                           observation_source_value=str(row['lokalisation']),
                                           value_as_string=str(row['lokalisation']),
                                           )

                person.add_condition(condition_concept_id=str(omop.ICD10GM2SNOMED[concept_id]),
                                     condition_start_date=fall_aufnahmedatum[:8],
                                     condition_start_datetime=datetime,
                                     condition_type_concept_id=omop.CONDITION_TYPE_LUT.get(str(row['diagnoseart'])),
                                     condition_source_concept_id=str(concept_id),
                                     condition_source_value=str(row['icd_kode']),
                                     )

                #  add sekundaer_kode
                if str(row["sekundaer_kode"]) != "nan":
                    concept_id = omop.get_valid_concept_id(LUT=omop.ICD10GM_LUT,
                                                           code=str(row['sekundaer_kode'])[:-1],
                                                           # remove '*' at the end of code
                                                           code_version=icd_version)
                    # add lokalisation
                    if str(row["lokalisation"]) != "nan":
                        person.add_observation(
                            observation_concept_id=omop.LOCALISATION_LUT.get(str(row['lokalisation'])),
                            observation_date=fall_aufnahmedatum[:8],
                            observation_type_concept_id="38000280",  # Observation recorded from EHR
                            observation_source_value=str(row['lokalisation']),
                            value_as_string=str(row['lokalisation']),
                        )

                    person.add_condition(condition_concept_id=str(omop.ICD10GM2SNOMED[concept_id]),
                                         condition_start_date=fall_aufnahmedatum[:8],
                                         condition_start_datetime=datetime,
                                         condition_type_concept_id=omop.CONDITION_TYPE_LUT.get("ND"),  # always ND
                                         condition_source_concept_id=str(concept_id),
                                         condition_source_value=str(row['sekundaer_kode']),
                                         )
                # TODO add FACT_RELATIONSHIPs

            elif domain_id == "Measurement":
                # Not Handled
                raise NotImplementedError("'Measurement' in the ICD.csv is not supported!")

            elif domain_id == "Procedure":
                # Not Handled
                raise NotImplementedError("'Procedure' in the ICD.csv is not supported!")

            elif domain_id == "KeyNotFound":
                print(f"WARNING: Skipping row in ICD.csv! \n{row}", file=stderr)

            else:
                raise NotImplementedError(f"'{domain_id}' in the ICD.csv is not supported!")

        # OPS.csv ------------------------------------------------------------------------------------------------------
        ops_df = ops_pd[ops_pd.kh_internes_kennzeichen.isin(internal_ids)]

        for i, row in ops_df.iterrows():
            ops_version = int(row['ops_version'])
            domain_id = omop.OPS_LUT.get(str(row["ops_kode"]))['domain_id']
            concept_id = omop.get_valid_concept_id(LUT=omop.OPS_LUT,
                                                   code=str(row['ops_kode']),
                                                   code_version=ops_version)

            if domain_id == "Procedure":
                datetime = f'{str(row["ops_datum"])[0:4]}-{str(row["ops_datum"])[4:6]}-{str(row["ops_datum"])[6:8]} ' \
                           f'{str(row["ops_datum"])[8:10]}:{str(row["ops_datum"])[10:12]}:00'

                person.add_procedure(procedure_concept_id=str(concept_id),
                                     procedure_date=str(row["ops_datum"])[:8],
                                     procedure_datetime=datetime,
                                     procedure_type_concept_id="38003622",  # Procedure recorded as diagnostic code
                                     procedure_source_value=str(row['ops_kode']),
                                     procedure_source_concept_id=str(concept_id),
                                     )

                # add lokalisation
                if str(row["lokalisation"]) != "nan":
                    person.add_observation(observation_concept_id=omop.LOCALISATION_LUT.get(str(row['lokalisation'])),
                                           observation_date=fall_aufnahmedatum[:8],
                                           observation_type_concept_id="38000280",  # Observation recorded from EHR
                                           observation_source_value=str(row['lokalisation']),
                                           value_as_string=str(row['lokalisation']),
                                           )


            elif domain_id == "Observation":
                # Not Handled
                raise NotImplementedError("'Observation' in the OPS.csv is not supported!")

            elif domain_id == "Condition":
                # Not Handled
                raise NotImplementedError("'Condition' in the OPS.csv is not supported!")

            elif domain_id == "Measurement":
                # Not Handled
                raise NotImplementedError("'Measurement' in the OPS.csv is not supported!")

            elif domain_id == "KeyNotFound":
                print(f"WARNING: Skipping row in OPS.csv! \n{row}", file=stderr)

            else:
                raise NotImplementedError(f"'{domain_id}' in the OPS.csv is not supported!")

        # insert into database -----------------------------------------------------------------------------------------
        sqls = person.insert_into_db()
        for sql in sqls:
            id = omop.select(sql)
            # print(id[0][0], sql)
        omop.commit()  # TODO enable commit of whole person or activate commits on omop-init
