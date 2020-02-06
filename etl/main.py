from optparse import OptionParser
from sys import stderr
from tqdm import tqdm
import pandas as pd
import os

from classes import Person
from database import OMOP



def labor(person, database_connection, row):
    """ adds entry of LABOR.csv to the patient
    :param person: object collecting the patients data to be inserted into the database
    :param database_connection: database connector
    :param row: row of pandas data frame containing the entry's data
    """
    domain_id = database_connection.LOINC_LUT.get(str(row["LOINC"]))['domain_id']
    concept_id = database_connection.get_valid_concept_id(LUT=database_connection.LOINC_LUT,
                                                          code=str(row['LOINC']))

    if domain_id == "Measurement":
        # adding optional information if given (low & high values)
        optional = {}
        if str(row["low"]) != "nan":
            optional['range_low'] = str(row["low"])
        if str(row["high"]) != "nan":
            optional['range_high'] = str(row["high"])

        # add measurement entry to patients data struct
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

    elif domain_id == "KeyNotFound":
        print(f"WARNING: Skipping row in LABOR.csv! \n{row}", file=stderr)

    else:
        print(f"WARNING: '{domain_id}' in the LABOR.csv is not supported!\n"
              f" Skipping row in LABOR.csv! \n"
              f"{row}", file=stderr)


def messungen(person, database_connection, row):
    """ adds entry of MESSUNGEN.csv to the patient
        :param person: object collecting the patients data to be inserted into the database
        :param database_connection: database connector
        :param row: row of pandas data frame containing the entry's data
        """
    domain_id = database_connection.LOINC_LUT.get(str(row["LOINC"]))['domain_id']
    concept_id = database_connection.get_valid_concept_id(LUT=database_connection.LOINC_LUT,
                                                          code=str(row['LOINC']))

    if domain_id == "Measurement":
        # add measurement entry to patients data struct
        person.add_measurement(measurement_concept_id=str(concept_id),
                               measurement_date=str(row["timestamp"])[:10],
                               measurement_datetime=str(row["timestamp"]),
                               measurement_type_concept_id="44818701",  # From physical examination
                               measurement_source_concept_id=str(concept_id),
                               measurement_source_value=str(row["LOINC"]),
                               value_as_number=str(row["value"]),
                               unit_source_value=str(row['unit']),
                               )

    elif domain_id == "KeyNotFound":
        print(f"WARNING: Skipping row in MESSUNGEN.csv! \n{row}", file=stderr)

    else:
        print(f"WARNING: 'Meas {domain_id}' in the MESSUNGEN.csv is not supported!\n"
              f" Skipping row in MESSUNGEN.csv! \n"
              f"{row}", file=stderr)


def icd(person, database_connection, row, ND=False):
    """ adds entry of ICD.csv to the patient
        :param person: object collecting the patients data to be inserted into the database
        :param database_connection: database connector
        :param row: row of pandas data frame containing the entry's data
        :param ND: Flag if the 'sekundär_kode' instead of the 'icd_kode' will be added
        """

    if ND:
        icd_code = str(row['sekundaer_kode'])[:-1]
    else:
        icd_code = str(row['icd_kode'])

    icd_version = int(row['icd_version'])
    domain_id = database_connection.ICD10GM_LUT.get(icd_code)['domain_id']
    concept_id = database_connection.get_valid_concept_id(LUT=database_connection.ICD10GM_LUT,
                                                          code=icd_code,
                                                          code_version=icd_version)

    fall_aufnahmedatum = str(files_pd['fall'][files_pd['fall'].kh_internes_kennzeichen.isin(
        [row['kh_internes_kennzeichen']])].iloc[0]['aufnahmedatum'])

    #  add sekundaer_kode if not already adding with this call
    if not ND and str(row["sekundaer_kode"]) != "nan":
        icd(person, database_connection, row, ND=True)

    if domain_id == "Observation":
        # add observation entry to patients data struct
        observation = person.add_observation(
            observation_concept_id=str(database_connection.ICD10GM2SNOMED[concept_id]),
            observation_date=fall_aufnahmedatum[:8],
            observation_type_concept_id="38000280",  # Observation recorded from EHR
            observation_source_concept_id=str(concept_id),
            observation_source_value=icd_code,
        )

        # add lokalisation
        if str(row["lokalisation"]) != "nan":
            localisation = person.add_observation(
                observation_concept_id=database_connection.LOCALISATION_LUT.get(str(row['lokalisation'])),
                observation_date=fall_aufnahmedatum[:8],
                observation_type_concept_id="38000280",  # Observation recorded from EHR
                observation_source_value=str(row['lokalisation']),
                value_as_string=str(row['lokalisation']),
            )

            # add fact relationship
            person.add_fact_relationship('o', observation,
                                         'o', localisation)

        # adding diagnosensicherheit
        if str(row["diagnosensicherheit"]) != "nan":
            diagnosensicherheit = person.add_observation(
                observation_concept_id=database_connection.DIAGNOSENSICHERHEIT_LUT.get(
                    str(row["diagnosensicherheit"])),
                observation_date=fall_aufnahmedatum[:8],
                observation_type_concept_id="38000280",  # Observation recorded from EHR
                observation_source_value=str(row["diagnosensicherheit"]),
                value_as_string=str(row["diagnosensicherheit"]),
            )
            # add fact relationship
            person.add_fact_relationship('o', observation,
                                         'o', diagnosensicherheit)

    elif domain_id == "Condition":
        if ND:
            diagnose_art = "ND"
        else:
            diagnose_art = str(row['diagnoseart'])

        datetime = f"{fall_aufnahmedatum[0:4]}-{fall_aufnahmedatum[4:6]}-{fall_aufnahmedatum[6:8]} " \
                   f"{fall_aufnahmedatum[8:10]}:{fall_aufnahmedatum[10:12]}:00"

        # add condition entry to patients data struct
        condition = person.add_condition(
            condition_concept_id=str(database_connection.ICD10GM2SNOMED[concept_id]),
            condition_start_date=fall_aufnahmedatum[:8],
            condition_start_datetime=datetime,
            condition_type_concept_id=database_connection.CONDITION_TYPE_LUT.get(diagnose_art),
            condition_source_concept_id=str(concept_id),
            condition_source_value=icd_code,
        )

        # add lokalisation
        if str(row["lokalisation"]) != "nan":
            localisation = person.add_observation(
                observation_concept_id=database_connection.LOCALISATION_LUT.get(str(row['lokalisation'])),
                observation_date=fall_aufnahmedatum[:8],
                observation_type_concept_id="38000280",  # Observation recorded from EHR
                observation_source_value=str(row['lokalisation']),
                value_as_string=str(row['lokalisation']),
            )
            # add fact relationship
            person.add_fact_relationship('c', condition,
                                         'o', localisation)  # localisation

        # adding diagnosensicherheit
        if str(row["diagnosensicherheit"]) != "nan":
            diagnosensicherheit = person.add_observation(
                observation_concept_id=database_connection.DIAGNOSENSICHERHEIT_LUT.get(
                    str(row["diagnosensicherheit"])),
                observation_date=fall_aufnahmedatum[:8],
                observation_type_concept_id="38000280",  # Observation recorded from EHR
                observation_source_value=str(row["diagnosensicherheit"]),
                value_as_string=str(row["diagnosensicherheit"]),
            )
            # add fact relationship
            person.add_fact_relationship('o', condition,
                                         'o', diagnosensicherheit)

    elif domain_id == "KeyNotFound":
        print(f"WARNING: Skipping row in ICD.csv! \n{row}", file=stderr)

    else:
        print(f"WARNING: '{domain_id}' in the ICD.csv is not supported!\n"
              f" Skipping row in ICD.csv! \n"
              f"{row}", file=stderr)


def ops(person, database_connection, row):
    """ adds entry of OPS.csv to the patient
        :param person: object collecting the patients data to be inserted into the database
        :param database_connection: database connector
        :param row: row of pandas data frame containing the entry's data
        """
    ops_version = int(row['ops_version'])
    domain_id = database_connection.OPS_LUT.get(str(row["ops_kode"]))['domain_id']
    concept_id = database_connection.get_valid_concept_id(LUT=database_connection.OPS_LUT,
                                                          code=str(row['ops_kode']),
                                                          code_version=ops_version)

    if domain_id == "Procedure":
        datetime = f'{str(row["ops_datum"])[0:4]}-{str(row["ops_datum"])[4:6]}-{str(row["ops_datum"])[6:8]} ' \
                   f'{str(row["ops_datum"])[8:10]}:{str(row["ops_datum"])[10:12]}:00'

        # add procedure entry to patients data struct
        procedure = person.add_procedure(procedure_concept_id=str(concept_id),
                                         procedure_date=str(row["ops_datum"])[:8],
                                         procedure_datetime=datetime,
                                         procedure_type_concept_id="38003622",  # Procedure recorded as diagnostic code
                                         procedure_source_value=str(row['ops_kode']),
                                         procedure_source_concept_id=str(concept_id),
                                         )

        # add lokalisation
        if str(row["lokalisation"]) != "nan":
            localisation = person.add_observation(
                observation_concept_id=database_connection.LOCALISATION_LUT.get(str(row['lokalisation'])),
                observation_date=datetime,
                observation_type_concept_id="38000280",  # Observation recorded from EHR
                observation_source_value=str(row['lokalisation']),
                value_as_string=str(row['lokalisation']),
            )
            # add fact relationship
            person.add_fact_relationship('p', procedure,  # procedure
                                         'o', localisation)  # localisation

    elif domain_id == "KeyNotFound":
        print(f"WARNING: Skipping row in OPS.csv! \n{row}", file=stderr)

    else:
        print(f"WARNING: '{domain_id}' in the OPS.csv is not supported!\n"
              f" Skipping row in OPS.csv! \n"
              f"{row}", file=stderr)


def patient(id, database_connection, files_pd):
    """ insert patiend with id into database
    collects all entries in the csv files concerning the patient

    :param id: id defining the patient to be inserted
    :param database_connection: database connector
    :type database_connection: OMOP
    :param files_pd: pandas io parser of the csv files containing the patient data
    """

    #  FALL.csv ----------------------------------------------------------------------------------------------------
    # select all rows containing entries concerning the patient
    fall_df = files_pd['fall'][files_pd['fall'].patienten_nummer.isin([id])]

    # get newest entry
    fall_df = fall_df.sort_values(by=["aufnahmedatum"])
    last_record = fall_df.iloc[-1]

    # set location according to the latest entry of the patient
    location = {"city": last_record.wohnort, "zip": last_record.plz}

    # create the patients data struct containing basic patient data and later all clinical findings to be inserted in DB
    person = Person(person_id=str(id),
                    gender_concept_id=database_connection.GENDER_LUT[last_record["geschlecht"]],
                    year_of_birth=str(last_record["geburtsjahr"]),
                    month_of_birth=str(last_record["geburtsmonat"]),
                    race_concept_id="8552",  # Unknown
                    ethnicity_concept_id="38003564",  # Non-Hispanic
                    gender_source_value=last_record["geschlecht"],
                    location=location,
                    )

    # create new visit for every entry in FALL.csv concerning the patient
    for _, row in fall_df.iterrows():
        # get care site name from FAB.csv if exists else set name "0"
        fabs = files_pd['fab'][files_pd['fab']['kh_internes_kennzeichen'] == int(row['kh_internes_kennzeichen'])]['fab']
        if len(fabs):
            fab = fabs.iloc[-1]
        else:
            fab = "0"

        # add visit to patients data struct
        person.add_visit(visit_occurrence_id=str(row['kh_internes_kennzeichen']),
                         visit_concept_id=database_connection.VISIT_TYPE_LUT.get(str(row['aufnahmeanlass'])),
                         visit_start_date=str(row["aufnahmedatum"])[:8],
                         visit_end_date=str(row["entlassungsdatum"])[:8],
                         visit_type_concept_id="44818518",  # Visit derived from EHR record
                         visit_source_value=str(row['aufnahmeanlass']),
                         admitting_source_value=str(row['aufnahmegrund']),
                         discharge_to_source_value=str(row['entlassungsgrund']),
                         care_site_name=fab,
                         )

    # get associated internal ids for patient
    internal_ids = [key for key in fall_df["kh_internes_kennzeichen"]]

    # LABOR.csv --------------------------------------------------------------------------------------------------------
    # select all rows containing entries concerning the patient
    labor_df = files_pd['labor'][files_pd['labor'].kh_internes_kennzeichen.isin(internal_ids)]

    # insert entries into the patients data struct
    for _, row in labor_df.iterrows():
        labor(person, database_connection, row)

    # MESSUNGEN.csv ----------------------------------------------------------------------------------------------------
    # select all rows containing entries concerning the patient
    messungen_df = files_pd['messungen'][files_pd['messungen'].kh_internes_kennzeichen.isin(internal_ids)]

    # insert entries into the patients data struct
    for _, row in messungen_df.iterrows():
        messungen(person, database_connection, row)

    # ICD.csv ----------------------------------------------------------------------------------------------------------
    # select all rows containing entries concerning the patient
    icd_df = files_pd['icd'][files_pd['icd'].kh_internes_kennzeichen.isin(internal_ids)]

    # insert entries into the patients data struct
    for _, row in icd_df.iterrows():
        icd(person, database_connection, row)

    # OPS.csv ----------------------------------------------------------------------------------------------------------
    # select all rows containing entries concerning the patient
    ops_df = files_pd['ops'][files_pd['ops'].kh_internes_kennzeichen.isin(internal_ids)]

    # insert entries into the patients data struct
    for _, row in ops_df.iterrows():
        ops(person, database_connection, row)

    # insert accumulated data into database ----------------------------------------------------------------------------
    person.insert_into_db(database_connection)


def get_opts_and_args():
    parser = OptionParser()
    parser.add_option("--db_host", dest="db_host", default="localhost")
    parser.add_option("--db_port", dest="db_port", default="5432")
    parser.add_option("--csv_delimiter", dest="csv_del", default=";")
    return parser.parse_args()


if __name__ == "__main__":
    # get options and arguments
    opts, args = get_opts_and_args()

    # get path to csv files
    csv_dir = args[0]

    # load csv files
    fab_csv = os.path.join(csv_dir, "FAB.csv")
    fall_csv = os.path.join(csv_dir, "FALL.csv")
    icd_csv = os.path.join(csv_dir, "ICD.csv")
    messungen_csv = os.path.join(csv_dir, "MESSUNGEN.csv")
    labor_csv = os.path.join(csv_dir, "LABOR.csv")
    ops_csv = os.path.join(csv_dir, "OPS.csv")

    # collect all files in a dict
    files_pd = {'fab': pd.read_csv(fab_csv, delimiter=opts.csv_del),
                'fall': pd.read_csv(fall_csv, delimiter=opts.csv_del),
                'icd': pd.read_csv(icd_csv, delimiter=opts.csv_del),
                'messungen': pd.read_csv(messungen_csv, delimiter=opts.csv_del),
                'labor': pd.read_csv(labor_csv, delimiter=opts.csv_del),
                'ops': pd.read_csv(ops_csv, delimiter=opts.csv_del)}

    # init database connector
    omop = OMOP(host=opts.db_host, port=opts.db_port)

    # add one patient after another
    for id in tqdm(files_pd['fall']["patienten_nummer"].unique()):
        patient(id, omop, files_pd)

    '''
    # insert multiple patients in parallel into Database (BusError occuring -> not working)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        ts = [executor.submit(patient, id) for id in tqdm(fall_pd["patienten_nummer"].unique())]

        bar = tqdm(total=len(ts))
        for t in concurrent.futures.as_completed(ts):
            bar.update(1)
    '''
