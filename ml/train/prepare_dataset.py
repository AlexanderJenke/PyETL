import psycopg2 as db
import pickle
from sys import stderr
from datetime import date
import numpy as np
import os
import threading
import concurrent.futures
from tqdm import tqdm
from shutil import rmtree

OUTPUT_DIR = "output/dataset_pos_f5/"  # path where to save the generated data
DATABASE_NAME = "p21_cdm"  # database to use
FUTURE_HORIZON = 5  # number of future time steps to include into the label
ONLY_POS = True  # only use patients who devellop a decubitus at least once within the recordings?


class DB_Connector:
    """ database connection to the omop cdm"""
    def __init__(self,
                 dbname='OHDSI',
                 user='ohdsi_admin_user',
                 host='localhost',
                 port='5432',
                 password='omop'):
        """initialize the connector"""
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'")
        self.cursors = []
        self._get_cursor_lock = threading.Lock()

    def __del__(self):
        """ deconstruct the connector
            close all cursors and the connection
        """
        with self._get_cursor_lock:
            for cursor in self.cursors:
                del cursor
        self.conn.close()

    def __enter__(self):
        """ enable code wrapping with the with statement """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ enable code wrapping with the with statement
            deconstruct connection on exit
        """
        self.__del__()

    class Cursor:
        """ sub class containing cursor functionality to enable different threads to use their own cursor """
        def __init__(self, conn):
            """ create new cursor"""
            self.conn = conn
            self.cursor = self.conn.cursor()

        def __call__(self, sql):
            """ execute slq statement """
            self.cursor.execute(sql)
            return self.cursor.fetchall()

        def __enter__(self):
            """ enable code wrapping with the with statement """
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            """ enable code wrapping with the with statement
                deconstruct cursor on exit
            """
            self.close()

        def __del__(self):
            """ deconstruct cursor and close cursor"""
            self.close()

        def close(self):
            """ close cursor """
            self.cursor.close()

    def get_cursor(self):
        """ returns a new cursor
            this is the point where threts get their own cursor
        """
        c = self.Cursor(self.conn)

        # keep track of all cursors
        with self._get_cursor_lock:
            self.cursors.append(c)

        return c


def get_table(tablename: str, colum_map: dict, condition=1):
    """ helper function to get specific colums of a table
        return a list of dicts containing colums content connected to the colums name
        each dict represents a row in the database
    """
    with omop.get_cursor() as cursor:
        cols = ""
        for col in sorted(colum_map.keys()):
            cols += f"{col},"

        rows = cursor(f""" SELECT {cols[:-1]} FROM {DATABASE_NAME}.{tablename} WHERE {condition}""")
        colum_names = [v for k, v in sorted(colum_map.items(), key=lambda x: x[0])]
        return [{colum_names[i]: v for i, v in enumerate(row)} for row in rows]


def prepare_person(pid, *args):
    """ prepares samples from the omop cdm
        loads all data from one patient specified by the id
        slices the patients timeline on every unique date, where new data was added
    """
    with omop.get_cursor() as cursor:
        # get basic patient data
        gender_concept_id, year_of_birth = cursor(f"""SELECT gender_concept_id, year_of_birth 
                                                      FROM {DATABASE_NAME}.person
                                                      WHERE person_id='{pid}'""")[-1]
        age = date.today().year - year_of_birth

        # get data
        obserations = get_table("observation",
                                {"observation_concept_id": "id",
                                 "value_as_number": "value",
                                 "observation_date": "date",
                                 },
                                f"person_id='{pid}'")

        measurements = get_table("measurement",
                                 {"measurement_concept_id": "id",
                                  "value_as_number": "value",
                                  "measurement_date": "date",
                                  },
                                 f"person_id='{pid}'")

        conditions = get_table("condition_occurrence",
                               {"condition_concept_id": "id",
                                "'1'": "value",  #
                                "condition_start_date": "date",
                                "condition_end_date": "end_date",
                                },
                               f"person_id='{pid}'")

        procedures = get_table("procedure_occurrence",
                               {"procedure_concept_id": "id",
                                "quantity": "value",
                                "procedure_date": "date",
                                },
                               f"person_id='{pid}'")

        data = obserations + measurements + conditions + procedures

        # prepare LUTs
        concept_ids = sorted(set([snomed_lut[str(row['id'])] for row in data]))
        concept_id_lut = {id: i for i, id in enumerate(concept_ids)}
        dates = sorted(set([row['date'] for row in data]))
        dates_lut = {d: i for i, d in enumerate(dates)}

        patient_data = np.zeros((len(concept_ids), len(dates)), dtype=np.float)

        # entrys -> patient data
        for row in data:
            concept_id = snomed_lut[str(row['id'])]

            # if a entry does not contay a vaule ist is set to 1 to indicate the clinical finding is in the data
            if row['value'] is None:
                value = 1.0
            else:
                try:
                    value = float(row['value'])
                except ValueError:
                    print(f"ERROR: {row['value']} can not be converted into float! \n{row}")
                    continue

            # conditions with an end date are only set within the timespan
            # everything else ist set from finding until the end of the patients records
            enddate = []
            if 'end_date' in row and row['end_date'] is not None:
                enddate += [i for i in range(len(dates) - 1) if dates[i] <= row['end_date'] < dates[i + 1]]
            enddate += [len(dates)]
            patient_data[concept_id_lut[concept_id], dates_lut[row['date']]:enddate[0]] = value

        # patient data -> samples
        samples = ()
        for i in range(len(dates) - FUTURE_HORIZON):  # slice the patients timeline into samples
            sample = {concept_ids[j]: r for j, r in enumerate(patient_data[:, i])}
            sample["age"] = age
            sample["female"] = int(gender_concept_id == 8532)
            sample["male"] = int(gender_concept_id == 8507)
            # decubitus will be diagnosed newly next n timestamps (n=FUTURE_HORIZON)
            label = bool(set([concept_ids[j]
                              for j in range(len(concept_ids))
                              if patient_data[j, i] == 0 and patient_data[j, i + 1: i + FUTURE_HORIZON + 1].sum() != 0]
                             ) & labels)

            # add sample to the patients data
            samples += (sample, label),

        # save patient data to a file
        with open(os.path.join(OUTPUT_DIR,
                               "1" if sum([int(s[1]) for s in samples]) else "0",
                               f"{str(pid).zfill(6)}.pkl"),
                  'wb') as file:
            pickle.dump(samples, file)


def split_set(ratio=0.1):
    """ splits the patients into a test and a train set
    """
    negative = [f for f in os.listdir(os.path.join(OUTPUT_DIR, "0")) if f.endswith(".pkl")]
    positive = [f for f in os.listdir(os.path.join(OUTPUT_DIR, "1")) if f.endswith(".pkl")]

    if not ONLY_POS:
        for f in negative[:int(len(negative) * ratio)]:
            os.rename(os.path.join(OUTPUT_DIR, "0", f), os.path.join(OUTPUT_DIR, "test", f))
        for f in negative[int(len(negative) * ratio):]:
            os.rename(os.path.join(OUTPUT_DIR, "0", f), os.path.join(OUTPUT_DIR, "train", f))
    for f in positive[:int(len(positive) * ratio)]:
        os.rename(os.path.join(OUTPUT_DIR, "1", f), os.path.join(OUTPUT_DIR, "test", f))
    for f in positive[int(len(positive) * ratio):]:
        os.rename(os.path.join(OUTPUT_DIR, "1", f), os.path.join(OUTPUT_DIR, "train", f))


def main():
    """ main
        prepares all patients contained in the database
    """

    # print warning if existent data will be replaced
    if os.path.isdir(OUTPUT_DIR):
        if input(f"WARNING: '{OUTPUT_DIR}' already exists!\n"
                 f"         Do you want to replace the data?\n"
                 f"         [Yes, No]:\n") == "Yes":
            rmtree(OUTPUT_DIR)  # remove existent data to overwrite it
        else:
            exit(1)
    # prepare output folder structure
    os.mkdir(OUTPUT_DIR)
    os.mkdir(os.path.join(OUTPUT_DIR, "0"))
    os.mkdir(os.path.join(OUTPUT_DIR, "1"))

    global omop, alphabet_d, labels, snomed_lut

    # get database connection
    omop = DB_Connector()

    with omop.get_cursor() as cursor:

        # prepare the alphabet
        alphabet_d = {}
        for concept_id, concept_name, domain_id, vocabulary_id in cursor(f"""
          SELECT concept_id, concept_name, domain_id, vocabulary_id
          FROM {DATABASE_NAME}.concept"""):
            alphabet_d[str(concept_id)] = (concept_name, domain_id, vocabulary_id)

        alphabet_d["age"] = ("the patients age", "proprietary", "proprietary")
        alphabet_d["male"] = ("the patient is male", "proprietary", "proprietary")
        alphabet_d["female"] = ("the patient is female", "proprietary", "proprietary")

        with open(os.path.join(OUTPUT_DIR, "alphabet.pkl"), 'wb') as file:
            pickle.dump(alphabet_d, file)

        # prepare snomed_lut
        snomed_lut = {}
        for src_id, snomed_id in cursor(f"""SELECT r.concept_id_1, c.concept_id
                                            FROM {DATABASE_NAME}.concept_relationship r
                                            JOIN {DATABASE_NAME}.concept c
                                            ON r.concept_id_2=c.concept_id
                                            WHERE (r.relationship_id='Maps to'
                                                   OR r.relationship_id='CPT4 - SNOMED eq'
                                                   OR r.relationship_id='RxNorm - SNOMED eq')
                                            AND c.vocabulary_id='SNOMED'
                                            AND c.standard_concept='S'"""):
            if src_id not in snomed_lut:
                snomed_lut[str(src_id)] = str(snomed_id)
            elif snomed_lut[src_id] != snomed_id:
                print(f"WARNING: SNOMED_LUT tries to map one id to multiple ids!\n"
                      f"{src_id} -> [{snomed_lut[src_id]},{snomed_id}]\n"
                      f"{src_id} -> {snomed_lut[src_id]} is used.", file=stderr)
        for i in alphabet_d.keys():
            if i not in snomed_lut:
                snomed_lut[str(i)] = str(i)

        # define labels
        labels = set()
        print("Lables:")
        for id, t in alphabet_d.items():
            name, domain, vocab = t
            if "Pressure ulcer" in name and not "NA-Pressure" in name:
                labels.add(id)
                print(f"    {name}")
        with open(os.path.join(OUTPUT_DIR, "labels.pkl"), 'wb') as file:
            pickle.dump(labels, file)

        # extract person data
        person_ids = cursor("select distinct person_id from p21_cdm.person")
        with concurrent.futures.ThreadPoolExecutor() as tpe:
            person_threads = [tpe.submit(prepare_person, pid) for pid, *args in person_ids]
            for _ in tqdm(concurrent.futures.as_completed(person_threads), total=len(person_threads)):
                pass

    del omop

    # split generated data into train & test set
    os.mkdir(os.path.join(OUTPUT_DIR, "train"))
    os.mkdir(os.path.join(OUTPUT_DIR, "test"))
    split_set()
    rmtree(os.path.join(OUTPUT_DIR, "0"))
    rmtree(os.path.join(OUTPUT_DIR, "1"))


if __name__ == '__main__':
    main()
