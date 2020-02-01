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

OUTPUT_DIR = "output/dataset/"
DATABASE_NAME = "p21_cdm"


def call(self, sql: str):
    self.execute(sql)
    return self.fetchall()


class DB_Connector:
    def __init__(self,
                 dbname='OHDSI',
                 user='ohdsi_admin_user',
                 host='localhost',
                 port='5432',
                 password='omop'):
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'")
        self.cursors = []
        self._get_cursor_lock = threading.Lock()

    def __del__(self):
        with self._get_cursor_lock:
            for cursor in self.cursors:
                del cursor
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()

    class Cursor:
        def __init__(self, conn):
            self.conn = conn
            self.cursor = self.conn.cursor()

        def __call__(self, sql):
            self.cursor.execute(sql)
            return self.cursor.fetchall()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()

        def __del__(self):
            self.close()

        def close(self):
            self.cursor.close()

    def get_cursor(self):
        c = self.Cursor(self.conn)

        with self._get_cursor_lock:
            self.cursors.append(c)

        return c


def get_table(tablename: str, colum_map: dict, condition=1):
    with omop.get_cursor() as cursor:
        cols = ""
        for col in sorted(colum_map.keys()):
            cols += f"{col},"

        rows = cursor(f""" SELECT {cols[:-1]} FROM {DATABASE_NAME}.{tablename} WHERE {condition}""")
        colum_names = [v for k, v in sorted(colum_map.items(), key=lambda x: x[0])]
        return [{colum_names[i]: v for i, v in enumerate(row)} for row in rows]


def prepare_person(pid, *args):
    with omop.get_cursor() as cursor:
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
                                "'1'": "value",
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

        concept_ids = sorted(set([snomed_lut[str(row['id'])] for row in data]))
        concept_id_lut = {id: i for i, id in enumerate(concept_ids)}
        dates = sorted(set([row['date'] for row in data]))
        dates_lut = {d: i for i, d in enumerate(dates)}

        patient_data = np.zeros((len(concept_ids), len(dates)), dtype=np.float)

        # entrys -> patient data
        for row in data:
            concept_id = snomed_lut[str(row['id'])]
            if 'end_date' in row:
                enddate = []
                if row['end_date'] is not None:
                    enddate += [i for i in range(len(dates) - 1)
                                if dates[i] <= row['end_date'] < dates[i + 1]]

                enddate += [len(dates) - 1]
                patient_data[concept_id_lut[concept_id], dates_lut[row['date']]:enddate[0]] = row['value']
            else:
                patient_data[concept_id_lut[concept_id], dates_lut[row['date']]] = row['value']

        # patient data -> samples
        samples = ()
        for i in range(len(dates) - 1):
            sample = {concept_ids[j]: r for j, r in enumerate(patient_data[:, i])}
            sample["age"] = age
            sample["female"] = int(gender_concept_id == 8532)
            sample["male"] = int(gender_concept_id == 8507)
            label = bool(set([concept_ids[j] for j in range(len(concept_ids)) if patient_data[j, i + 1] != 0]) & labels)
            samples += (sample, label),

        with open(os.path.join(OUTPUT_DIR,
                               "1" if sum([int(s[1]) for s in samples]) else "0",
                               f"{str(pid).zfill(6)}.pkl"),
                  'wb') as file:
            pickle.dump(samples, file)


def split_set(pos_neg_ratio=0.1):
    negative = [f for f in os.listdir(os.path.join(OUTPUT_DIR, "0")) if f.endswith(".pkl")]
    positive = [f for f in os.listdir(os.path.join(OUTPUT_DIR, "1")) if f.endswith(".pkl")]

    for f in negative[:int(len(negative) * pos_neg_ratio)]:
        os.rename(os.path.join(OUTPUT_DIR, "0", f), os.path.join(OUTPUT_DIR, "test", f))
    for f in negative[int(len(negative) * pos_neg_ratio):]:
        os.rename(os.path.join(OUTPUT_DIR, "0", f), os.path.join(OUTPUT_DIR, "train", f))
    for f in positive[:int(len(positive) * pos_neg_ratio)]:
        os.rename(os.path.join(OUTPUT_DIR, "1", f), os.path.join(OUTPUT_DIR, "test", f))
    for f in positive[int(len(positive) * pos_neg_ratio):]:
        os.rename(os.path.join(OUTPUT_DIR, "1", f), os.path.join(OUTPUT_DIR, "train", f))


def main():
    rmtree(OUTPUT_DIR)
    os.mkdir(OUTPUT_DIR)
    os.mkdir(os.path.join(OUTPUT_DIR, "0"))
    os.mkdir(os.path.join(OUTPUT_DIR, "1"))

    global omop, alphabet_d, labels, snomed_lut

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

    os.mkdir(os.path.join(OUTPUT_DIR, "train"))
    os.mkdir(os.path.join(OUTPUT_DIR, "test"))
    split_set()
    rmtree(os.path.join(OUTPUT_DIR, "0"))
    rmtree(os.path.join(OUTPUT_DIR, "1"))


if __name__ == '__main__':
    main()
