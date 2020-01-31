import psycopg2 as db
import torch
import pickle
from sys import stderr
from tqdm import tqdm
from datetime import date, timedelta
import numpy as np
import pickle_workaround as pkl
import os
import threading
import concurrent.futures

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


def prepare_person(pid, *args):
    with omop.get_cursor() as cursor:
        gender_concept_id, year_of_birth = cursor(f"""SELECT gender_concept_id, year_of_birth 
                                                      FROM {DATABASE_NAME}.person
                                                      WHERE person_id='{pid}'""")[-1]
        age = date.today().year - year_of_birth
    return pid, gender_concept_id, year_of_birth, age


def main():
    global omop, alphabet_d, labels, snomed_lut

    omop = DB_Connector()

    with omop.get_cursor() as cursor:

        # prepare the alphabet
        alphabet_d = {}
        for concept_id, concept_name, domain_id, vocabulary_id in cursor(f"""
          SELECT concept_id, concept_name, domain_id, vocabulary_id
          FROM {DATABASE_NAME}.concept"""):
            alphabet_d[concept_id] = (concept_name, domain_id, vocabulary_id)

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
                snomed_lut[src_id] = snomed_id
            elif snomed_lut[src_id] != snomed_id:
                print(f"WARNING: SNOMED_LUT tries to map one id to multiple ids!\n"
                      f"{src_id} -> [{snomed_lut[src_id]},{snomed_id}]\n"
                      f"{src_id} -> {snomed_lut[src_id]} is used.", file=stderr)

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
            for t in concurrent.futures.as_completed(person_threads):
                print(t.result())

    del omop


if __name__ == '__main__':
    main()
