import torch
import pickle
import os
import numpy as np
import psycopg2 as db
from sys import stderr
from datetime import date
from tqdm import tqdm

DATABASE_NAME = "synpuf_cdm"  # "p21_cdm"


class DB_Connector:
    def __init__(self,
                 dbname='OHDSI',
                 user='ohdsi_admin_user',
                 host='localhost',
                 port='5432',
                 password='omop'):
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'")
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()

    def __call__(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()


class NewestOMOP:
    def __init__(self, disease_lut_file):

        def get_table(tablename: str, colum_map: dict, condition=1):
            cols = ""
            for col in sorted(colum_map.keys()):
                cols += f"{col},"

            rows = omop(f""" SELECT {cols[:-1]} FROM {DATABASE_NAME}.{tablename} WHERE {condition}""")
            colum_names = [v for k, v in sorted(colum_map.items(), key=lambda x: x[0])]
            return [{colum_names[i]: v for i, v in enumerate(row)} for row in rows]

        with open(disease_lut_file, 'rb') as file:
            self.disease_lut = pickle.load(file)

        with DB_Connector() as omop:

            # prepare the alphabet
            alphabet_d = {}
            for concept_id, concept_name, domain_id, vocabulary_id in omop(f"""
                     SELECT concept_id, concept_name, domain_id, vocabulary_id
                     FROM {DATABASE_NAME}.concept"""):
                alphabet_d[str(concept_id)] = (concept_name, domain_id, vocabulary_id)

            alphabet_d["age"] = ("the patients age", "proprietary", "proprietary")
            alphabet_d["male"] = ("the patient is male", "proprietary", "proprietary")
            alphabet_d["female"] = ("the patient is female", "proprietary", "proprietary")

            # prepare snomed_lut
            snomed_lut = {}
            for src_id, snomed_id in omop(f"""SELECT r.concept_id_1, c.concept_id
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

            self.snomed_lut = snomed_lut

            person_ids = omop(f"select distinct person_id from {DATABASE_NAME}.person")

            self.data = []

            for pid, *args in tqdm(person_ids, desc="Loding patient data"):
                gender_concept_id, year_of_birth = omop(f"""SELECT gender_concept_id, year_of_birth 
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

                concept_ids = sorted(set([snomed_lut[str(row['id'])] for row in data]))
                concept_id_lut = {id: i for i, id in enumerate(concept_ids)}
                dates = sorted(set([row['date'] for row in data]))

                sample = {}

                # entrys -> patient data
                for row in data:
                    concept_id = snomed_lut[str(row['id'])]

                    if row['value'] is None:
                        value = 1.0
                    else:
                        try:
                            value = float(row['value'])
                        except ValueError:
                            print(f"ERROR: {row['value']} can not be converted into float! \n{row}")
                            continue

                    if 'end_date' in row and row['end_date'] is not None and row['end_date'] < dates[-1]:
                        continue

                    sample[concept_id] = value

                sample["age"] = age
                sample["female"] = int(gender_concept_id == 8532)
                sample["male"] = int(gender_concept_id == 8507)

                ids = sorted([self.disease_lut[i]['cid'] for i in self.disease_lut])

                feature = [sample[i] if i in sample else 0 for i in ids]

                self.data.append((pid, feature))

        """
        ids = sorted(set(
            np.concatenate([[k for k in p[0].keys() if p[0][k] != 0] for p in test_patients + train_patients if p[1]])))

        self.disease_lut = {i: self.alphabet[j][0] for i, j in enumerate(ids)}

        self.test_data = []
        self.train_data = []

        for feature_d, label in test_patients:
            feature_l = [feature_d[id] if id in feature_d else 0 for i, id in enumerate(ids)]
            self.test_data.append((feature_l, label))

        for feature_d, label in train_patients:
            feature_l = [feature_d[id] if id in feature_d else 0 for i, id in enumerate(ids)]
            self.train_data.append((feature_l, label))
        """

    def __getitem__(self, item):
        return torch.tensor(self.data[item][0],
                            dtype=torch.float32), torch.tensor(self.data[item][1],
                                                               dtype=torch.float32)

    def __len__(self):
        return len(self.data)


if __name__ == '__main__':
    dss = NewestOMOP("../output/dataset_pos_f5/disease_lut.pkl")
