'''# -*- coding: future_fstrings -*-'''

import psycopg2 as db
import torch
import pickle
from sys import stderr
from tqdm import tqdm
from datetime import date, timedelta
import numpy as np

RELEVANT_DOMAIN_IDS = ["Observation", "Procedure", "Condition", "Measurement"]
DATABASE_NAME = "synpuf_cdm"


class OMOP_Base(torch.utils.data.Dataset):
    def __init__(self, data, path=None):
        if path is None:
            self.data = self._prepare_data(data)
        else:
            with open(path, "rb") as f:
                d = pickle.load(f)
                if "samples" in d:
                    self.data = d["samples"]
                    self.features_lut = d["features_lut"]
                    self.alphabet = d["alphabet"]
                else:
                    self.data = d

    def n_pos(self):
        return sum([s[1] for s in self.data])

    def _prepare_data(self, data_d: dict):
        print("preparing data...")
        data = []

        patient_data = data_d['data']
        alphabet = data_d['alphabet']
        self.alphabet = alphabet

        labels = set()
        print("Lables:")
        for id, t in alphabet.items():
            name, domain, vocab = t
            if "Pressure ulcer" in name:
                labels.add(id)
                print(f"    {name}")

        features_map = sorted(list(set(np.concatenate(
            [list(patient['datapoints_d'].keys())
             for patient in patient_data if len(patient['datapoints_d'])
             if bool(set(patient['datapoints_d'].keys()) & labels)
             ]))))

        features_lut = {key: features_map.index(key) + 3 for key in features_map}
        self.features_lut = features_lut

        print(f"N_features: {len(features_map) + 3}")

        for patient in tqdm(patient_data, desc="generating samples"):
            timeline = {}
            for concept_id in patient['datapoints_d']:
                for datapoint in patient['datapoints_d'][concept_id]:
                    timestamp = (datapoint['date'] - date(1970, 1, 1)) / timedelta(seconds=1)
                    if timestamp not in timeline:
                        timeline[timestamp] = []
                    datapoint["concept_id"] = concept_id
                    timeline[timestamp].append(datapoint)

            timestamps = sorted(list(timeline.keys()))
            for i in range(len(timestamps) - 1):
                label = bool(set(d['concept_id'] for d in timeline[timestamps[i + 1]]) & labels)
                features = np.zeros(len(features_map) + 3)
                features[0] = int(patient['gender_id'] == 8507)  # is male?
                features[1] = int(patient['gender_id'] == 8532)  # is female?
                features[2] = date.today().year - patient['year_of_birth']  # age
                for j in range(i + 1):
                    for datapoint in timeline[timestamps[i + 1]]:
                        if datapoint['concept_id'] not in features_map:
                            continue
                        if "enddate" in datapoint and \
                                (datapoint['enddate'] - date(1970, 1, 1)) / timedelta(seconds=1) < timestamps[i]:
                            continue
                        if datapoint['value'] is None:
                            features[features_lut[datapoint['concept_id']]] = 1
                        else:
                            features[features_lut[datapoint['concept_id']]] = datapoint['value']
                data.append((features, label))
        return data

    def __getitem__(self, item):
        return torch.FloatTensor(self.data[item][0]), torch.FloatTensor([self.data[item][1]])  # features, label

    def __len__(self):
        return len(self.data)

    def save_sample_data(self, path):
        print(f"saving data to {path}")
        obj = {"samples": self.data,
               "features_lut": self.features_lut,
               "alphabet": self.alphabet}
        with open(path, 'wb') as file:
            pickle.dump(obj, file)


class OMOP_DB(OMOP_Base):
    def __init__(self,
                 dbname='OHDSI',
                 user='ohdsi_admin_user',
                 host='localhost',
                 port='5432',
                 password='omop',
                 path=None):
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'")
        self.cursor = self.conn.cursor()
        self.db_data = self._load_db()
        if path is not None:
            # save db data to file
            self.save_db_data(path)

        super().__init__(self.db_data)

    def _load_db(self):
        print(f"loading data from database ({self.conn.dsn})")

        patient_data = []

        alphabet_d = {}
        for concept_id, concept_name, domain_id, vocabulary_id in self._select(f"""SELECT concept_id, concept_name, domain_id, vocabulary_id
                                                                                   FROM {DATABASE_NAME}.concept"""):
            alphabet_d[concept_id] = (concept_name, domain_id, vocabulary_id)

        # prepare snomed_lut
        snomed_lut = {}
        for src_id, snomed_id in self._select(f"""SELECT r.concept_id_1, c.concept_id
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

        # get patients
        persons = self._select(f"""SELECT person_id, gender_concept_id, year_of_birth
                                   FROM {DATABASE_NAME}.person""")

        # fill patient data
        for person_id, gender_id, year_of_birth in tqdm(persons):
            person_d = {"person_id": person_id,
                        "gender_id": gender_id,
                        "year_of_birth": year_of_birth,
                        "datapoints_d": {}}

            if "Observation" in RELEVANT_DOMAIN_IDS:
                for concept_id, date, value in self._select(f"""SELECT observation_concept_id, observation_date, value_as_number
                                                                FROM {DATABASE_NAME}.observation
                                                                WHERE person_id='{person_id}'"""):
                    try:
                        concept_id = snomed_lut[concept_id]
                    except KeyError:
                        pass
                    if concept_id not in person_d["datapoints_d"]:
                        person_d["datapoints_d"][concept_id] = []
                    person_d["datapoints_d"][concept_id].append({"date": date, "value": value})

            if "Measurement" in RELEVANT_DOMAIN_IDS:
                for concept_id, date, value in self._select(f"""SELECT measurement_concept_id, measurement_date, value_as_number
                                                                FROM {DATABASE_NAME}.measurement
                                                                WHERE person_id='{person_id}'"""):
                    try:
                        concept_id = snomed_lut[concept_id]
                    except KeyError:
                        pass
                    if concept_id not in person_d["datapoints_d"]:
                        person_d["datapoints_d"][concept_id] = []
                    person_d["datapoints_d"][concept_id].append({"date": date, "value": value})

            if "Condition" in RELEVANT_DOMAIN_IDS:
                for concept_id, startdate, enddate in self._select(f"""SELECT condition_concept_id, condition_start_date, condition_end_date
                                                                FROM {DATABASE_NAME}.condition_occurrence
                                                                WHERE person_id='{person_id}'"""):
                    try:
                        concept_id = snomed_lut[concept_id]
                    except KeyError:
                        pass
                    if concept_id not in person_d["datapoints_d"]:
                        person_d["datapoints_d"][concept_id] = []
                    person_d["datapoints_d"][concept_id].append({"date": startdate, "enddate": enddate, "value": None})

            if "Procedure" in RELEVANT_DOMAIN_IDS:
                for concept_id, date, quantity in self._select(f"""SELECT procedure_concept_id, procedure_date, quantity
                                                                FROM {DATABASE_NAME}.procedure_occurrence
                                                                WHERE person_id='{person_id}'"""):
                    try:
                        concept_id = snomed_lut[concept_id]
                    except KeyError:
                        pass
                    if concept_id not in person_d["datapoints_d"]:
                        person_d["datapoints_d"][concept_id] = []
                    person_d["datapoints_d"][concept_id].append({"date": date, "value": quantity})

            patient_data.append(person_d)

        return {"data": patient_data, "alphabet": alphabet_d}

    def _select(self, sql: str):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def save_db_data(self, path):
        print(f"saving data to {path}")
        with open(path, 'wb') as file:
            pickle.dump(self.db_data, file)


class OMOP_File(OMOP_Base):
    def __init__(self, path):
        super().__init__(self._load_file(path))

    @staticmethod
    def _load_file(path):
        print(f"loading data from {path}")
        with open(path, 'rb') as file:
            return pickle.load(file)


if __name__ == '__main__':
    # OMOP_DB(path=f"{DATABASE_NAME}.db.pkl")
    OMOP_File(f"{DATABASE_NAME}.db.pkl").save_sample_data(f"{DATABASE_NAME}.reduced_samples.pkl")
    # ds = OMOP_Base(None, path=f"{DATABASE_NAME}.reduced_samples.pkl")
    # print(f"{DATABASE_NAME}.samples.pkl")
    # print(f"n_samples: {len(ds)}")
    # print(f"n_positiv: {ds.n_pos()}")
