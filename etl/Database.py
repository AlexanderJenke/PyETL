import psycopg2 as db
from datetime import date
from sys import stderr


class LUT(dict):
    def __init__(self, name, default=None, content={}):
        super().__init__(content)
        self.name = name
        self.default = {'domain_id': "KeyNotFound",
                        'concept_ids': ((-1, date(1970, 1, 1), date(2099, 12, 31)),)} if default is None else default

    def get(self, item, default=None):
        try:
            return self[item]
        except KeyError:
            print(f"WARNING: Key '{item}' not found in {self.name}!", file=stderr)
            return self.default if default is None else default


class OMOP:
    def __init__(self, dbname='OHDSI', user='ohdsi_admin_user', host='localhost', password='omop', do_commits=True):
        self.do_commits = do_commits
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' password='{password}'")
        self.cursor = self.conn.cursor()
        self.preload_lut()
        self.GENDER_LUT = {"m": "8507", "w": "8532", "nan": "8551"}
        self.CONDITION_TYPE_LUT = {"ND": "44786629", "HD": "44786627"}

    def select(self, sql: str):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def insert(self, sql: str):
        self.cursor.execute(sql)
        if self.do_commits:
            self.conn.commit()

    def commit(self):
        self.conn.commit()

    def preload_lut(self):
        """Preloads Look-up Tables for ICD10GM-, OPS- and LOINC-code -> concept_id
        """
        print("Loading look-up tables...", end="")
        self.OPS_LUT = LUT("OPS_LUT")

        for line in self.select("""SELECT DISTINCT concept_id, domain_id, concept_code,valid_start_date, valid_end_date 
                               FROM p21_cdm.concept
                               WHERE vocabulary_id='OPS'"""):
            concept_id, domain_id, concept_code, valid_start_date, valid_end_date = line

            # remove '-' & '.' from concept code
            concept_code = concept_code.replace("-", "")
            concept_code = concept_code.replace(".", "")

            if concept_code not in self.OPS_LUT:
                self.OPS_LUT[concept_code] = {'domain_id': domain_id,
                                              'concept_ids': ((concept_id, valid_start_date, valid_end_date),)}
            else:
                self.OPS_LUT[concept_code]['concept_ids'] += (concept_id, valid_start_date, valid_end_date),

        self.ICD10GM_LUT = LUT("ICD10GM_LUT")
        for line in self.select("""SELECT DISTINCT concept_id, domain_id, concept_code,valid_start_date, valid_end_date 
                               FROM p21_cdm.concept
                               WHERE vocabulary_id='ICD10GM'"""):
            concept_id, domain_id, concept_code, valid_start_date, valid_end_date = line

            if concept_code not in self.ICD10GM_LUT:
                self.ICD10GM_LUT[concept_code] = {'domain_id': domain_id,
                                                  'concept_ids': ((concept_id, valid_start_date, valid_end_date),)}
            else:
                self.ICD10GM_LUT[concept_code]['concept_ids'] += (concept_id, valid_start_date, valid_end_date),

        self.LOINC_LUT = LUT("LOINC_LUT")
        for line in self.select("""SELECT DISTINCT concept_id, domain_id, concept_code,valid_start_date, valid_end_date 
                               FROM p21_cdm.concept
                               WHERE vocabulary_id='LOINC'"""):
            concept_id, domain_id, concept_code, valid_start_date, valid_end_date = line

            if concept_code not in self.LOINC_LUT:
                self.LOINC_LUT[concept_code] = {'domain_id': domain_id,
                                                'concept_ids': ((concept_id, valid_start_date, valid_end_date),)}
            else:
                self.LOINC_LUT[concept_code]['concept_ids'] += (concept_id, valid_start_date, valid_end_date),

        self.ICD10GM2SNOMED = LUT("ICD10GM2SNOMED", default=-1,
                                  content={icd_id: snomed_id for icd_id, snomed_id in self.select("""
                                SELECT r.concept_id_1, r.concept_id_2 FROM p21_cdm.concept_relationship r
                                JOIN p21_cdm.concept c1 ON r.concept_id_1 = c1.concept_id
                                JOIN p21_cdm.concept c2 ON r.concept_id_2 = c2.concept_id
                                WHERE  r.relationship_id='Maps to'
                                AND c2.vocabulary_id='SNOMED'
                                AND c1.vocabulary_id='ICD10GM'""")})
        print("done")


    def get_valid_concept_id(self, LUT, code: str, code_version: int = -1):
        concept_id = None
        concept_ids = LUT.get(code)['concept_ids']

        if code_version is -1:  # no code_version is given
            return concept_ids[0][0]

        # try to get valid id
        for id, start, end in concept_ids:
            if end.year >= code_version >= start.year:
                concept_id = id

        # if no valid id is found
        if concept_id is None:
            # if invalid id exists or no code_version is given
            if len(concept_ids) >= 1:
                concept_id = sorted(concept_ids, key=lambda x: x[1])[0][0]  # select newest id
                print(f"WARNING: {code} has no valid entry for version {code_version} in {LUT.name}!"
                      f" The newest entry was selected (concept_id={concept_id}).", file=stderr)

            # if no id exists
            else:
                raise RuntimeError(f"{code} is not related to any concept_id in {LUT.name}. This should never happen!")

        return concept_id


if __name__ == '__main__':
    """Connection-Test"""
    omop = OMOP()
    tables = omop.select("""select tablename From pg_catalog.pg_tables Where schemaname = 'p21_cdm'""")
    for table in tables:
        print(table[0])
