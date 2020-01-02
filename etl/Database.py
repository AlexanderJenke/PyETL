import psycopg2 as db


class OMOP:
    def __init__(self, dbname='OHDSI', user='ohdsi_admin_user', host='localhost', password='omop', do_commits=True):
        self.do_commits = do_commits
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' password='{password}'")
        self.cursor = self.conn.cursor()
        self.preload_lut()
        self.GENDER_LUT = {"m": "8507", "w": "8532", "nan": "8551"}

    def select(self, sql: str):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def insert(self, sql: str):
        self.cursor.execute(sql)
        if self.do_commits:
            self.conn.commit()

    def preload_lut(self):
        """Preloads Look-up Tables for ICD10GM-, OPS- and LOINC-code -> concept_id
        """
        self.OPS_LUT = {}
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

        self.ICD10GM_LUT = {}
        for line in self.select("""SELECT DISTINCT concept_id, domain_id, concept_code,valid_start_date, valid_end_date 
                                   FROM p21_cdm.concept
                                   WHERE vocabulary_id='ICD10GM'"""):
            concept_id, domain_id, concept_code, valid_start_date, valid_end_date = line

            if concept_code not in self.ICD10GM_LUT:
                self.ICD10GM_LUT[concept_code] = {'domain_id': domain_id,
                                                  'concept_ids': ((concept_id, valid_start_date, valid_end_date),)}
            else:
                self.ICD10GM_LUT[concept_code]['concept_ids'] += (concept_id, valid_start_date, valid_end_date),

        self.LOINC_LUT = {}
        for line in self.select("""SELECT DISTINCT concept_id, domain_id, concept_code,valid_start_date, valid_end_date 
                                   FROM p21_cdm.concept
                                   WHERE vocabulary_id='LOINC'"""):
            concept_id, domain_id, concept_code, valid_start_date, valid_end_date = line

            if concept_code not in self.LOINC_LUT:
                self.LOINC_LUT[concept_code] = {'domain_id': domain_id,
                                                'concept_ids': ((concept_id, valid_start_date, valid_end_date),)}
            else:
                self.LOINC_LUT[concept_code]['concept_ids'] += (concept_id, valid_start_date, valid_end_date),

        self.ICD10GM2SNOMED = {icd_id: snomed_id for icd_id, snomed_id in self.select("""
                SELECT r.concept_id_1, r.concept_id_2 FROM p21_cdm.concept_relationship r
                JOIN p21_cdm.concept c1 ON r.concept_id_1 = c1.concept_id
                JOIN p21_cdm.concept c2 ON r.concept_id_2 = c2.concept_id
                WHERE  r.relationship_id='Maps to'
                AND c2.vocabulary_id='SNOMED'
                AND c1.vocabulary_id='ICD10GM'""")}


if __name__ == '__main__':
    """Connection-Test"""
    omop = OMOP()
    tables = omop.select("""select tablename From pg_catalog.pg_tables Where schemaname = 'p21_cdm'""")
    for table in tables:
        print(table[0])
