import psycopg2 as db
from datetime import date
from sys import stderr


class LUT(dict):
    """ A Look up Table to quickly look up mappings
        This is basically a dict but if a key is not found an warning is printed and a default value is returned.
    """
    def __init__(self, name, default=None, content={}):
        """

        :param name: name describing the LUT. Used in warnings to describe where the error occurred
        :param default: default value to be returned if a key is not found
        :param content: content used to initialize the LUT
        """
        super().__init__(content)
        self.name = name
        self.default = {'domain_id': "KeyNotFound",
                        'concept_ids': ((0, date(1970, 1, 1), date(2099, 12, 31)),)} if default is None else default

    def get(self, item, default=None):
        """ returns vaule if key is in LUT, else returns default value
        :param item: key of value to be returned
        :param default: specific default value to be returned if key is not found.
        If not given the default value of the LUT is used
        :return: value of key or default value if key not found
        """
        try:
            return self[item]
        except KeyError:
            print(f"WARNING: Key '{item}' not found in {self.name}!", file=stderr)
            return self.default if default is None else default


class OMOP:
    """ Database connector to the OMOP CDM"""
    def __init__(self, dbname='OHDSI', user='ohdsi_admin_user', host='localhost', port='5432', password='omop',
                 do_commits=False):
        """initializes the database connection including preparation of LUTs"""
        self.do_commits = do_commits
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'")
        self.cursor = self.conn.cursor()
        self.preload_lut()
        self.GENDER_LUT = {"m": "8507", "w": "8532", "nan": "8551", "(null)": "8551"}
        self.CONDITION_TYPE_LUT = {"ND": "44786629", "HD": "44786627"}
        self.VISIT_TYPE_LUT = LUT(default="0",
                                  name="VISIT_TYPE_LUT",
                                  content={"E": "9201",  # Einweisung  -> Inpatient Visit
                                           "V": "9201",  # Verlegung >24h -> Inpatient Visit
                                           "R": "9201",  # Aufnahme  aud Reha -> Inpatient Visit
                                           "A": "9202",  # Verlegung <24h -> Outpatient Visit
                                           "N": "9203",  # Notfall -> Emergency
                                           "nan": "0",  # no reason given
                                           })
        self.LOCALISATION_LUT = LUT(default="0",
                                    name="LOCALISATION_LUT",
                                    content={"L": "4149748",
                                             "R": "4310553",
                                             "B": "0",
                                             }
                                    )

        self.DIAGNOSENSICHERHEIT_LUT = LUT(default="0",
                                           name="DIAGNOSENSICHERHEIT_LUT",
                                           content={"A": "37116688",  # Diagnosis of exclusion
                                                    "V": "4033240",  # Preliminary diagnosis
                                                    "Z": "4284245",  # Asymptomatic diagnosis of
                                                    "G": "4032659",  # Established diagnosis
                                                    }
                                           )

    def select(self, sql: str):
        """ execute SQL SELECT on database
        :param sql: SQL to be executed
        :return: result of SQL SELECT
        """
        self.cursor.execute(sql)
        try:
            res = self.cursor.fetchall()
            return res
        except:
            return [(None,)]

    def insert(self, sql: str):
        """ execute SQL INSERT on database
        :param sql: SQL INSERT to be executed
        :return:
        """
        self.cursor.execute(sql)
        if self.do_commits:
            self.conn.commit()

    def commit(self):
        """ makes transactions on database persistent """
        self.conn.commit()

    def preload_lut(self):
        """Preloads Look-up Tables for ICD10GM-, OPS- and LOINC-code -> concept_id """

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

        self.ICD10GM2SNOMED = LUT("ICD10GM2SNOMED", default=0,
                                  content={icd_id: snomed_id for icd_id, snomed_id in self.select("""
                                SELECT r.concept_id_1, r.concept_id_2 FROM p21_cdm.concept_relationship r
                                JOIN p21_cdm.concept c1 ON r.concept_id_1 = c1.concept_id
                                JOIN p21_cdm.concept c2 ON r.concept_id_2 = c2.concept_id
                                WHERE  r.relationship_id='Maps to'
                                AND c2.vocabulary_id='SNOMED'
                                AND c1.vocabulary_id='ICD10GM'""")})
        print("done")

    def get_valid_concept_id(self, LUT, code: str, code_version: int = -1):
        """ Returns a valid concept id.
        If no valid entry for the given year is found the newest entry is returned.
        If no entry at all for a code is found an error is risen.
        :param LUT: LUT to be used to look the code up
        :param code: code to be translated to concept id
        :param code_version: year where the concept id should be valid
        :return: concept id
        """
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
    """Clear Database 
    On execution of this file and confirmaion by typing 'Yes' (case sensitive!) all entries of the named tables 
    are deleted from the database.
    """
    database = "p21_cdm"
    tables = ["condition_occurrence",
              "fact_relationship",
              "location",
              "measurement",
              "observation",
              "person",
              "procedure_occurrence",
              "visit_occurrence",
              "care_site",
              ]

    if input(f"Do you realy want to clear the Tables {tables} \n"
             f"in the Database '{database}'? \n"
             f"Type 'Yes'.\n") == "Yes":
        omop = OMOP()
        for table in tables:
            omop.insert(f"DELETE FROM {database}.{table}")
        omop.commit()
        print("Database ist now empty again.")
