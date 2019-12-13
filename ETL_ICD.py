import OMOP
import csv_reader

db = OMOP.DB()
data = csv_reader.InEK_File("Data/ICD.csv")


sel_c = ['kh_internes_kennzeichen',
         'icd_kode',
         'icd_version',
         ]


rel = db.select("""
    SELECT distinct t1.concept_code, t2.concept_id_2 as snomed
    FROM p21_cdm.concept t1
    JOIN p21_cdm.concept_relationship t2
    ON t1.concept_id = t2.concept_id_1
    JOIN p21_cdm.concept t3
    ON t3.concept_id = t2.concept_id_2
    WHERE t1.vocabulary_id = 'ICD10GM' and t3.vocabulary_id = 'SNOMED' and t2.relationship_id='Maps to' """)

IDC10GM_SNOMED = {s[0]: s[1] for s in rel}

print(rel)

for row in data.get_colums(sel_c):
    snomed_id = IDC10GM_SNOMED[row['icd_kode']]
    print(row, snomed_id)