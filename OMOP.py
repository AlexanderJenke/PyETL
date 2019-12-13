import psycopg2 as db


class DB:
    def __init__(self, dbname='OHDSI', user='ohdsi_admin_user', host='localhost', password='omop'):
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' password='{password}'")
        self.cursor = self.conn.cursor()

    def select(self, sql: str):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def insert(self, sql: str):
        self.cursor.execute(sql)
        self.conn.commit()


if __name__ == '__main__':
    db = DB()

    res = db.select("""SELECT  * FROM p21_cdm.concept """)
    print(len(res))

    res = db.insert("""
    INSERT INTO p21_cdm.person (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
    VALUES(0,0,0,0,0)
    """)

    res = db.select("""SELECT  * FROM p21_cdm.person """)
    print(res)

