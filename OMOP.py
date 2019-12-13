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

    def availabe_tables(self):
        return [t[0] for t in
                self.select("""select tablename From pg_catalog.pg_tables Where schemaname = 'p21_cdm'""")]

    def availabel_colums(self, table):
        return self.select(
            """select column_name,data_type  from information_schema.columns  where table_name = '""" + table + """'""")

    def __getattr__(self, item):
        return "p21_cdm." + str(item)


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
