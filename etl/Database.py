import psycopg2 as db


class OMOP:
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
    """Connection-Test"""
    omop = OMOP()
    tables = omop.select("""select tablename From pg_catalog.pg_tables Where schemaname = 'p21_cdm'""")
    for table in tables:
        print(table[0])
