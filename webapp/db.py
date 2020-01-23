import psycopg2 as db

class DB:

    def __init__(self, dbname='OHDSI', user='ohdsi_admin_user', host='localhost', password='omop'):
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' password='{password}'")
        self.cursor = self.conn.cursor()
