import psycopg2 as db

class DB:

    def __init__(self, dbname='results', user='ohdsi_admin_user', host='localhost', port=5432 , password='omop'):
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'")
        self.cursor = self.conn.cursor()

    def get_patients_with_reasons():
        query = "SELECT p.patient_id, p.estimation, d.diagnosis FROM patients p JOIN diagnoses d ON p.patient_id = d.patient_id;"
        self.cursor.execute(query)
        return self.cursor.fetchall()
