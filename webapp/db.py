import psycopg2 as db

class DB:

    def __init__(self, dbname='OHDSI', user='ohdsi_admin_user', host='localhost', port=5432 , password='omop'):
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'")
        print(user)
        self.cursor = self.conn.cursor()

    def get_patients_with_reasons(self):
        query = "SELECT p.patient_id, p.day_of_birth, p.estimation, d.diagnosis FROM ml_res.patient p JOIN ml_res.diagnosis d ON p.patient_id = d.patient_id;"
        self.cursor.execute(query)
        data = {}
        for row in self.cursor.fetchall():
            if row[0] not in data:
                data[row[0]] = (row[1], row[2], [row[3]])
            else:
                data[row[0]][2].append(row[3])
        return data
