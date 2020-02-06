import psycopg2 as db

class DB:

    def __init__(self, dbname='ML_RESULTS', user='ml', host='localhost', port=5432 , password='1234'):
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'")
        print(user)
        self.cursor = self.conn.cursor()

    def get_patients_with_reasons(self):
        query = "SELECT p.patient_id, p.birthday, p.gender, p.prediction, d.reason FROM results.PATIENT p LEFT JOIN results.reason d ON p.patient_id = d.patient_id;"
        self.cursor.execute(query)
        data = {}
        for row in self.cursor.fetchall():
            if row[0] not in data:
                data[row[0]] = (row[1], row[2], row[3], [])
            if not (row[4] == None):
                data[row[0]][3].append(row[4])
        print(data)
        return data
