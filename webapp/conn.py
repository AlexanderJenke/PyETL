import psycopg2 as db

class AbstractDB:

    def __init__(self, dbname="OHDSI", user="ohdsi_admin_user", host="localhost", port="5432", password="omop", do_commits=False):
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port='{port}'")
        self.cursor = self.conn.cursor()
        self.do_commits = do_commits

class DBInitializer(AbstractDB):

    def __init__(self): 
        super(DBInitializer, self).__init__()

    def create_table(self):
        query = "CREATE TABLE IF NOT EXISTS reasons(person_id int, reason varchar(255))"
        self.cursor.execute(query)
        insert = "INSERT INTO reasons VALUES(22, 'uses pentaho')"
        self.cursor.execute(insert)

class DBReceiver(AbstractDB):
    def __init__(self):
        super(DBReceiver, self).__init__()

    def receive_reasons(self):
        patients = {}
        query = "SELECT * FROM reasons"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            if (row[0], True) in patients:
                patients[(row[0], True)].append(row[1])
            else:
                patients[(row[0], True)] = [row[1]]
        query = "SELECT DISTINCT person_id FROM p21_cdm.Person"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            if not (row[0], True) in patients:
                patients[(row[0], False)] = []
        return patients


    def create_table(self):
        query = "CREATE TABLE IF NOT EXISTS reasons(person_id int, reason varchar(255))"
        self.cursor.execute(query)
        insert = "INSERT INTO reasons VALUES(22, 'uses pentaho')"
        self.cursor.execute(insert)
