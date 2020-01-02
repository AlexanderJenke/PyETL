import sqlite3

class ConfigHandler:

    def __init__(self):
        self.conn = sqlite3.connect("config.db")
        self.conn.execute("CREATE TABLE IF NOT EXISTS config(key varchar(255) PRIMARY KEY NOT NULL, value varchar(255))")
        query = "SELECT value FROM config WHERE key = 'interval'"
        cursor = self.conn.execute(query)
        if len(cursor.fetchall()) == 0:
            self.conn.execute("INSERT INTO config VALUES('interval', '* * * * *')")
        
        self.conn.execute("CREATE TABLE IF NOT EXISTS csv_row_count(csv_file varchar(255) PRIMARY KEY NOT NULL, row_count int)")

        cursor = self.conn.execute("SELECT row_count FROM csv_row_count WHERE csv_file = 'FALL.csv'")
        if len(cursor.fetchall()) == 0:
            self.conn.execute("INSERT INTO csv_row_count VALUES('FALL.csv', 0)")
        cursor = self.conn.execute("SELECT row_count FROM csv_row_count WHERE csv_file = 'FAB.csv'")
        if len(cursor.fetchall()) == 0:
            self.conn.execute("INSERT INTO csv_row_count VALUES('FAB.csv', 0)")
        cursor = self.conn.execute("SELECT row_count FROM csv_row_count WHERE csv_file = 'ENTGELTE.csv'")
        if len(cursor.fetchall()) == 0:
            self.conn.execute("INSERT INTO csv_row_count VALUES('ENTGELTE.csv', 0)")
        cursor = self.conn.execute("SELECT row_count FROM csv_row_count WHERE csv_file = 'ICD.csv'")
        if len(cursor.fetchall()) == 0:
            self.conn.execute("INSERT INTO csv_row_count VALUES('ICD.csv', 0)")
        cursor = self.conn.execute("SELECT row_count FROM csv_row_count WHERE csv_file = 'LABOR.csv'")
        if len(cursor.fetchall()) == 0:
            self.conn.execute("INSERT INTO csv_row_count VALUES('LABOR.csv', 0)")
        cursor = self.conn.execute("SELECT row_count FROM csv_row_count WHERE csv_file = 'MESSUNGEN.csv'")
        if len(cursor.fetchall()) == 0:
            self.conn.execute("INSERT INTO csv_row_count VALUES('MESSUNGEN.csv', 0)")
        cursor = self.conn.execute("SELECT row_count FROM csv_row_count WHERE csv_file = 'OPS.csv'")
        if len(cursor.fetchall()) == 0:
            self.conn.execute("INSERT INTO csv_row_count VALUES('OPS.csv', 0)")

        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'postgres_port'")
        if (len(cursor.fetchall()) == 0):
            self.conn.execute("INSERT INTO config VALUES('postgres_port', 5000)")

        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'postgres_host'")
        if (len(cursor.fetchall()) == 0):
            self.conn.execute("INSERT INTO config VALUES('postgres_host', 'localhost')")

        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'username'")
        if (len(cursor.fetchall()) == 0):
            self.conn.execute("INSERT INTO config VALUES('username', 'med')")

        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'password'")
        if (len(cursor.fetchall()) == 0):
            self.conn.execute("INSERT INTO config VALUES('password', '1')")
        
        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'csv_directory'")
        if (len(cursor.fetchall()) == 0):
            self.conn.execute("INSERT INTO config VALUES('csv_directory', '/home/markus/uni/medizinische Informatik/Daten/20191121')")
        self.conn.commit()
    
    def update_interval(self, interval):
        update = "UPDATE config SET value = '" + interval + "' WHERE key= 'interval'"
        self.conn.execute(update)
        self.interval = interval

        self.conn.commit()

    def update_row_count(self, csv_file, row_count):
        self.conn.execute("UPDATE csv_row_count SET row_count ='" + row_count + "' WHERE csv_file = '" + csv_file + "'")
        self.conn.commit()

    def get_row_count(self, csv_file):
        cursor = self.conn.execute("SELECT row_count FROM csv_row_count WHERE csv_file = '" + csv_file + "'")
        self.conn.commit()

    def get_interval(self):
        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'interval'")
        for value in cursor:
            return value[0]

    def get_password(self):
        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'password'")
        for value in cursor:
            return value[0]

    def get_username(self):
        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'username'")
        for value in cursor:
            return value[0]

    def get_host(self):
        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'postgres_host'")
        for value in cursor:
            return value[0]

    def get_port(self):
        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'postgres_port'")
        for value in cursor:
            return value[0]


    def get_csv_dir(self):
        cursor = self.conn.execute("SELECT value FROM config WHERE key = 'csv_directory'")
        for value in cursor:
            return value[0]
