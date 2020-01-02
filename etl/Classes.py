class Person:
    def __init__(self, **meta_d):
        self.meta_d = meta_d

    def add_measurements(self, measurements):
        self.measurements_d = measurements

    def insert_into_db(self):
        keys = ""
        values = ""
        for key, value in self.meta_d.items():
            if key == "location": continue
            keys += key + ","
            values += value + ","
        queries = ()
        queries += f"INSERT INTO p21_cdm.person ({keys[:-1]}) VALUES({values[:-1]})",
        #measurements
        print (len(self.measurements_d))
        for m in self.measurements_d:
            keys = ""
            values = ""
            for key, value in m.items():
                keys += key + ","
                values += value + "," 
            queries += f"INSERT INTO p21_cdm.measurements({keys[:-1]}) VALUES({values[:-1]})",
        return queries



