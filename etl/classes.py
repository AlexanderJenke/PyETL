class Person:
    def __init__(self, **kwargs):
        self.person = kwargs  # -> table: person
        self.visits = []  # -> table: visit_occurrence
        self.measurements = []  # -> table: measurement
        self.observations = []  # -> table: observation
        self.conditions = []  # -> table: condition_occurrence
        self.procedures = []  # -> table: procedure_occurrence
        self.fact_relations = []  # -> table: fact_relationship

    def add_visit(self,
                  visit_occurrence_id,
                  visit_concept_id,
                  visit_start_date,
                  visit_end_date,
                  visit_type_concept_id,
                  **kwargs):
        """ Adds a visit entry to the patients data struct 
        """

        # add all optional values
        visit_d = {key: value for key, value in kwargs.items()}

        # Required values (Except person_id)
        visit_d["visit_occurrence_id"] = visit_occurrence_id
        visit_d["visit_concept_id"] = visit_concept_id
        visit_d["visit_start_date"] = visit_start_date
        visit_d["visit_end_date"] = visit_end_date
        visit_d["visit_type_concept_id"] = visit_type_concept_id

        self.visits.append(visit_d)

    def add_measurement(self,
                        measurement_concept_id,
                        measurement_date,
                        measurement_type_concept_id,
                        **kwargs):
        """ Adds a measurement entry to the patients data struct 
        """

        # add all optional values
        measurement_d = {key: value for key, value in kwargs.items()}

        # Required values (Except measurement_id & person_id)
        measurement_d["measurement_concept_id"] = measurement_concept_id
        measurement_d["measurement_date"] = measurement_date
        measurement_d["measurement_type_concept_id"] = measurement_type_concept_id

        self.measurements.append(measurement_d)
        return measurement_d

    def add_observation(self,
                        observation_concept_id,
                        observation_date,
                        observation_type_concept_id,
                        **kwargs):
        """ Adds a observation entry to the patients data struct
        Does not insert the entry in the excact same data already exists. This mostly affects 'lokalisation' and
        'diagnosensicherheit' entrys beeing doubled due to the existence on a 'sekundär_kode'.
        """

        # add all optional values
        observation_d = {key: value for key, value in kwargs.items()}

        # Required values (Except observation_id & person_id)
        observation_d["observation_concept_id"] = observation_concept_id
        observation_d["observation_date"] = observation_date
        observation_d["observation_type_concept_id"] = observation_type_concept_id

        # do not insert recurrent information, reuse instead
        # especially 'lokalisation' & 'diagnosensicherheit' definitions are affected by this
        if observation_d in self.observations:
            return self.observations[self.observations.index(observation_d)]

        self.observations.append(observation_d)
        return observation_d

    def add_condition(self,
                      condition_concept_id,
                      condition_start_date,
                      condition_start_datetime,
                      condition_type_concept_id,
                      **kwargs):
        """ Adds a condition entry to the patients data struct
        """

        # add all optional values
        condition_d = {key: value for key, value in kwargs.items()}

        # Required values (Except condition_id & person_id)
        condition_d["condition_concept_id"] = condition_concept_id
        condition_d["condition_start_date"] = condition_start_date
        condition_d["condition_start_datetime"] = condition_start_datetime
        condition_d["condition_type_concept_id"] = condition_type_concept_id

        self.conditions.append(condition_d)
        return condition_d

    def add_procedure(self,
                      procedure_concept_id,
                      procedure_date,
                      procedure_datetime,
                      procedure_type_concept_id,
                      **kwargs):
        """ Adds a procedure entry to the patients data struct
        """

        # add all optional values
        procedure_d = {key: value for key, value in kwargs.items()}

        # Required values (Except procedure_id & person_id)
        procedure_d["procedure_concept_id"] = procedure_concept_id
        procedure_d["procedure_date"] = procedure_date
        procedure_d["procedure_datetime"] = procedure_datetime
        procedure_d["procedure_type_concept_id"] = procedure_type_concept_id

        self.procedures.append(procedure_d)
        return procedure_d

    def add_fact_relationship(self, table_from: str, entry_from: dict, table_to: str, entry_to: dict):
        """ Adds a fact relation entry to the patients data struct
        """

        table_lut = {'p': "10",  # procedure
                     'c': "19",  # condition
                     'm': "21",  # measurement
                     'o': "27"}  # observation
        self.fact_relations.append((table_lut[table_from], entry_from, table_lut[table_to], entry_to))

    def insert_into_db(self, database):
        """ Inserts the collected patient data into the database
        """

        # insert person
        keys = ""
        values = ""
        for key, value in self.person.items():
            # location
            if key == "location":
                # ensure location is in table
                database.select(f"""DO $do$ BEGIN IF NOT EXISTS (SELECT * FROM p21_cdm.location WHERE city='{value['city']}' 
                               AND zip='{value['zip']}') THEN INSERT INTO  p21_cdm.location (city, zip) 
                               VALUES ('{value['city']}', '{value['zip']}'); END IF; END; $do$""")
                continue

            keys += f"{key},"
            values += f"'{value}',"

        database.select(f"""INSERT INTO p21_cdm.person (location_id, {keys[:-1]}) 
                            VALUES((SELECT location_id 
                                    FROM p21_cdm.location
                                    WHERE city='{self.person['location']['city']}' 
                                    and zip='{self.person['location']['zip']}'), 
                                   {values[:-1]})""")

        # insert visits
        for visit in self.visits:
            keys = "person_id,"
            values = f"'{self.person['person_id']}',"
            for key, value in visit.items():
                if key == "care_site_name":
                    # ensure care site is in table
                    database.select(f"""DO $do$ BEGIN IF NOT EXISTS (SELECT * 
                                                                FROM p21_cdm.care_site 
                                                                WHERE care_site_name='{value}') 
                                           THEN INSERT INTO  p21_cdm.care_site (care_site_name) 
                                           VALUES ('{value}'); END IF; END; $do$""")
                    continue

                keys += f"{key},"
                values += f"'{value}',"

            database.select(f"""INSERT INTO p21_cdm.visit_occurrence (care_site_id, {keys[:-1]}) 
                                VALUES((SELECT care_site_id
                                        FROM p21_cdm.care_site
                                        WHERE care_site_name='{visit['care_site_name']}'),
                                       {values[:-1]}) 
                                RETURNING visit_occurrence_id""")

        # insert measurements, observations, conditions & procedures
        for data, tablename in [(self.measurements, "measurement"),
                                (self.observations, "observation"),
                                (self.conditions, "condition_occurrence"),
                                (self.procedures, "procedure_occurrence")]:
            for entry in data:
                keys = "person_id,"
                values = f"'{self.person['person_id']}',"

                for key, value in entry.items():
                    keys += f"{key},"
                    values += f"'{value}',"

                entry["sql_id"] = database.select(f"""INSERT INTO p21_cdm.{tablename}({keys[:-1]})
                                                      VALUES({values[:-1]}) RETURNING {tablename}_id""")[0][0]

        # insert fact_relationships in both directions
        for table1, entry1, table2, entry2 in self.fact_relations:
            # 44818890 = Finding associated with (SNOMED)
            database.select(f"""INSERT INTO p21_cdm.fact_relationship(domain_concept_id_1, fact_id_1, 
                                                                     domain_concept_id_2, fact_id_2, 
                                                                     relationship_concept_id)
                                VALUES('{table1}','{entry1['sql_id']}','{table2}','{entry2['sql_id']}','44818890')""")
            # 44818792 = Associated with finding (SNOMED)
            database.select(f"""INSERT INTO p21_cdm.fact_relationship(domain_concept_id_1, fact_id_1, 
                                                                      domain_concept_id_2, fact_id_2, 
                                                                      relationship_concept_id)
                                VALUES('{table2}','{entry2['sql_id']}','{table1}','{entry1['sql_id']}','44818792')""")

        # make transactions persistent
        database.commit()
