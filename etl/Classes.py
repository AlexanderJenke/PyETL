class Person:
    def __init__(self, **kwargs):
        self.person = kwargs  # -> table: person
        self.visits = []  # -> table: visit_occurrence
        self.measurements = []  # -> table: measurement
        self.observations = []  # -> table: observation
        self.conditions = []  # -> table: condition_occurrence
        self.procedures = []  # -> table: procedure_occurrence

    def add_visit(self,
                  visit_occurrence_id,
                  visit_concept_id,
                  visit_start_date,
                  visit_end_date,
                  visit_type_concept_id,
                  **kwargs):

        # Optional Values
        visit_d = {key: value for key, value in kwargs.items()}

        # Required Values (Except person_id)
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

        # Optional Values
        measurement_d = {key: value for key, value in kwargs.items()}

        # Required Values (Except measurement_id & person_id)
        measurement_d["measurement_concept_id"] = measurement_concept_id
        measurement_d["measurement_date"] = measurement_date
        measurement_d["measurement_type_concept_id"] = measurement_type_concept_id

        self.measurements.append(measurement_d)

    def add_observation(self,
                        observation_concept_id,
                        observation_date,
                        observation_type_concept_id,
                        **kwargs):

        # Optional Values
        observation_d = {key: value for key, value in kwargs.items()}

        # Required Values (Except observation_id & person_id)
        observation_d["observation_concept_id"] = observation_concept_id
        observation_d["observation_date"] = observation_date
        observation_d["observation_type_concept_id"] = observation_type_concept_id

        self.observations.append(observation_d)

    def add_condition(self,
                      condition_concept_id,
                      condition_start_date,
                      condition_start_datetime,
                      condition_type_concept_id,
                      **kwargs):

        # Optional Values
        condition_d = {key: value for key, value in kwargs.items()}

        # Required Values (Except condition_id & person_id)
        condition_d["condition_concept_id"] = condition_concept_id
        condition_d["condition_start_date"] = condition_start_date
        condition_d["condition_start_datetime"] = condition_start_datetime
        condition_d["condition_type_concept_id"] = condition_type_concept_id

        self.conditions.append(condition_d)

    def add_procedure(self,
                      procedure_concept_id,
                      procedure_date,
                      procedure_datetime,
                      procedure_type_concept_id,
                      **kwargs):
        # Optional Values
        procedure_d = {key: value for key, value in kwargs.items()}

        # Required Values (Except procedure_id & person_id)
        procedure_d["procedure_concept_id"] = procedure_concept_id
        procedure_d["procedure_date"] = procedure_date
        procedure_d["procedure_datetime"] = procedure_datetime
        procedure_d["procedure_type_concept_id"] = procedure_type_concept_id

        self.procedures.append(procedure_d)

    def insert_into_db(self):
        queries = ()

        # person
        keys = ""
        values = ""
        for key, value in self.person.items():
            if key == "location": continue
            keys += f"{key},"
            values += f"'{value}',"
        queries += f"INSERT INTO p21_cdm.person ({keys[:-1]}) VALUES({values[:-1]})",

        # TODO Add location

        for data, tablename in [(self.measurements, "measurement"),
                                (self.observations, "observation"),
                                (self.conditions, "condition_occurrence"),
                                (self.procedures, "procedure_occurrence"),
                                (self.visits, "visit_occurrence")]:
            # print(len(data), tablename)  # TODO Remove later
            for m in data:
                keys = "person_id,"
                values = f"'{self.person['person_id']}',"

                for key, value in m.items():
                    keys += f"{key},"
                    values += f"'{value}',"

                queries += f"INSERT INTO p21_cdm.{tablename}({keys[:-1]}) VALUES({values[:-1]})",

        return queries
