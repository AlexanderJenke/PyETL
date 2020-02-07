import unittest
from classes import Person

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.person = Person()

    def test_add_visit(self):
        visit_occurrence_id = 1
        visit_concept_id = 2
        visit_start_date = "31.10.2019"
        visit_end_date = "01.01.2020"
        visit_type_concept_id = 4
        self.person.add_visit(visit_occurrence_id, visit_concept_id, visit_start_date, visit_end_date, visit_type_concept_id)
        visit_d = {}
        visit_d["visit_occurrence_id"] = 1
        visit_d["visit_concept_id"] = 2
        visit_d["visit_start_date"] = "31.10.2019"
        visit_d["visit_end_date"] = "01.01.2020"
        visit_d["visit_type_concept_id"] = 4
        for key in visit_d:
            self.assertEqual(visit_d[key], self.person.visits[0][key])
        self.assertEqual(1, len(self.person.measurements))

    def test_add_measurement(self):
        measurement_concept_id = 2
        measurement_date = "31.10.2019"
        measurement_type_concept_id = 4
        self.person.add_measurement(measurement_concept_id, measurement_date, measurement_type_concept_id)
        measurement_d = {}
        measurement_d["measurement_concept_id"] = 2
        measurement_d["measurement_date"] = "31.10.2019"
        measurement_d["measurement_type_concept_id"] = 4
        for key in measurement_d:
            self.assertEqual(measurement_d[key], self.person.measurements[0][key])
        self.assertEqual(1, len(self.person.measurements))


    def test_add_observation(self):
        observation_concept_id = 2
        observation_date = "31.10.2019"
        observation_type_concept_id = 4
        self.person.add_observation(observation_concept_id, observation_date, observation_type_concept_id)
        observation_d = {}
        observation_d["observation_concept_id"] = 2
        observation_d["observation_date"] = "31.10.2019"
        observation_d["observation_type_concept_id"] = 4
        for key in observation_d:
            self.assertEqual(observation_d[key], self.person.observations[0][key])
        self.assertEqual(1, len(self.person.observations))


    def test_add_condition(self):
        condition_concept_id = 2
        condition_start_date = "31.10.2019"
        condition_start_datetime = "31.10.2019:00:00"
        condition_type_concept_id = 4
        self.person.add_condition(condition_concept_id, condition_start_date, condition_start_datetime, condition_type_concept_id)
        condition_d = {}
        condition_d["condition_concept_id"] = 2
        condition_d["condition_start_date"] = "31.10.2019"
        condition_d["condition_start_datetime"] = "31.10.2019:00:00"
        condition_d["condition_type_concept_id"] = 4
        for key in condition_d:
            self.assertEqual(condition_d[key], self.person.conditions[0][key])
        self.assertEqual(1, len(self.person.conditions))


    def test_add_procedure(self):
        procedure_concept_id = 2
        procedure_date = "31.10.2019"
        procedure_datetime = "31.10.2019:00:00"
        procedure_type_concept_id = 4
        self.person.add_procedure(procedure_concept_id, procedure_date, procedure_datetime, procedure_type_concept_id)
        procedure_d = {}
        procedure_d["procedure_concept_id"] = 2
        procedure_d["procedure_date"] = "31.10.2019"
        procedure_d["procedure_datetime"] = "31.10.2019:00:00"
        procedure_d["procedure_type_concept_id"] = 4
        for key in procedure_d:
            self.assertEqual(procedure_d[key], self.person.procedures[0][key])
        self.assertEqual(1, len(self.person.procedures))

    def test_add_fact_relationship(self):
        self.person.observations = []
        observation_concept_id = 2
        observation_date = "31.10.2019"
        observation_type_concept_id = 4
        self.person.add_observation(observation_concept_id, observation_date, observation_type_concept_id)
        observation_d = {}
        observation_d["observation_concept_id"] = 2
        observation_d["observation_date"] = "31.10.2019"
        observation_d["observation_type_concept_id"] = 4

        self.person.add_fact_relationship("o", self.person.observations[0] , "o", self.person.observations[0] )
        self.assertEqual("27", self.person.fact_relations[0][0])
        self.assertEqual("27", self.person.fact_relations[0][2])
        for key in observation_d:
            self.assertEqual(observation_d[key], self.person.fact_relations[0][1][key])
            self.assertEqual(observation_d[key], self.person.fact_relations[0][3][key])
    

if __name__ == "__main__":
    unittest.main()
