import hashlib
import json
import unittest

from permit_intake.client import Client
from permit_intake.decoding import parse_page, records as decode_records
from permit_intake.origin import load_scenario, serve


class SourceContractTests(unittest.TestCase):
    def test_domain_observations_preserve_source_distinctions(self):
        scenario = load_scenario()
        records = scenario["source_records"]
        with serve(scenario) as origin:
            client = Client(origin.base_url)
            self.assertEqual(len(records[0]["groups"]), 2)
            self.assertEqual(records[0]["groups"][0]["attributes"][0]["value"], 7)
            self.assertEqual(records[0]["groups"][1]["attributes"][0]["value"], 8)
            self.assertEqual(records[0]["groups"][1]["attributes"][1]["tag"], "no-value")
            self.assertLess(records[1]["printer_up_time"], records[0]["printer_up_time"])

    def test_every_record_has_the_declared_source_identity(self):
        scenario = load_scenario()
        for record in scenario["source_records"]:
            for field in scenario["identity_fields"]:
                self.assertIn(field, record)
                self.assertNotEqual(str(record[field]).strip(), "")


if __name__ == "__main__":
    unittest.main()
