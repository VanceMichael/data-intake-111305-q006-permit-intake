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
            page = parse_page(client.fetch("/feed").body)
            self.assertEqual(len(page.forms), 2)
            self.assertEqual(page.forms[0]["fields"]["id"], records[0]["application"])
            self.assertEqual(records[0]["status"], "draft")
            self.assertEqual(records[1]["status"], "accepted")
            self.assertIsNotNone(records[1]["receipt"])

    def test_every_record_has_the_declared_source_identity(self):
        scenario = load_scenario()
        for record in scenario["source_records"]:
            for field in scenario["identity_fields"]:
                self.assertIn(field, record)
                self.assertNotEqual(str(record[field]).strip(), "")


if __name__ == "__main__":
    unittest.main()
