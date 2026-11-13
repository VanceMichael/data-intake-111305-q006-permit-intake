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
            schema = json.loads(client.fetch("/wizard/schema").body)
            self.assertNotEqual(schema["steps"]["materials-water"]["required"], schema["steps"]["materials-field"]["required"])
            remote = json.loads(client.fetch("/wizard/draft/APP-41").body)
            self.assertGreater(remote["revision"], records[0]["draft_revision"])
            self.assertNotEqual(remote["fields"]["licence_type"], records[0]["fields"]["licence_type"])
            from urllib.request import Request, urlopen
            from urllib.parse import urlencode
            from urllib.error import HTTPError
            fields = {"application": "APP-41", "revision": remote["revision"], "csrf": "form-2026-b", "step": "materials-water", "fields": "{}"}
            with self.assertRaises(HTTPError) as caught:
                urlopen(Request(origin.base_url + "/wizard/save-step", data=urlencode(fields).encode()), timeout=2)
            self.assertEqual(caught.exception.code, 422)
            fields.update({"step": "qualification", "fields": json.dumps({"company": "确认名称"})})
            with urlopen(Request(origin.base_url + "/wizard/save-step", data=urlencode(fields).encode()), timeout=2) as response:
                self.assertEqual(json.load(response)["revision"], remote["revision"] + 1)
            with self.assertRaises(HTTPError) as caught:
                urlopen(Request(origin.base_url + "/wizard/save-step", data=urlencode(fields).encode()), timeout=2)
            self.assertEqual(caught.exception.code, 409)

    def test_every_record_has_the_declared_source_identity(self):
        scenario = load_scenario()
        for record in scenario["source_records"]:
            for field in scenario["identity_fields"]:
                self.assertIn(field, record)
                self.assertNotEqual(str(record[field]).strip(), "")


if __name__ == "__main__":
    unittest.main()
