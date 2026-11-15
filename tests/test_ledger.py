import json
import tempfile
import unittest
from pathlib import Path

from permit_intake.archive import Archive
from permit_intake.client import Client, collect
from permit_intake.ledger import JobLedger, estimate_wall_clock
from permit_intake.origin import serve


def group(*attributes, tag="job-attributes"):
    return {"tag": tag, "attributes": list(attributes)}


def attr(name, tag, value):
    return {"name": name, "tag": tag, "value": value}


def record(gateway="window-a", observation=1, uuid="urn:uuid:dev-1",
           uri="ipp://local/printer", up_time=100, groups=(), **extra):
    envelope = {
        "gateway": gateway,
        "observation": observation,
        "printer_uuid": uuid,
        "printer_uri": uri,
        "printer_up_time": up_time,
        "groups": list(groups),
    }
    envelope.update(extra)
    return envelope


class LedgerCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.archive = Archive(root / "raw.sqlite3")
        self.ledger = JobLedger(root / "jobs.sqlite3")

    def tearDown(self):
        self.ledger.close()
        self.archive.close()
        self.temporary.cleanup()

    def add_capture(self, items, status=200, source="window-a", url="/feed"):
        body = json.dumps({"items": items}, ensure_ascii=False).encode()
        headers = {"content-type": "application/json"} if status == 200 else {}
        return self.archive.append(source, url, status, headers, body)

    def ingest(self):
        return self.ledger.ingest_all(self.archive)

    def report(self, **filters):
        return self.ledger.report(self.archive, **filters)

    def receipts(self, **filters):
        return self.report(**filters)["receipts"]


class GroupBoundaryTests(LedgerCase):
    def test_attributes_never_flatten_across_job_groups(self):
        self.add_capture([
            record(groups=[
                group(attr("job-id", "integer", 7), attr("job-state", "enum", 5)),
                group(attr("job-id", "integer", 8),
                      attr("time-at-completed", "no-value", None)),
            ]),
        ])
        self.ingest()
        receipts = self.receipts()
        self.assertEqual(len(receipts), 2)
        by_id = {receipt["job_id"]: receipt for receipt in receipts}
        self.assertEqual(by_id[7]["timeline"][0]["job_state"]["value"], 5)
        self.assertEqual(by_id[7]["processing_times"]["time-at-completed"]["state"], "absent")
        self.assertEqual(by_id[8]["timeline"][0]["job_state"]["state"], "absent")
        completed = by_id[8]["processing_times"]["time-at-completed"]
        self.assertEqual(completed["state"], "out_of_band")
        self.assertEqual(completed["tag"], "no-value")

    def test_no_value_unknown_and_absent_stay_distinct(self):
        self.add_capture([
            record(groups=[
                group(attr("job-id", "integer", 9),
                      attr("job-state", "unknown", None),
                      attr("time-at-completed", "no-value", None)),
            ]),
        ])
        self.ingest()
        receipt = self.receipts()[0]
        self.assertEqual(receipt["timeline"][0]["job_state"]["state"], "out_of_band")
        self.assertEqual(receipt["timeline"][0]["job_state"]["tag"], "unknown")
        times = receipt["processing_times"]
        self.assertEqual(times["time-at-completed"]["state"], "out_of_band")
        self.assertEqual(times["time-at-completed"]["tag"], "no-value")
        self.assertEqual(times["time-at-processing"]["state"], "absent")
        self.assertEqual(receipt["assessment"], "unverified")


class DeviceIdentityTests(LedgerCase):
    def test_restart_reused_job_id_is_not_merged(self):
        self.add_capture([record(observation=1, up_time=800, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 5)),
        ])])
        self.add_capture([record(observation=2, up_time=12, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 3)),
        ])])
        self.ingest()
        epochs = self.ledger.connection.execute(
            "SELECT seq, reason FROM epochs ORDER BY seq").fetchall()
        self.assertEqual([(row["seq"], row["reason"]) for row in epochs],
                         [(1, "first_observation"), (2, "up_time_regressed")])
        receipts = self.receipts()
        self.assertEqual(len(receipts), 2)
        self.assertEqual({receipt["job_id"] for receipt in receipts}, {7})
        old = next(r for r in receipts if r["epoch"]["seq"] == 1)
        new = next(r for r in receipts if r["epoch"]["seq"] == 2)
        self.assertNotEqual(old["receipt_id"], new["receipt_id"])
        self.assertEqual(old["assessment"], "unverified")
        self.assertTrue(any("重启" in line for line in old["explanation"]))
        self.assertEqual(len(old["timeline"]), 1)
        self.assertEqual(new["assessment"], "in_progress")
        self.assertEqual(len(new["timeline"]), 1)

    def test_multiple_uris_are_aliases_of_one_device(self):
        self.add_capture([record(observation=1, uri="ipp://local/printer-a", up_time=100)])
        self.add_capture([record(observation=2, uri="ipp://local/alias-a", up_time=200)])
        self.ingest()
        devices = self.report()["devices"]
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices[0]["uris"], ["ipp://local/alias-a", "ipp://local/printer-a"])
        self.assertIsNone(devices[0]["shared_uri_warning"])

    def test_same_uri_with_new_uuid_is_a_replacement(self):
        self.add_capture([record(observation=1, uuid="urn:uuid:old", up_time=100)])
        self.add_capture([record(observation=2, uuid="urn:uuid:new", up_time=50)])
        self.ingest()
        devices = self.report()["devices"]
        self.assertEqual(len(devices), 2)
        self.assertTrue(all(device["shared_uri_warning"] for device in devices))
        # 新 uuid 的设备有自己的纪元，不从旧设备继承。
        epochs = self.ledger.connection.execute(
            "SELECT device_id, seq FROM epochs ORDER BY device_id").fetchall()
        self.assertEqual([row["seq"] for row in epochs], [1, 1])


class DisappearanceTests(LedgerCase):
    def test_disappearance_is_unverified_not_completed(self):
        self.add_capture([record(observation=1, up_time=100, groups=[
            group(attr("job-id", "integer", 9), attr("job-state", "enum", 5)),
        ])])
        self.add_capture([record(observation=2, up_time=200, groups=[])])
        self.ingest()
        receipt = self.receipts()[0]
        self.assertEqual(receipt["assessment"], "unverified")
        self.assertIsNone(receipt["terminal_state"])
        self.assertEqual(len(receipt["absences"]), 1)
        candidates = receipt["absences"][0]["candidates"]
        self.assertIn("retention_expired_or_history_scope", candidates)
        self.assertIn("query_scope_or_permission", candidates)
        self.assertNotIn("completed", json.dumps(receipt["explanation"]))

    def test_failed_fetch_creates_no_absence_but_is_visible(self):
        self.add_capture([record(observation=1, up_time=100, groups=[
            group(attr("job-id", "integer", 9), attr("job-state", "enum", 5)),
        ])])
        self.archive.append("window-a", "/feed", 503, {}, b"temporarily unavailable")
        self.add_capture([record(observation=3, up_time=300, groups=[])])
        summaries = self.ingest()
        self.assertEqual(summaries[1]["note"], "http_status_503")
        self.assertEqual(summaries[1]["records"], 0)
        receipt = self.receipts()[0]
        self.assertEqual(len(receipt["absences"]), 1)
        self.assertEqual(receipt["absences"][0]["capture"], 3)
        self.assertIn("fetch_incomplete", receipt["absences"][0]["candidates"])
        issues = self.report()["fetch_and_record_issues"]
        self.assertTrue(any(issue["issue"] == "http_status_503" for issue in issues))

    def test_reappearing_job_clears_unverified(self):
        self.add_capture([record(observation=1, up_time=100, groups=[
            group(attr("job-id", "integer", 9), attr("job-state", "enum", 5)),
        ])])
        self.add_capture([record(observation=2, up_time=200, groups=[])])
        self.add_capture([record(observation=3, up_time=300, groups=[
            group(attr("job-id", "integer", 9), attr("job-state", "enum", 5)),
        ])])
        self.ingest()
        receipt = self.receipts()[0]
        self.assertEqual(receipt["assessment"], "in_progress")
        self.assertEqual(len(receipt["absences"]), 1)  # 历史缺席仍作证据保留


class IngestionRobustnessTests(LedgerCase):
    def test_reingest_is_idempotent_across_process_restart(self):
        self.add_capture([record(observation=1, up_time=100, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 5)),
        ])])
        first = self.ingest()
        self.assertEqual(first[0]["skipped"], False)
        # 模拟进程重启：关闭台账后重开，重复摄入同一批 capture。
        self.ledger.close()
        self.ledger = JobLedger(Path(self.temporary.name) / "jobs.sqlite3")
        second = self.ingest()
        self.assertTrue(all(item["skipped"] for item in second))
        self.assertEqual(self.ledger.connection.execute(
            "SELECT count(*) FROM job_observations").fetchone()[0], 1)
        self.assertEqual(self.ledger.connection.execute(
            "SELECT count(*) FROM jobs").fetchone()[0], 1)

    def test_redelivered_record_does_not_open_a_false_epoch(self):
        original = record(observation=1, up_time=800, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 5)),
        ])
        self.add_capture([original])
        self.add_capture([record(observation=2, up_time=12, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 3)),
        ])])
        self.add_capture([original])  # 旧记录被重复投递
        summaries = self.ingest()
        self.assertEqual(summaries[2]["redeliveries"], 1)
        self.assertEqual(self.ledger.connection.execute(
            "SELECT count(*) FROM epochs").fetchone()[0], 2)
        self.assertEqual(self.ledger.connection.execute(
            "SELECT count(*) FROM job_observations").fetchone()[0], 2)

    def test_unkeyed_group_is_listed_not_dropped(self):
        self.add_capture([record(observation=1, up_time=100, groups=[
            group(attr("job-id", "no-value", None), attr("job-state", "enum", 5)),
        ])])
        self.ingest()
        result = self.report()
        self.assertEqual(result["receipts"], [])
        self.assertEqual(len(result["unattributed_observations"]), 1)
        unattributed = result["unattributed_observations"][0]
        self.assertEqual(unattributed["note"], "job-id out_of_band:no-value")
        self.assertEqual(unattributed["raw_group"]["attributes"][1]["name"], "job-state")

    def test_interleaved_windows_share_one_device_epoch(self):
        self.add_capture([record(gateway="window-a", observation=1, up_time=800, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 3)),
        ])], source="window-a")
        self.add_capture([record(gateway="window-b", observation=1, up_time=820, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 5)),
        ])], source="window-b")
        self.add_capture([record(gateway="window-a", observation=2, up_time=900, groups=[])],
                         source="window-a")
        self.ingest()
        self.assertEqual(self.ledger.connection.execute(
            "SELECT count(*) FROM epochs").fetchone()[0], 1)
        receipt = self.receipts()[0]
        self.assertEqual(receipt["assessment"], "unverified")
        self.assertEqual([entry["gateway"] for entry in receipt["timeline"]],
                         ["window-a", "window-b"])
        self.assertEqual(len(receipt["absences"]), 1)


class ReceiptTests(LedgerCase):
    def test_terminal_receipt_carries_verifiable_time_and_evidence(self):
        first = self.add_capture([record(observation=1, up_time=800, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 5),
                  attr("job-state-reasons", "keyword", "job-printing"),
                  attr("time-at-processing", "integer", 795)),
        ])])
        second = self.add_capture([record(observation=2, up_time=900, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 9),
                  attr("job-state-reasons", "keyword", "job-completed-successfully"),
                  attr("time-at-completed", "integer", 880)),
        ])])
        self.ingest()
        receipt = self.receipts()[0]
        self.assertEqual(receipt["assessment"], "processed")
        self.assertEqual(receipt["terminal_state"]["name"], "completed")
        self.assertEqual(receipt["terminal_state"]["capture"], second)
        completed = receipt["processing_times"]["time-at-completed"]
        self.assertEqual(completed["state"], "value")
        received_at = self.archive.capture(second)["received_at"]
        self.assertEqual(completed["wall_clock_estimate"],
                         estimate_wall_clock(received_at, 900, 880))
        self.assertEqual(completed["estimate_basis"]["printer_up_time"], 900)
        evidence = {item["capture"]: item for item in receipt["evidence"]}
        self.assertEqual(set(evidence), {first, second})
        for capture_id, item in evidence.items():
            self.assertEqual(item["digest"], self.archive.capture(capture_id)["digest"])
        self.assertEqual(receipt["timeline"][0]["job_state_reasons"], ["job-printing"])
        self.assertEqual(receipt["timeline"][1]["job_state_reasons"],
                         ["job-completed-successfully"])

    def test_unverified_jobs_are_listed_separately(self):
        self.add_capture([record(observation=1, up_time=100, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 9)),
            group(attr("job-id", "integer", 8), attr("job-state", "enum", 5)),
        ])])
        self.add_capture([record(observation=2, up_time=200, groups=[
            group(attr("job-id", "integer", 7), attr("job-state", "enum", 9)),
        ])])
        self.ingest()
        result = self.report()
        unverified = result["unverified_receipts"]
        self.assertEqual(len(unverified), 1)
        self.assertIn("job-8", unverified[0])
        self.assertEqual(result["collector"]["read_only"], True)
        self.assertEqual(result["collector"]["reprint"], "not_supported")

    def test_end_to_end_against_local_origin(self):
        with serve() as origin:
            capture_id, response = collect(Client(origin.base_url), self.archive, "fixture")
            self.assertEqual(response.status, 200)
        summaries = self.ingest()
        self.assertEqual(summaries[0]["records"], 2)
        result = self.report()
        self.assertEqual(len(result["devices"]), 1)
        self.assertEqual(result["devices"][0]["uris"],
                         ["ipp://local/alias-a", "ipp://local/printer-a"])
        receipts = {(r["epoch"]["seq"], r["job_id"]): r for r in result["receipts"]}
        self.assertEqual(receipts[(1, 7)]["assessment"], "unverified")
        self.assertEqual(receipts[(1, 8)]["assessment"], "unverified")
        self.assertEqual(receipts[(2, 7)]["assessment"], "in_progress")
        self.assertEqual(receipts[(1, 8)]["processing_times"]["time-at-completed"]["tag"],
                         "no-value")
        self.assertEqual(len(result["unverified_receipts"]), 2)
        self.assertEqual(capture_id, 1)


if __name__ == "__main__":
    unittest.main()
