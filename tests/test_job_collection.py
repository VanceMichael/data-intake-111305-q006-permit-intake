import json
import tempfile
import unittest
from pathlib import Path

from permit_intake.archive import Archive
from permit_intake.client import Client
from permit_intake.ingest import poll_once, replay_archive
from permit_intake.ipp import (
    JOB_STATE_NAMES,
    collect_attribute,
    view_attribute,
)
from permit_intake.jobstore import JobStore, canonical_json, digest_of
from permit_intake.origin import load_scenario, serve
from permit_intake.reconcile import build_report


def observation(gateway, number, uuid, uri, uptime, groups, *, uptime_present=True, tag="integer"):
    record = {
        "gateway": gateway,
        "observation": str(number),
        "printer_uuid": uuid,
        "printer_uri": uri,
        "groups": groups,
    }
    if uptime_present:
        record["printer_up_time"] = uptime
        record["printer_up_time_tag"] = tag
    return record


def job_group(job_id, state=None, *, completed=None, completed_presence="value",
              reasons=None, reason_presence="value", include_completed=True,
              unsupported_attr=False, unknown_attr=False, include_job_id=True):
    attributes = []
    if include_job_id:
        attributes.append({"name": "job-id", "tag": "integer", "value": job_id})
    if state is not None:
        attributes.append({"name": "job-state", "tag": "enum", "value": state})
    if reason_presence == "value":
        attributes.append({"name": "job-state-reasons", "tag": "keyword", "value": reasons or "none"})
    else:
        attributes.append({"name": "job-state-reasons", "tag": reason_presence, "value": None})
    if include_completed:
        if completed_presence == "value":
            attributes.append({"name": "time-at-completed", "tag": "integer", "value": completed})
        else:
            attributes.append({"name": "time-at-completed", "tag": completed_presence, "value": None})
    if unsupported_attr:
        attributes.append({"name": "job-k-octets", "tag": "unsupported", "value": None})
    if unknown_attr:
        attributes.append({"name": "job-message-from-operator", "tag": "unknown", "value": None})
    return {"tag": "job-attributes", "attributes": attributes}


class IppSemanticsTests(unittest.TestCase):
    def test_four_absences_are_distinct(self):
        group = [
            {"name": "a", "tag": "no-value", "value": None},
            {"name": "b", "tag": "unknown", "value": None},
            {"name": "c", "tag": "unsupported", "value": None},
        ]
        self.assertEqual(view_attribute(group, "a").presence, "no-value")
        self.assertEqual(view_attribute(group, "b").presence, "unknown")
        self.assertEqual(view_attribute(group, "c").presence, "unsupported")
        missing = view_attribute(group, "d")
        self.assertEqual(missing.presence, "missing")
        self.assertIsNone(missing.tag)
        self.assertIsNone(missing.value)
        for name in ("a", "b", "c", "d"):
            self.assertIsNone(view_attribute(group, name).value)

    def test_multi_value_attribute_collects_every_occurrence(self):
        group = [
            {"name": "job-state-reasons", "tag": "keyword", "value": "job-printing"},
            {"name": "job-state-reasons", "tag": "keyword", "value": "resources-are-not-ready"},
        ]
        view = collect_attribute(group, "job-state-reasons")
        self.assertEqual(view.presence, "value")
        self.assertEqual(view.value, ["job-printing", "resources-are-not-ready"])
        self.assertEqual(
            collect_attribute(group, "absent").presence, "missing"
        )

    def test_terminal_enum_table_covers_rfc8011_states(self):
        self.assertEqual(JOB_STATE_NAMES[9], "completed")
        self.assertEqual(JOB_STATE_NAMES[7], "canceled")
        self.assertEqual(JOB_STATE_NAMES[8], "aborted")


class JobStoreFixtureTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = JobStore(Path(self.temporary.name) / "jobs.sqlite3")
        self.run_id = self.store.start_run("test")

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def test_groups_are_not_flattened_across_job_boundaries(self):
        record = observation(
            "window-a", 1, "urn:uuid:p1", "ipp://p1", 100,
            [job_group(7, state=5, completed=0, completed_presence="no-value"),
             job_group(8, state=3, completed=0, completed_presence="no-value")],
        )
        summary = self.store.ingest_record(record, self.run_id)
        self.assertEqual(summary.groups, 2)
        self.assertEqual(summary.linked_groups, 2)
        rows = self.store.connection.execute(
            "SELECT j.job_id, g.position FROM job_groups g JOIN jobs j ON j.job_row_id=g.job_row_id ORDER BY g.position"
        ).fetchall()
        self.assertEqual([(row["job_id"], row["position"]) for row in rows], [(7, 0), (8, 1)])
        # 每个属性只属于自己的组。
        group_zero = [
            row["name"]
            for row in self.store.connection.execute(
                "SELECT name FROM group_attributes WHERE job_group_id=? ORDER BY position",
                (1,),
            ).fetchall()
        ]
        self.assertIn("job-id", group_zero)
        group_one = self.store.raw_observation(1)["groups"][1]
        self.assertEqual(group_one["attributes"][0]["value"], 8)

    def test_value_tags_keep_distinct_presence_rows(self):
        record = observation(
            "window-a", 1, "urn:uuid:p1", "ipp://p1", 100,
            [job_group(7, state=5, completed_presence="no-value",
                       unsupported_attr=True, unknown_attr=True)],
        )
        self.store.ingest_record(record, self.run_id)
        presences = {
            row["name"]: (row["presence"], row["value_tag"])
            for row in self.store.connection.execute(
                "SELECT name,presence,value_tag FROM group_attributes"
            ).fetchall()
        }
        self.assertEqual(presences["time-at-completed"], ("no-value", "no-value"))
        self.assertEqual(presences["job-k-octets"], ("unsupported", "unsupported"))
        self.assertEqual(presences["job-message-from-operator"], ("unknown", "unknown"))
        self.assertEqual(presences["job-state"][0], "value")
        # 未返回的属性不在属性行里，但快照口径记为 missing。
        self.assertNotIn("job-media-sheets", presences)
        snapshot = self.store.connection.execute(
            "SELECT * FROM job_state_snapshots"
        ).fetchone()
        self.assertEqual(snapshot["completed_presence"], "no-value")
        self.assertIsNone(snapshot["completed_value_json"])

    def test_restart_reuses_job_id_but_opens_new_epoch(self):
        uuid = "urn:uuid:p1"
        self.store.ingest_record(
            observation("w", 1, uuid, "ipp://p1", 800, [job_group(7, state=5)]),
            self.run_id,
        )
        summary = self.store.ingest_record(
            observation("w", 2, uuid, "ipp://p1", 12, [job_group(7, state=3)]),
            self.run_id,
        )
        self.assertEqual(summary.epoch_reason, "uptime_went_backwards")
        self.assertEqual(summary.epoch_generation, 2)
        jobs = self.store.connection.execute(
            "SELECT job_row_id, epoch_id, job_id FROM jobs ORDER BY job_row_id"
        ).fetchall()
        self.assertEqual(len(jobs), 2)
        self.assertNotEqual(jobs[0]["epoch_id"], jobs[1]["epoch_id"])
        self.assertEqual([row["job_id"] for row in jobs], [7, 7])

    def test_same_uuid_multiple_uris_are_aliases_not_new_devices(self):
        uuid = "urn:uuid:p1"
        self.store.ingest_record(
            observation("w", 1, uuid, "ipp://p1", 100, [job_group(1, state=3)]), self.run_id
        )
        self.store.ingest_record(
            observation("w", 2, uuid, "ipp://alias", 140, [job_group(1, state=5)]), self.run_id
        )
        devices = self.store.devices()
        self.assertEqual(len(devices), 1)
        self.assertEqual({u["printer_uri"] for u in devices[0]["uris"]},
                         {"ipp://p1", "ipp://alias"})
        self.assertEqual(len(devices[0]["epochs"]), 1)

    def test_same_uri_new_uuid_is_device_replacement(self):
        self.store.ingest_record(
            observation("w", 1, "urn:uuid:p1", "ipp://shared", 100, [job_group(1, state=5)]),
            self.run_id,
        )
        self.store.ingest_record(
            observation("w", 2, "urn:uuid:p2", "ipp://shared", 300, [job_group(1, state=5)]),
            self.run_id,
        )
        devices = self.store.devices()
        self.assertEqual({d["printer_uuid"] for d in devices},
                         {"urn:uuid:p1", "urn:uuid:p2"})
        jobs = self.store.connection.execute(
            "SELECT d.printer_uuid, j.job_id FROM jobs j JOIN devices d ON d.device_id=j.device_id"
        ).fetchall()
        self.assertEqual({(row["printer_uuid"], row["job_id"]) for row in jobs},
                         {("urn:uuid:p1", 1), ("urn:uuid:p2", 1)})

    def test_process_restart_with_monotonic_uptime_keeps_same_epoch(self):
        uuid = "urn:uuid:p1"
        self.store.ingest_record(
            observation("w", 1, uuid, "ipp://p1", 100, [job_group(1, state=5)]), self.run_id
        )
        # 采集进程重启（新 run_id），但打印机 uptime 继续单调递增：没有
        # 重启证据，必须沿用同一世代，不能把同一作业拆成两条。
        new_run = self.store.start_run("after-restart")
        summary = self.store.ingest_record(
            observation("w", 2, uuid, "ipp://p1", 200, [job_group(2, state=5)]), new_run
        )
        self.assertEqual(summary.epoch_generation, 1)
        self.assertEqual(summary.epoch_reason, "continued")
        epoch = self.store.connection.execute(
            "SELECT * FROM device_epochs WHERE epoch_id=?", (summary.epoch_id,)
        ).fetchone()
        self.assertEqual(epoch["continuity_verified"], 1)
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 2
        )

    def test_restart_across_process_gap_is_still_detected_by_uptime_drop(self):
        uuid = "urn:uuid:p1"
        self.store.ingest_record(
            observation("w", 1, uuid, "ipp://p1", 800, [job_group(7, state=5)]), self.run_id
        )
        new_run = self.store.start_run("after-restart")
        summary = self.store.ingest_record(
            observation("w", 2, uuid, "ipp://p1", 12, [job_group(7, state=3)]), new_run
        )
        self.assertEqual(summary.epoch_reason, "uptime_went_backwards")
        self.assertEqual(summary.epoch_generation, 2)
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 2
        )

    def test_small_uptime_backtrack_from_interleaved_windows_is_not_a_restart(self):
        uuid = "urn:uuid:p1"
        self.store.ingest_record(
            observation("window-a", 1, uuid, "ipp://p1", 500, [job_group(1, state=5)]),
            self.run_id,
        )
        # 窗口 b 的观察送达稍晚，uptime 小幅落后（乱序抖动），不应开新世代。
        summary = self.store.ingest_record(
            observation("window-b", 1, uuid, "ipp://p1", 470, [job_group(1, state=5)]),
            self.run_id,
        )
        self.assertEqual(summary.epoch_reason, "continued")
        self.assertEqual(summary.epoch_generation, 1)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT count(*) FROM device_epochs"
            ).fetchone()[0],
            1,
        )
        # 大幅回退仍然判定重启。
        restarted = self.store.ingest_record(
            observation("window-a", 2, uuid, "ipp://p1", 10, [job_group(2, state=3)]),
            self.run_id,
        )
        self.assertEqual(restarted.epoch_reason, "uptime_went_backwards")
        self.assertEqual(restarted.epoch_generation, 2)

    def test_missing_uptime_never_splits_epochs(self):
        uuid = "urn:uuid:p1"
        first = self.store.ingest_record(
            observation("w", 1, uuid, "ipp://p1", 100, [job_group(1, state=5)]), self.run_id
        )
        new_run = self.store.start_run("after-restart")
        summary = self.store.ingest_record(
            observation("w", 2, uuid, "ipp://p1", None, [job_group(2, state=5)],
                        uptime_present=False),
            new_run,
        )
        self.assertEqual(summary.epoch_id, first.epoch_id)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT count(*) FROM device_epochs"
            ).fetchone()[0],
            1,
        )

    def test_first_epoch_without_any_uptime_is_flagged_unverified(self):
        summary = self.store.ingest_record(
            observation("w", 1, "urn:uuid:p1", "ipp://p1", None, [job_group(1, state=5)],
                        uptime_present=False),
            self.run_id,
        )
        epoch = self.store.connection.execute(
            "SELECT continuity_verified FROM device_epochs WHERE epoch_id=?",
            (summary.epoch_id,),
        ).fetchone()
        self.assertEqual(epoch["continuity_verified"], 0)

    def test_redelivery_is_idempotent_but_keeps_second_delivery_evidence(self):
        record = observation("w", 1, "urn:uuid:p1", "ipp://p1", 100, [job_group(1, state=5)])
        first = self.store.ingest_record(record, self.run_id)
        second = self.store.ingest_record(record, self.store.start_run("again"))
        self.assertTrue(second.is_redelivery)
        self.assertEqual(first.source_observation_id, second.source_observation_id)
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 1
        )
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM job_groups").fetchone()[0], 1
        )
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM observation_deliveries").fetchone()[0], 2
        )

    def test_redelivery_with_changed_payload_is_flagged_not_overwritten(self):
        record = observation("w", 1, "urn:uuid:p1", "ipp://p1", 100, [job_group(1, state=5)])
        self.store.ingest_record(record, self.run_id)
        changed = dict(record)
        changed["printer_up_time"] = 999
        summary = self.store.ingest_record(changed, self.run_id)
        self.assertTrue(summary.is_redelivery)
        self.assertTrue(summary.payload_conflict)
        stored = self.store.raw_observation(summary.source_observation_id)
        self.assertEqual(stored["printer_up_time"], 100)

    def test_unattributed_group_is_retained_without_guessing(self):
        record = observation(
            "w", 1, "urn:uuid:p1", "ipp://p1", 100,
            [
                job_group(1, state=5),
                {"tag": "job-attributes", "attributes": [
                    {"name": "job-state", "tag": "enum", "value": 5},
                    {"name": "time-at-completed", "tag": "no-value", "value": None},
                ]},
            ],
        )
        summary = self.store.ingest_record(record, self.run_id)
        self.assertEqual(summary.unattributed_groups, 1)
        unattributed = self.store.connection.execute(
            "SELECT reason_code FROM unattributed_groups"
        ).fetchall()
        self.assertEqual([row["reason_code"] for row in unattributed], ["job-id_missing"])
        # 原始组仍在，且没有凭空建作业。
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 1
        )
        raw_groups = self.store.raw_observation(1)["groups"]
        self.assertEqual(len(raw_groups), 2)

    def test_raw_group_json_and_digest_are_preserved(self):
        record = observation("w", 1, "urn:uuid:p1", "ipp://p1", 100, [job_group(1, state=5)])
        self.store.ingest_record(record, self.run_id)
        row = self.store.connection.execute("SELECT raw_json, raw_digest FROM job_groups").fetchone()
        self.assertEqual(digest_of(row["raw_json"]), row["raw_digest"])
        self.assertEqual(json.loads(row["raw_json"]), record["groups"][0])
        obs = self.store.connection.execute("SELECT raw_json, raw_digest FROM source_observations").fetchone()
        self.assertEqual(digest_of(canonical_json(json.loads(obs["raw_json"]))), obs["raw_digest"])

    def test_integer_observation_bridge_field_is_accepted(self):
        record = {
            "gateway": "window-a",
            "observation": 3,
            "printer_uuid": "urn:uuid:p1",
            "printer_uri": "ipp://p1",
            "printer_up_time": 100,
            "groups": [job_group(1, state=5)],
        }
        summary = self.store.ingest_record(record, self.run_id)
        row = self.store.connection.execute(
            "SELECT gateway, observation FROM source_observations WHERE source_observation_id=?",
            (summary.source_observation_id,),
        ).fetchone()
        self.assertEqual((row["gateway"], row["observation"]), ("window-a", "3"))
        with self.assertRaises(ValueError):
            bad = dict(record, observation=True)
            self.store.ingest_record(bad, self.run_id)

    def test_missing_uptime_stays_distinct_from_zero_uptime(self):
        record = observation(
            "w", 1, "urn:uuid:p1", "ipp://p1", None, [job_group(1, state=5)],
            uptime_present=False,
        )
        summary = self.store.ingest_record(record, self.run_id)
        row = self.store.connection.execute(
            "SELECT uptime_presence, uptime_value FROM source_observations WHERE source_observation_id=?",
            (summary.source_observation_id,),
        ).fetchone()
        self.assertEqual(row["uptime_presence"], "missing")
        self.assertIsNone(row["uptime_value"])

    def test_validation_rejects_malformed_records_without_partial_write(self):
        bad_records = [
            {"gateway": "w", "printer_uuid": "u", "groups": []},
            {"gateway": "w", "observation": "1", "printer_uuid": "u", "groups": "nope"},
            {"gateway": "w", "observation": "1", "printer_uuid": "u",
             "groups": [{"tag": "job-attributes", "attributes": [
                 {"name": "job-id", "tag": "", "value": 1}]}]},
        ]
        for bad in bad_records:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.store.ingest_record(bad, self.run_id)
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM source_observations").fetchone()[0], 0
        )


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = JobStore(Path(self.temporary.name) / "jobs.sqlite3")
        self.run_id = self.store.start_run("test")
        self.uuid = "urn:uuid:p1"

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def _feed(self, number, uptime, groups, *, gateway="window-a", uri="ipp://p1"):
        return self.store.ingest_record(
            observation(gateway, number, self.uuid, uri, uptime, groups),
            self.run_id,
        )

    def _fetch(self, gateway="window-a", complete=True, capture_id=1):
        self.store.record_fetch(
            self.run_id, gateway, capture_id=capture_id if complete else None,
            complete=complete, decode_ok=complete,
            record_count=1 if complete else None,
        )

    def test_completed_state_with_verifiable_time_is_confirmed(self):
        self._feed(1, 800, [job_group(7, state=5, completed_presence="no-value")])
        self._feed(2, 830, [job_group(7, state=9, completed=820)])
        self._fetch(capture_id=2)
        report = build_report(self.store)
        self.assertEqual(report["counts"]["confirmed_processed"], 1)
        entry = report["confirmed_processed"][0]
        self.assertNotIn("success", json.dumps(entry))
        timing = entry["processing_time"]
        self.assertEqual(timing["status"], "estimated")
        self.assertEqual(timing["ipp_time_at_completed_seconds"], 820)
        self.assertEqual(timing["clock_basis_uptime_seconds"], 830)
        self.assertEqual(timing["monotonic_check"], "pass")
        # 证据链可回溯到属性组。
        self.assertTrue(entry["evidence"]["raw_groups"])

    def test_disappearance_without_terminal_state_stays_unverified(self):
        self._feed(1, 100, [job_group(7, state=3, completed_presence="no-value")])
        self._fetch(capture_id=1)
        # 下一次完整获取里没有该作业（空观察，仍属同设备同世代）。
        self._feed(2, 140, [])
        self._fetch(capture_id=2)
        report = build_report(self.store)
        self.assertEqual(report["counts"]["confirmed_processed"], 0)
        self.assertEqual(len(report["unverified"]), 1)
        self.assertEqual(report["unverified"][0]["reason_code"], "absent_after_complete_fetch")

    def test_incomplete_fetch_cannot_confirm_disappearance(self):
        self._feed(1, 100, [job_group(7, state=5, completed_presence="no-value")])
        self._fetch(capture_id=1)
        self._fetch(complete=False)
        report = build_report(self.store)
        entry = report["unverified"][0]
        self.assertEqual(entry["reason_code"], "absence_unverified_incomplete_fetch")
        health = report["fetch_health"][0]
        self.assertFalse(health["latest_attempt"]["complete"])

    def test_canceled_and_aborted_are_terminal_but_not_confirmed_printed(self):
        self._feed(1, 100, [
            job_group(7, state=7, completed_presence="no-value"),
        ])
        self.store.ingest_record(
            observation("window-a", 2, self.uuid, "ipp://p1", 120,
                        [job_group(8, state=8, completed_presence="no-value")]),
            self.run_id,
        )
        self._fetch(capture_id=1)
        report = build_report(self.store)
        self.assertEqual(report["counts"]["confirmed_processed"], 0)
        reasons = {entry["job_identity"]["job_id"]: entry["reason_code"]
                   for entry in report["unverified"]}
        self.assertEqual(reasons, {7: "terminal_without_completion",
                                   8: "terminal_without_completion"})

    def test_completed_without_time_value_keeps_raw_presence(self):
        self._feed(1, 100, [job_group(7, state=9, completed_presence="unknown")])
        self._fetch(capture_id=1)
        report = build_report(self.store)
        entry = report["confirmed_processed"][0]
        self.assertEqual(entry["processing_time"]["status"], "ipp_time_unavailable")
        self.assertEqual(entry["processing_time"]["presence"], "unknown")

    def test_restart_epoch_keeps_two_jobs_with_same_id_separate_in_report(self):
        self._feed(1, 800, [job_group(7, state=9, completed=790)])
        self._feed(2, 12, [job_group(7, state=3, completed_presence="no-value")])
        self._fetch(capture_id=2)
        report = build_report(self.store)
        keys = {entry["job_identity"]["composite_key"] for entry in report["confirmed_processed"]}
        keys |= {entry["job_identity"]["composite_key"] for entry in report["unverified"]}
        self.assertIn(f"{self.uuid}/gen1/job7", keys)
        self.assertIn(f"{self.uuid}/gen2/job7", keys)
        self.assertEqual(len(keys), 2)

    def test_unattributed_groups_are_listed_separately(self):
        self.store.ingest_record(
            observation("window-a", 1, self.uuid, "ipp://p1", 100, [
                {"tag": "job-attributes", "attributes": [
                    {"name": "job-state", "tag": "enum", "value": 5}]}
            ]),
            self.run_id,
        )
        report = build_report(self.store)
        self.assertEqual(report["counts"]["unattributed_groups"], 1)
        self.assertEqual(report["unattributed_groups"][0]["reason_code"], "job-id_missing")

    def test_report_is_rebuildable_and_contains_no_boolean_success(self):
        self._feed(1, 100, [job_group(7, state=5, completed_presence="no-value")])
        self._fetch(capture_id=1)
        report = build_report(self.store)
        text = json.dumps(report, ensure_ascii=False)
        self.assertNotIn('"success"', text)
        self.assertIn("disappearance_is_not_completion", text)
        self.assertIn("no_automatic_reprint", text)


class InterleavingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.archive = Archive(Path(self.temporary.name) / "raw.sqlite3")
        self.store = JobStore(Path(self.temporary.name) / "jobs.sqlite3")

    def tearDown(self):
        self.store.close()
        self.archive.close()
        self.temporary.cleanup()

    def test_concurrent_windows_sharing_one_store_keep_single_epoch(self):
        import threading

        other = JobStore(Path(self.temporary.name) / "jobs.sqlite3")
        errors = []
        try:
            run_a = self.store.start_run("window-a")
            run_b = other.start_run("window-b")

            def worker(store, gateway, run):
                try:
                    for number in range(1, 21):
                        record = observation(
                            gateway, number, "urn:uuid:p1", "ipp://p1",
                            100 + number, [job_group(number, state=3)],
                        )
                        store.ingest_record(record, run)
                except Exception as exc:  # pragma: no cover - 失败即断言
                    errors.append(exc)

            threads = [
                threading.Thread(target=worker, args=(self.store, "window-a", run_a)),
                threading.Thread(target=worker, args=(other, "window-b", run_b)),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        finally:
            other.close()
        self.assertEqual(errors, [])
        # 两个窗口看到同一台设备的同一批 job-id（1..20），必须归并成
        # 20 个作业、1 个世代、40 条观察。
        self.assertEqual(
            self.store.connection.execute(
                "SELECT count(*) FROM source_observations"
            ).fetchone()[0],
            40,
        )
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 20
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT count(*) FROM device_epochs"
            ).fetchone()[0],
            1,
        )

    def test_two_gateways_interleave_without_duplicating_jobs(self):
        run = self.store.start_run("interleave")
        shared = observation("window-a", "10", "urn:uuid:p1", "ipp://p1", 100,
                             [job_group(1, state=3)])
        # 两个窗口各自抓到同一条网关观察（交错送达）。
        first = self.store.ingest_record(shared, run, capture_id=1,
                                         body_digest="digest-x")
        second = self.store.ingest_record(shared, run, capture_id=2,
                                          body_digest="digest-x")
        self.assertTrue(second.is_redelivery)
        self.assertEqual(first.source_observation_id, second.source_observation_id)
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 1
        )
        # 窗口 b 自己的观察独立保存。
        self.store.ingest_record(
            observation("window-b", "1", "urn:uuid:p1", "ipp://p1", 110,
                        [job_group(1, state=5)]),
            run,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT count(*) FROM source_observations WHERE gateway='window-a'"
            ).fetchone()[0],
            1,
        )

    def test_archive_replay_rebuilds_equivalent_report(self):
        scenario = load_scenario(
            Path(__file__).parents[1] / "fixtures" / "job_cases.json"
        )
        with serve(scenario) as origin:
            client = Client(origin.base_url)
            run = self.store.start_run("live")
            for _ in scenario["routes"]["/feed"]:
                poll_once(client, self.archive, self.store, run, "window-a", "/feed")
        expected = build_report(self.store)

        rebuilt = JobStore(Path(self.temporary.name) / "rebuilt.sqlite3")
        try:
            replay_run = rebuilt.start_run("replay")
            replay_archive(self.archive, rebuilt, replay_run, "window-a")
            actual = build_report(rebuilt)
        finally:
            rebuilt.close()
        self.assertEqual(actual["counts"], expected["counts"])
        self.assertEqual(
            [entry["job_identity"]["composite_key"] for entry in actual["confirmed_processed"]],
            [entry["job_identity"]["composite_key"] for entry in expected["confirmed_processed"]],
        )

    def test_replay_is_idempotent_on_the_same_store(self):
        run = self.store.start_run("live")
        self.store.ingest_record(
            observation("window-a", 1, "urn:uuid:p1", "ipp://p1", 100,
                        [job_group(1, state=5)]),
            run, capture_id=1, body_digest="d",
        )
        replay_archive(self.archive, self.store, self.store.start_run("replay-1"))
        replay_archive(self.archive, self.store, self.store.start_run("replay-2"))
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 1
        )


class PollFlowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.archive = Archive(Path(self.temporary.name) / "raw.sqlite3")
        self.store = JobStore(Path(self.temporary.name) / "jobs.sqlite3")
        self.scenario_path = Path(__file__).parents[1] / "fixtures" / "job_cases.json"

    def tearDown(self):
        self.store.close()
        self.archive.close()
        self.temporary.cleanup()

    def test_full_fixture_feed_ends_with_expected_partitions(self):
        scenario = load_scenario(self.scenario_path)
        with serve(scenario) as origin:
            client = Client(origin.base_url)
            run = self.store.start_run("flow")
            results = [
                poll_once(client, self.archive, self.store, run, "window-a", "/feed")
                for _ in scenario["routes"]["/feed"]
            ]
        self.assertTrue(all(result.ok for result in results))
        report = build_report(self.store)
        # 作业 7（gen1）、8 已完成；作业 9、重启后的 7（gen2）、替换设备的 1 待核。
        self.assertEqual(report["counts"]["confirmed_processed"], 2)
        self.assertEqual(report["counts"]["unverified"], 3)
        self.assertEqual(report["counts"]["unattributed_groups"], 1)
        # 两个设备、同 URI 替换关系保留。
        self.assertEqual(len(report["devices"]), 2)

    def test_http_non_200_is_recorded_but_creates_no_jobs(self):
        scenario = load_scenario(self.scenario_path)
        with serve(scenario) as origin:
            run = self.store.start_run("flow")
            result = poll_once(
                Client(origin.base_url), self.archive, self.store, run,
                "window-a", "/rate-limited",
            )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_code, "http_non_200")
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 0
        )
        attempt = self.store.connection.execute(
            "SELECT * FROM fetch_attempts"
        ).fetchone()
        self.assertEqual(attempt["http_status"], 429)
        self.assertEqual(attempt["complete"], 1)
        self.assertEqual(attempt["decode_ok"], 0)
        # 原始响应仍在归档里。
        self.assertEqual(len(self.archive.captures()), 1)

    def test_truncated_response_is_logged_as_incomplete_and_not_archived_as_feed(self):
        scenario = load_scenario(self.scenario_path)
        with serve(scenario) as origin:
            run = self.store.start_run("flow")
            result = poll_once(
                Client(origin.base_url), self.archive, self.store, run,
                "window-a", "/truncated",
            )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_code, "incomplete_read")
        self.assertEqual(
            self.store.connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 0
        )
        attempt = self.store.connection.execute(
            "SELECT * FROM fetch_attempts"
        ).fetchone()
        self.assertEqual(attempt["complete"], 0)

    def test_archive_body_digest_matches_poll_evidence(self):
        scenario = load_scenario(self.scenario_path)
        with serve(scenario) as origin:
            run = self.store.start_run("flow")
            result = poll_once(
                Client(origin.base_url), self.archive, self.store, run,
                "window-a", "/feed",
            )
        self.assertTrue(result.ok)
        capture = self.archive.captures()[0]
        import hashlib
        self.assertEqual(
            hashlib.sha256(self.archive.body(capture["id"])).hexdigest(),
            self.store.connection.execute(
                "SELECT body_digest FROM observation_deliveries WHERE capture_id=?",
                (capture["id"],),
            ).fetchone()["body_digest"],
        )


class FixtureContractTests(unittest.TestCase):
    def test_job_fixture_covers_declared_business_cases(self):
        scenario = load_scenario(
            Path(__file__).parents[1] / "fixtures" / "job_cases.json"
        )
        feeds = scenario["routes"]["/feed"]
        first = feeds[0]["json"]["items"][0]
        self.assertEqual(len(first["groups"]), 3)
        # 第三个组没有 job-id，对应无法关联的属性组。
        self.assertNotIn("job-id", first["groups"][2]["attributes"][0].values())
        # 重启：观察 4 的 uptime 明显回退且复用 job-id 7。
        obs4 = feeds[3]["json"]["items"][0]
        self.assertLess(obs4["printer_up_time"], first["printer_up_time"])
        self.assertEqual(obs4["groups"][0]["attributes"][0]["value"], 7)
        # 设备替换：观察 5 同一 URI 换了 UUID。
        obs5 = feeds[4]["json"]["items"][0]
        self.assertEqual(obs5["printer_uri"], obs4["printer_uri"])
        self.assertNotEqual(obs5["printer_uuid"], obs4["printer_uuid"])
        # 四种取值口径都在场景里出现。
        all_tags = {
            attribute["tag"]
            for feed in feeds
            for item in feed["json"]["items"]
            for group in item["groups"]
            for attribute in group["attributes"]
        }
        self.assertIn("no-value", all_tags)
        self.assertIn("unknown", all_tags)
        self.assertIn("unsupported", all_tags)


if __name__ == "__main__":
    unittest.main()
