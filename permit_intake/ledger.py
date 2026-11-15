"""作业资料采集层：把归档的 IPP 信封整理成可核实的作业台账。

本层在只读归档（archive.py）之上解释来源内容，自身不抓取、不投递、
不重打。设计口径：

- 按属性组理解作业：每个 job-attributes 组是一条作业观察，属性不跨组
  摊平；no-value、unknown 与属性未返回分别保留；
- 设备身份以 printer_uuid 为准，同一 uuid 的多个 URI 是别名；uuid 变化
  视为设备替换，即使 URI 相同；uptime 回退视为重启并开启新纪元，作业号
  只在纪元内唯一；
- 作业从查询结果消失只记录候选原因（留存到期、查询范围或权限变化、本次
  获取不完整），不自动认定完成；缺终态证据的办件保持待核；
- 摄入按 capture 逐个事务提交，重复摄入幂等，进程重启后从未摄入的
  capture 继续；多个窗口的轮询在归档层按到达顺序交错，本层按 capture
  顺序重放，结论只依赖已落库内容。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .decoding import records as decode_records
from .ipp import (
    ABSENT,
    OUT_OF_BAND,
    VALUE,
    JOB_STATE_NAMES,
    TERMINAL_JOB_STATES,
    EnvelopeError,
    parse_record,
    presence,
)

SCHEMA_VERSION = "1"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS devices (
    id INTEGER PRIMARY KEY,
    identity TEXT NOT NULL UNIQUE,
    identity_basis TEXT NOT NULL,
    printer_uuid TEXT,
    first_capture INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS device_uris (
    device_id INTEGER NOT NULL REFERENCES devices(id),
    uri TEXT NOT NULL,
    first_capture INTEGER NOT NULL,
    UNIQUE(device_id, uri)
);
CREATE TABLE IF NOT EXISTS epochs (
    id INTEGER PRIMARY KEY,
    device_id INTEGER NOT NULL REFERENCES devices(id),
    seq INTEGER NOT NULL,
    opened_capture INTEGER NOT NULL,
    opened_up_time INTEGER,
    max_up_time INTEGER,
    reason TEXT NOT NULL,
    UNIQUE(device_id, seq)
);
CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY,
    capture_id INTEGER NOT NULL,
    record_index INTEGER NOT NULL,
    gateway TEXT NOT NULL,
    observation INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    device_id INTEGER REFERENCES devices(id),
    epoch_id INTEGER REFERENCES epochs(id),
    up_time_state TEXT NOT NULL,
    up_time INTEGER,
    printer_uri TEXT,
    scope TEXT,
    notes TEXT NOT NULL DEFAULT '[]',
    UNIQUE(capture_id, record_index),
    UNIQUE(gateway, observation, content_hash)
);
CREATE TABLE IF NOT EXISTS redeliveries (
    id INTEGER PRIMARY KEY,
    capture_id INTEGER NOT NULL,
    record_index INTEGER NOT NULL,
    gateway TEXT NOT NULL,
    observation INTEGER NOT NULL,
    record_id INTEGER NOT NULL REFERENCES records(id),
    UNIQUE(capture_id, record_index)
);
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    device_id INTEGER NOT NULL REFERENCES devices(id),
    epoch_id INTEGER NOT NULL REFERENCES epochs(id),
    job_id INTEGER NOT NULL,
    first_capture INTEGER NOT NULL,
    last_record_id INTEGER NOT NULL,
    job_name TEXT,
    originating_user TEXT,
    UNIQUE(epoch_id, job_id)
);
CREATE TABLE IF NOT EXISTS job_observations (
    id INTEGER PRIMARY KEY,
    job_ref INTEGER NOT NULL REFERENCES jobs(id),
    record_id INTEGER NOT NULL REFERENCES records(id),
    group_index INTEGER NOT NULL,
    job_state INTEGER,
    job_state_presence TEXT NOT NULL,
    job_state_tag TEXT,
    state_reasons TEXT,
    attributes TEXT NOT NULL,
    raw_group TEXT NOT NULL,
    UNIQUE(record_id, group_index)
);
CREATE TABLE IF NOT EXISTS unkeyed_observations (
    id INTEGER PRIMARY KEY,
    record_id INTEGER NOT NULL REFERENCES records(id),
    group_index INTEGER NOT NULL,
    note TEXT NOT NULL,
    raw_group TEXT NOT NULL,
    UNIQUE(record_id, group_index)
);
CREATE TABLE IF NOT EXISTS absences (
    id INTEGER PRIMARY KEY,
    job_ref INTEGER NOT NULL REFERENCES jobs(id),
    record_id INTEGER NOT NULL REFERENCES records(id),
    candidates TEXT NOT NULL,
    UNIQUE(job_ref, record_id)
);
CREATE TABLE IF NOT EXISTS ingested_captures (
    capture_id INTEGER PRIMARY KEY,
    status INTEGER NOT NULL,
    records_found INTEGER NOT NULL,
    note TEXT
);
CREATE TABLE IF NOT EXISTS record_issues (
    id INTEGER PRIMARY KEY,
    capture_id INTEGER NOT NULL,
    record_index INTEGER NOT NULL,
    issue TEXT NOT NULL
);
"""

TIME_ATTRIBUTES = (
    "time-at-creation",
    "time-at-processing",
    "time-at-completed",
    "date-time-at-creation",
    "date-time-at-processing",
    "date-time-at-completed",
)

OUT_OF_BAND_MEANINGS = {
    "no-value": "属性已返回但当前无值（例如尚未完成）",
    "unknown": "设备声明不知道该值",
    "unsupported": "设备不支持该属性",
    "not-settable": "该属性不可设置",
    "default": "设备返回了默认值标记",
}


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_instant(text):
    try:
        return datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_instant(moment):
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def estimate_wall_clock(received_at, up_time, value):
    """由打印机相对秒数推算可核实的挂钟时间；输入不齐时返回 None。"""
    if isinstance(up_time, bool) or isinstance(value, bool):
        return None
    if not isinstance(up_time, int) or not isinstance(value, int):
        return None
    base = _parse_instant(received_at)
    if base is None:
        return None
    return _format_instant(base + timedelta(seconds=value - up_time))


class JobLedger:
    """SQLite 作业台账。所有写操作按 capture 事务化，可安全重复执行。"""

    def __init__(self, path):
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(self.path), timeout=10)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=10000")
        try:
            self.connection.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            pass
        self.connection.executescript(SCHEMA)
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO meta VALUES('schema_version', ?)", (SCHEMA_VERSION,)
            )

    def close(self):
        self.connection.close()

    @contextmanager
    def _immediate(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except BaseException:
            self.connection.rollback()
            raise
        self.connection.commit()

    # ------------------------------------------------------------------
    # 摄入
    # ------------------------------------------------------------------

    def ingest_all(self, archive, source=None):
        """按 capture 顺序摄入归档中尚未处理的抓取，返回每个 capture 的摘要。"""
        return [self.ingest_capture(archive, row["id"]) for row in archive.captures(source)]

    def ingest_capture(self, archive, capture_id):
        capture = archive.capture(capture_id)
        with self._immediate() as db:
            if db.execute(
                "SELECT 1 FROM ingested_captures WHERE capture_id=?", (capture_id,)
            ).fetchone():
                return {"capture_id": capture_id, "skipped": True}
            summary = {
                "capture_id": capture_id,
                "skipped": False,
                "status": capture["status"],
                "records": 0,
                "jobs_seen": 0,
                "absences": 0,
                "redeliveries": 0,
                "unkeyed": 0,
                "issues": 0,
                "note": None,
            }
            items = []
            if capture["status"] != 200:
                summary["note"] = "http_status_%d" % capture["status"]
            else:
                try:
                    body = archive.body(capture_id)
                except (KeyError, ValueError) as error:
                    summary["note"] = "body_unavailable:%s" % type(error).__name__
                else:
                    try:
                        items = decode_records(body, capture["headers"].get("content-type", ""))
                    except (ValueError, KeyError, UnicodeDecodeError) as error:
                        summary["note"] = "decode_failed:%s" % type(error).__name__
            for index, raw in enumerate(items):
                self._ingest_record(db, capture, index, raw, summary)
            db.execute(
                "INSERT INTO ingested_captures VALUES(?,?,?,?)",
                (capture_id, capture["status"], summary["records"], summary["note"]),
            )
            return summary

    def _ingest_record(self, db, capture, index, raw, summary):
        capture_id = capture["id"]
        try:
            record, problems = parse_record(raw)
        except EnvelopeError as error:
            db.execute(
                "INSERT INTO record_issues(capture_id, record_index, issue) VALUES(?,?,?)",
                (capture_id, index, str(error)),
            )
            summary["issues"] += 1
            return
        existing = db.execute(
            "SELECT id FROM records WHERE gateway=? AND observation=? AND content_hash=?",
            (record["gateway"], record["observation"], record["content_hash"]),
        ).fetchone()
        if existing is not None:
            db.execute(
                "INSERT OR IGNORE INTO redeliveries(capture_id, record_index, gateway, observation, record_id)"
                " VALUES(?,?,?,?,?)",
                (capture_id, index, record["gateway"], record["observation"], existing["id"]),
            )
            summary["redeliveries"] += 1
            return

        notes = list(problems)
        last = db.execute(
            "SELECT observation FROM records WHERE gateway=? ORDER BY id DESC LIMIT 1",
            (record["gateway"],),
        ).fetchone()
        if last is not None:
            if record["observation"] > last["observation"] + 1:
                notes.append("observation_gap:%s->%s" % (last["observation"], record["observation"]))
            elif record["observation"] <= last["observation"]:
                notes.append("counter_reset_or_reorder")

        device_id = self._device(db, record, capture_id, notes)
        up_time = record["printer_up_time"]
        up_value = up_time["values"][0] if up_time["state"] == VALUE else None
        epoch_id = None
        if device_id is not None:
            epoch_id = self._epoch(db, device_id, up_time["state"], up_value, capture_id, notes)
        scope = None
        if record["operation"] or record["which_jobs"]:
            scope = json.dumps(
                {"operation": record["operation"], "which_jobs": record["which_jobs"]},
                ensure_ascii=False,
                sort_keys=True,
            )
        cursor = db.execute(
            "INSERT INTO records(capture_id, record_index, gateway, observation, content_hash,"
            " device_id, epoch_id, up_time_state, up_time, printer_uri, scope, notes)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                capture_id,
                index,
                record["gateway"],
                record["observation"],
                record["content_hash"],
                device_id,
                epoch_id,
                up_time["state"],
                up_value if isinstance(up_value, int) and not isinstance(up_value, bool) else None,
                record["printer_uri"],
                scope,
                json.dumps(notes, ensure_ascii=False),
            ),
        )
        record_id = cursor.lastrowid
        summary["records"] += 1

        for group_index, group in enumerate(record["groups"]):
            if group["tag"] != "job-attributes":
                continue
            self._job_group(db, record_id, device_id, epoch_id, group_index, group, capture_id, summary)
        if device_id is not None and epoch_id is not None:
            summary["absences"] += self._absences(db, epoch_id, record_id, record, scope, notes)

    def _device(self, db, record, capture_id, notes):
        uuid, uri = record["printer_uuid"], record["printer_uri"]
        if uuid:
            identity, basis = "uuid:" + uuid, "uuid"
        elif uri:
            identity, basis = "uri:" + uri, "uri"
            notes.append("device_identity_by_uri_only")
        else:
            notes.append("device_unidentified")
            return None
        row = db.execute("SELECT id FROM devices WHERE identity=?", (identity,)).fetchone()
        if row is None:
            cursor = db.execute(
                "INSERT INTO devices(identity, identity_basis, printer_uuid, first_capture)"
                " VALUES(?,?,?,?)",
                (identity, basis, uuid if basis == "uuid" else None, capture_id),
            )
            device_id = cursor.lastrowid
        else:
            device_id = row["id"]
        if uri:
            db.execute(
                "INSERT OR IGNORE INTO device_uris VALUES(?,?,?)", (device_id, uri, capture_id)
            )
        return device_id

    def _epoch(self, db, device_id, up_state, up_value, capture_id, notes):
        current = db.execute(
            "SELECT * FROM epochs WHERE device_id=? ORDER BY seq DESC LIMIT 1", (device_id,)
        ).fetchone()
        usable = (
            up_state == VALUE
            and isinstance(up_value, int)
            and not isinstance(up_value, bool)
        )
        if current is None:
            cursor = db.execute(
                "INSERT INTO epochs(device_id, seq, opened_capture, opened_up_time, max_up_time, reason)"
                " VALUES(?,?,?,?,?,?)",
                (device_id, 1, capture_id, up_value if usable else None, up_value if usable else None,
                 "first_observation"),
            )
            return cursor.lastrowid
        if usable and current["max_up_time"] is not None and up_value < current["max_up_time"]:
            notes.append("printer_restarted:up_time %s->%s" % (current["max_up_time"], up_value))
            cursor = db.execute(
                "INSERT INTO epochs(device_id, seq, opened_capture, opened_up_time, max_up_time, reason)"
                " VALUES(?,?,?,?,?,?)",
                (device_id, current["seq"] + 1, capture_id, up_value, up_value, "up_time_regressed"),
            )
            return cursor.lastrowid
        if usable and (current["max_up_time"] is None or up_value > current["max_up_time"]):
            db.execute("UPDATE epochs SET max_up_time=? WHERE id=?", (up_value, current["id"]))
        if up_state != VALUE:
            notes.append("up_time_" + up_state)
        return current["id"]

    def _job_group(self, db, record_id, device_id, epoch_id, group_index, group, capture_id, summary):
        attributes = group["attributes"]
        raw_group = json.dumps(group["raw"], ensure_ascii=False, sort_keys=True)
        job_id_presence = presence(attributes, "job-id")
        job_id_value = None
        note = None
        if device_id is None or epoch_id is None:
            note = "device_unidentified"
        elif job_id_presence["state"] != VALUE:
            note = "job-id %s" % job_id_presence["state"]
            if job_id_presence["tag"]:
                note += ":" + job_id_presence["tag"]
        else:
            candidate = job_id_presence["values"][0]
            if isinstance(candidate, bool) or not isinstance(candidate, int):
                note = "job-id not an integer"
            else:
                job_id_value = candidate
        if note is not None:
            db.execute(
                "INSERT OR IGNORE INTO unkeyed_observations(record_id, group_index, note, raw_group)"
                " VALUES(?,?,?,?)",
                (record_id, group_index, note, raw_group),
            )
            summary["unkeyed"] += 1
            return

        job = db.execute(
            "SELECT id FROM jobs WHERE epoch_id=? AND job_id=?", (epoch_id, job_id_value)
        ).fetchone()
        if job is None:
            cursor = db.execute(
                "INSERT INTO jobs(device_id, epoch_id, job_id, first_capture, last_record_id)"
                " VALUES(?,?,?,?,?)",
                (device_id, epoch_id, job_id_value, capture_id, record_id),
            )
            job_ref = cursor.lastrowid
        else:
            job_ref = job["id"]
            db.execute("UPDATE jobs SET last_record_id=? WHERE id=?", (record_id, job_ref))
        for column, name in (("job_name", "job-name"), ("originating_user", "job-originating-user-name")):
            found = presence(attributes, name)
            if found["state"] == VALUE:
                db.execute(
                    "UPDATE jobs SET %s=COALESCE(%s, ?) WHERE id=?" % (column, column),
                    (str(found["values"][0]), job_ref),
                )

        state_presence = presence(attributes, "job-state")
        state_value = state_presence["values"][0] if state_presence["state"] == VALUE else None
        if state_value is not None and (isinstance(state_value, bool) or not isinstance(state_value, int)):
            state_value = None
        reasons_presence = presence(attributes, "job-state-reasons")
        reasons = (
            json.dumps([str(value) for value in reasons_presence["values"]], ensure_ascii=False)
            if reasons_presence["state"] == VALUE
            else None
        )
        db.execute(
            "INSERT INTO job_observations(job_ref, record_id, group_index, job_state,"
            " job_state_presence, job_state_tag, state_reasons, attributes, raw_group)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (
                job_ref,
                record_id,
                group_index,
                state_value,
                state_presence["state"],
                state_presence["tag"],
                reasons,
                json.dumps(attributes, ensure_ascii=False, sort_keys=True),
                raw_group,
            ),
        )
        summary["jobs_seen"] += 1

    def _absences(self, db, epoch_id, record_id, record, scope, notes):
        candidates = ["retention_expired_or_history_scope", "query_scope_or_permission"]
        if scope:
            candidates.append("declared_scope:" + scope)
        else:
            candidates.append("listing_scope_undeclared")
        if any(note.startswith("observation_gap") for note in notes):
            candidates.append("fetch_incomplete")
        rows = db.execute(
            "SELECT j.id FROM jobs j WHERE j.epoch_id=? AND j.last_record_id<?"
            " AND NOT EXISTS (SELECT 1 FROM job_observations o WHERE o.job_ref=j.id"
            "  AND o.job_state IN (7,8,9))",
            (epoch_id, record_id),
        ).fetchall()
        for row in rows:
            db.execute(
                "INSERT OR IGNORE INTO absences(job_ref, record_id, candidates) VALUES(?,?,?)",
                (row["id"], record_id, json.dumps(candidates, ensure_ascii=False)),
            )
        return len(rows)

    # ------------------------------------------------------------------
    # 回执
    # ------------------------------------------------------------------

    def report(self, archive, device=None, job_id=None):
        captures = {row["id"]: row for row in archive.captures()}
        db = self.connection
        uri_map = {}
        for row in db.execute("SELECT * FROM device_uris ORDER BY device_id, uri").fetchall():
            uri_map.setdefault(row["device_id"], []).append(row["uri"])
        uri_owners = {}
        for owner, uris in uri_map.items():
            for uri in uris:
                uri_owners.setdefault(uri, set()).add(owner)

        devices_section = []
        device_rows = db.execute("SELECT * FROM devices ORDER BY id").fetchall()
        for row in device_rows:
            uris = uri_map.get(row["id"], [])
            shared = sorted(uri for uri in uris if len(uri_owners.get(uri, ())) > 1)
            devices_section.append(
                {
                    "identity": row["identity"],
                    "identity_basis": row["identity_basis"],
                    "printer_uuid": row["printer_uuid"],
                    "uris": uris,
                    "shared_uri_warning": (
                        "URI 同时出现在其他设备上，疑似设备替换: " + ", ".join(shared)
                        if shared
                        else None
                    ),
                    "first_capture": row["first_capture"],
                }
            )

        query = (
            "SELECT j.*, e.seq AS epoch_seq, e.reason AS epoch_reason, e.opened_capture,"
            " d.identity AS device_identity, d.identity_basis, d.printer_uuid"
            " FROM jobs j JOIN epochs e ON e.id=j.epoch_id JOIN devices d ON d.id=j.device_id"
        )
        clauses, params = [], []
        if device is not None:
            clauses.append("(d.identity=? OR d.printer_uuid=?)")
            params.extend([device, device])
        if job_id is not None:
            clauses.append("j.job_id=?")
            params.append(job_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY j.id"
        receipts = [self._receipt(db, row, captures, uri_map) for row in db.execute(query, params)]

        unattributed = []
        for row in db.execute(
            "SELECT u.*, r.capture_id, r.gateway, r.observation FROM unkeyed_observations u"
            " JOIN records r ON r.id=u.record_id ORDER BY u.id"
        ).fetchall():
            capture = captures.get(row["capture_id"], {})
            unattributed.append(
                {
                    "capture": row["capture_id"],
                    "received_at": capture.get("received_at"),
                    "gateway": row["gateway"],
                    "observation": row["observation"],
                    "group_index": row["group_index"],
                    "note": row["note"],
                    "raw_group": json.loads(row["raw_group"]),
                }
            )
        issues = []
        for row in db.execute(
            "SELECT * FROM record_issues ORDER BY id"
        ).fetchall():
            issues.append(
                {
                    "capture": row["capture_id"],
                    "record_index": row["record_index"],
                    "issue": row["issue"],
                }
            )
        for row in db.execute(
            "SELECT * FROM ingested_captures WHERE note IS NOT NULL ORDER BY capture_id"
        ).fetchall():
            issues.append(
                {"capture": row["capture_id"], "record_index": None, "issue": row["note"]}
            )

        return {
            "generated_at": _now_iso(),
            "collector": {
                "read_only": True,
                "reprint": "not_supported",
                "note": "本层只读采集与解释，不投递文档、不重打、不操作门户",
            },
            "filters": {"device": device, "job_id": job_id},
            "devices": devices_section,
            "receipts": receipts,
            "unverified_receipts": [
                receipt["receipt_id"] for receipt in receipts if receipt["assessment"] == "unverified"
            ],
            "unattributed_observations": unattributed,
            "fetch_and_record_issues": issues,
        }

    def _receipt(self, db, job, captures, uri_map):
        observations = db.execute(
            "SELECT o.*, r.capture_id, r.gateway, r.observation, r.up_time, r.up_time_state"
            " FROM job_observations o JOIN records r ON r.id=o.record_id"
            " WHERE o.job_ref=? ORDER BY o.id",
            (job["id"],),
        ).fetchall()
        absence_rows = db.execute(
            "SELECT a.*, r.capture_id FROM absences a JOIN records r ON r.id=a.record_id"
            " WHERE a.job_ref=? ORDER BY a.id",
            (job["id"],),
        ).fetchall()

        timeline = []
        for row in observations:
            capture = captures.get(row["capture_id"], {})
            state = {"state": row["job_state_presence"], "tag": row["job_state_tag"]}
            if row["job_state"] is not None:
                state["value"] = row["job_state"]
                state["name"] = JOB_STATE_NAMES.get(row["job_state"], "unrecognized-%d" % row["job_state"])
            timeline.append(
                {
                    "capture": row["capture_id"],
                    "received_at": capture.get("received_at"),
                    "gateway": row["gateway"],
                    "observation": row["observation"],
                    "group_index": row["group_index"],
                    "printer_up_time": row["up_time"] if row["up_time_state"] == VALUE else None,
                    "up_time_state": row["up_time_state"],
                    "job_state": state,
                    "job_state_reasons": json.loads(row["state_reasons"]) if row["state_reasons"] else None,
                }
            )

        absences = []
        for row in absence_rows:
            capture = captures.get(row["capture_id"], {})
            absences.append(
                {
                    "capture": row["capture_id"],
                    "received_at": capture.get("received_at"),
                    "candidates": json.loads(row["candidates"]),
                }
            )

        concrete = [row for row in observations if row["job_state"] is not None]
        last_concrete = concrete[-1] if concrete else None
        last_record_id = observations[-1]["record_id"] if observations else None
        later_absences = [
            row for row in absence_rows if last_record_id is None or row["record_id"] > last_record_id
        ]
        max_seq = db.execute(
            "SELECT MAX(seq) AS seq FROM epochs WHERE device_id=?", (job["device_id"],)
        ).fetchone()["seq"]
        epoch_closed = max_seq is not None and job["epoch_seq"] < max_seq

        explanation = []
        terminal_state = None
        if last_concrete is not None and last_concrete["job_state"] in TERMINAL_JOB_STATES:
            assessment = "processed"
            capture = captures.get(last_concrete["capture_id"], {})
            terminal_state = {
                "value": last_concrete["job_state"],
                "name": JOB_STATE_NAMES[last_concrete["job_state"]],
                "capture": last_concrete["capture_id"],
                "received_at": capture.get("received_at"),
            }
            reasons = json.loads(last_concrete["state_reasons"]) if last_concrete["state_reasons"] else []
            explanation.append(
                "capture %s（%s）观察到终态 %s（job-state=%d）"
                % (
                    last_concrete["capture_id"],
                    capture.get("received_at"),
                    terminal_state["name"],
                    last_concrete["job_state"],
                )
            )
            if reasons:
                explanation.append("终态原因: " + ", ".join(reasons))
        elif last_concrete is None:
            assessment = "unverified"
            explanation.append("从未获得具体 job-state（均为带外值或未返回），无法判断处理结果")
        elif epoch_closed or later_absences:
            assessment = "unverified"
            state_name = JOB_STATE_NAMES.get(last_concrete["job_state"], str(last_concrete["job_state"]))
            explanation.append(
                "最后一次具体状态为 %s（capture %s），未见终态"
                % (state_name, last_concrete["capture_id"])
            )
        else:
            assessment = "in_progress"
            state_name = JOB_STATE_NAMES.get(last_concrete["job_state"], str(last_concrete["job_state"]))
            explanation.append("最近观察状态为 %s，仍在队列中" % state_name)
        if assessment == "unverified":
            if epoch_closed:
                explanation.append(
                    "打印机已重启（uptime 回退，见纪元 %d），作业号可能已被回收，终态未知"
                    % (job["epoch_seq"] + 1)
                )
            for row in later_absences:
                explanation.append(
                    "capture %s 起作业不再出现在查询结果，候选原因: %s"
                    % (row["capture_id"], ", ".join(json.loads(row["candidates"])))
                )

        times = {}
        for name in TIME_ATTRIBUTES:
            times[name] = self._time_entry(observations, name, captures)

        evidence_ids = {row["capture_id"] for row in observations}
        evidence_ids.update(row["capture_id"] for row in absence_rows)
        evidence_ids.add(job["opened_capture"])
        evidence = []
        for capture_id in sorted(evidence_ids):
            capture = captures.get(capture_id)
            if capture is None:
                evidence.append({"capture": capture_id, "note": "capture 不在当前归档中"})
                continue
            evidence.append(
                {
                    "capture": capture_id,
                    "digest": capture["digest"],
                    "received_at": capture["received_at"],
                    "status": capture["status"],
                    "url": capture["url"],
                    "source": capture["source"],
                }
            )

        receipt_id = "%s/epoch-%d/job-%d" % (job["device_identity"], job["epoch_seq"], job["job_id"])
        return {
            "receipt_id": receipt_id,
            "device": {
                "identity": job["device_identity"],
                "identity_basis": job["identity_basis"],
                "printer_uuid": job["printer_uuid"],
                "uris": uri_map.get(job["device_id"], []),
            },
            "epoch": {
                "seq": job["epoch_seq"],
                "opened_capture": job["opened_capture"],
                "reason": job["epoch_reason"],
            },
            "job_id": job["job_id"],
            "job_name": job["job_name"],
            "originating_user": job["originating_user"],
            "assessment": assessment,
            "terminal_state": terminal_state,
            "explanation": explanation,
            "processing_times": times,
            "timeline": timeline,
            "absences": absences,
            "evidence": evidence,
        }

    def _time_entry(self, observations, name, captures):
        latest = None
        for row in reversed(observations):
            attributes = json.loads(row["attributes"])
            found = attributes.get(name)
            if found is not None:
                latest = (row, found)
                break
        if latest is None:
            return {"state": ABSENT, "meaning": "各次观察均未返回该属性（可能未请求或不支持）"}
        row, found = latest
        if found["state"] == OUT_OF_BAND:
            tag = found.get("tag")
            return {
                "state": OUT_OF_BAND,
                "tag": tag,
                "meaning": OUT_OF_BAND_MEANINGS.get(tag, "带外值: %s" % tag),
                "capture": row["capture_id"],
            }
        value = found["values"][0] if found["values"] else None
        entry = {"state": VALUE, "tag": found.get("tag"), "value": value, "capture": row["capture_id"]}
        if name.startswith("date-time-at-"):
            entry["absolute"] = value
            return entry
        capture = captures.get(row["capture_id"], {})
        estimate = estimate_wall_clock(capture.get("received_at"), row["up_time"], value)
        if estimate is not None:
            entry["wall_clock_estimate"] = estimate
            entry["estimate_basis"] = {
                "received_at": capture.get("received_at"),
                "printer_up_time": row["up_time"],
                "formula": "received_at - (printer_up_time - value)",
            }
        else:
            entry["wall_clock_estimate"] = None
            entry["estimate_missing"] = "uptime 或时间属性不是具体整数，无法推算挂钟时间"
        return entry
