"""作业资料采集层：把 IPP 属性信封落成可关联、可重放的证据。

设计原则（对应窗口对账诉求）：

1. 属性组是作业的边界。一个观察里的多个 ``job-attributes`` 组分别落库，
   属性绝不跨组摊平；原始组 JSON 原样留存，可按摘要复核。
2. 作业身份不是单个 job-id。打印机重启后 job-id 会复用，因此作业键为
   （设备 UUID、重启世代、job-id）。世代由 printer-up-time 回退等证据
   划分；证据不足（典型为本进程启动前的空档）保守地另开世代并标注
   待核，绝不把两次办件静默合并。
3. 同一 UUID 下出现的多个 printer-uri 是同设备的别名，记入设备 URI 史；
   同一 URI 出现新的 UUID 是设备替换，旧设备的作业保留在旧设备名下。
4. 作业从查询里消失不等于完成。只有 IPP 终态（含 state=9 completed）
   才算有终态证据；消失原因（留存到期/权限变化/本次获取不完整）无法
   区分时，作业进入待核清单，不自动判完成，也不触发重打。
5. 全部表只追加或受控更新，采集进程可重启、多窗口可交错轮询；同一
   (gateway, observation) 的重复送达只增加送达证据，不重复建作业。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import uuid as uuidlib
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .ipp import collect_attribute, integer_value, normalize_tag, view_attribute

SCHEMA_VERSION = 1


def utc_now() -> str:
    """统一的 UTC 时间戳口径（秒后带毫秒，Z 结尾）。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def canonical_json(value: Any) -> str:
    """规范化 JSON 文本，键排序、不转义非 ASCII，用于留档与摘要。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest_of(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require_text(record: dict, name: str) -> str:
    value = record.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


@dataclass
class IngestSummary:
    """一次 :meth:`JobStore.ingest_record` 的结果。"""

    source_observation_id: int
    is_redelivery: bool
    payload_conflict: bool
    device_id: int
    epoch_id: int
    epoch_generation: int
    epoch_reason: str
    groups: int = 0
    linked_groups: int = 0
    unattributed_groups: int = 0
    jobs_seen: list[str] = field(default_factory=list)


class JobStore:
    """作业证据库。与原始响应归档库分开存放，通过 capture_id/摘要互证。"""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # 多窗口可能在同一进程内以线程方式交错轮询；连接允许跨线程，
        # 写事务由 self._write_lock 串行化，多进程场景则由 SQLite 文件
        # 锁与 busy timeout 兜底。
        self._write_lock = threading.RLock()
        self.connection = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=10000")
        self._create_schema()

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        # BEGIN IMMEDIATE 在事务开始即取得保留锁，把跨连接/跨进程的
        # “先查后插”串行化，避免设备与作业唯一键上的竞态插入；拿不到
        # 锁时由 busy_timeout 等待而不是立即报 SQLITE_BUSY。
        with self._write_lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield self.connection
            except Exception:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                pid INTEGER NOT NULL,
                note TEXT
            );
            CREATE TABLE IF NOT EXISTS devices (
                device_id INTEGER PRIMARY KEY,
                printer_uuid TEXT NOT NULL UNIQUE,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS device_uris (
                device_uri_id INTEGER PRIMARY KEY,
                device_id INTEGER NOT NULL REFERENCES devices(device_id),
                printer_uri TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                UNIQUE(device_id, printer_uri)
            );
            CREATE TABLE IF NOT EXISTS device_epochs (
                epoch_id INTEGER PRIMARY KEY,
                device_id INTEGER NOT NULL REFERENCES devices(device_id),
                generation INTEGER NOT NULL,
                opened_at TEXT NOT NULL,
                opened_run_id TEXT,
                open_reason_code TEXT NOT NULL,
                open_reason_detail TEXT NOT NULL,
                continuity_verified INTEGER NOT NULL,
                max_uptime INTEGER,
                last_run_id TEXT,
                last_uptime INTEGER,
                UNIQUE(device_id, generation)
            );
            CREATE TABLE IF NOT EXISTS source_observations (
                source_observation_id INTEGER PRIMARY KEY,
                gateway TEXT NOT NULL,
                observation TEXT NOT NULL,
                device_id INTEGER NOT NULL REFERENCES devices(device_id),
                epoch_id INTEGER NOT NULL REFERENCES device_epochs(epoch_id),
                printer_uri TEXT,
                uptime_presence TEXT NOT NULL,
                uptime_tag TEXT,
                uptime_value INTEGER,
                raw_json TEXT NOT NULL,
                raw_digest TEXT NOT NULL,
                first_delivered_at TEXT NOT NULL,
                UNIQUE(gateway, observation)
            );
            CREATE TABLE IF NOT EXISTS observation_deliveries (
                delivery_id INTEGER PRIMARY KEY,
                source_observation_id INTEGER NOT NULL REFERENCES source_observations(source_observation_id),
                run_id TEXT NOT NULL,
                capture_id INTEGER,
                body_digest TEXT,
                delivered_at TEXT NOT NULL,
                payload_conflict INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS jobs (
                job_row_id INTEGER PRIMARY KEY,
                device_id INTEGER NOT NULL REFERENCES devices(device_id),
                epoch_id INTEGER NOT NULL REFERENCES device_epochs(epoch_id),
                job_id INTEGER NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                UNIQUE(epoch_id, job_id)
            );
            CREATE TABLE IF NOT EXISTS job_groups (
                job_group_id INTEGER PRIMARY KEY,
                source_observation_id INTEGER NOT NULL REFERENCES source_observations(source_observation_id),
                job_row_id INTEGER REFERENCES jobs(job_row_id),
                position INTEGER NOT NULL,
                group_tag TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                raw_digest TEXT NOT NULL,
                UNIQUE(source_observation_id, position)
            );
            CREATE TABLE IF NOT EXISTS group_attributes (
                group_attribute_id INTEGER PRIMARY KEY,
                job_group_id INTEGER NOT NULL REFERENCES job_groups(job_group_id),
                position INTEGER NOT NULL,
                name TEXT NOT NULL,
                value_tag TEXT NOT NULL,
                presence TEXT NOT NULL,
                value_json TEXT,
                UNIQUE(job_group_id, position)
            );
            CREATE TABLE IF NOT EXISTS job_state_snapshots (
                snapshot_id INTEGER PRIMARY KEY,
                job_row_id INTEGER NOT NULL REFERENCES jobs(job_row_id),
                job_group_id INTEGER NOT NULL REFERENCES job_groups(job_group_id),
                source_observation_id INTEGER NOT NULL REFERENCES source_observations(source_observation_id),
                observed_at TEXT NOT NULL,
                state_presence TEXT NOT NULL,
                state_tag TEXT,
                state_value INTEGER,
                reasons_presence TEXT NOT NULL,
                reasons_value_json TEXT,
                completed_presence TEXT NOT NULL,
                completed_tag TEXT,
                completed_value_json TEXT,
                UNIQUE(job_group_id)
            );
            CREATE TABLE IF NOT EXISTS unattributed_groups (
                unattributed_group_id INTEGER PRIMARY KEY,
                source_observation_id INTEGER NOT NULL REFERENCES source_observations(source_observation_id),
                job_group_id INTEGER NOT NULL REFERENCES job_groups(job_group_id),
                reason_code TEXT NOT NULL,
                reason_detail TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS fetch_attempts (
                fetch_attempt_id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL,
                gateway TEXT NOT NULL,
                url TEXT,
                capture_id INTEGER,
                http_status INTEGER,
                complete INTEGER NOT NULL,
                decode_ok INTEGER NOT NULL,
                record_count INTEGER,
                error_code TEXT,
                error_detail TEXT,
                attempted_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_observations_device
                ON source_observations(device_id, source_observation_id);
            CREATE INDEX IF NOT EXISTS idx_snapshots_job
                ON job_state_snapshots(job_row_id, snapshot_id);
            CREATE INDEX IF NOT EXISTS idx_fetch_gateway
                ON fetch_attempts(gateway, fetch_attempt_id);
            CREATE INDEX IF NOT EXISTS idx_groups_job
                ON job_groups(job_row_id);
            """
        )
        self.connection.execute(
            "INSERT OR IGNORE INTO schema_meta(key,value) VALUES('version',?)",
            (str(SCHEMA_VERSION),),
        )
        self.connection.commit()

    # ------------------------------------------------------------------ runs
    def start_run(self, note: str | None = None) -> str:
        """登记一次采集进程启动。进程重启会产生新的 run_id。"""
        run_id = uuidlib.uuid4().hex
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO ingestion_runs(run_id,started_at,pid,note) VALUES(?,?,?,?)",
                (run_id, utc_now(), os.getpid(), note),
            )
        return run_id

    def record_fetch(
        self,
        run_id: str,
        gateway: str,
        *,
        url: str | None = None,
        capture_id: int | None = None,
        http_status: int | None = None,
        complete: bool,
        decode_ok: bool,
        record_count: int | None = None,
        error_code: str | None = None,
        error_detail: str | None = None,
    ) -> int:
        """记录一次轮询尝试的健康状况，无论成功失败都留痕。"""
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO fetch_attempts(run_id,gateway,url,capture_id,http_status,
                    complete,decode_ok,record_count,error_code,error_detail,attempted_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    gateway,
                    url,
                    capture_id,
                    http_status,
                    int(complete),
                    int(decode_ok),
                    record_count,
                    error_code,
                    error_detail,
                    utc_now(),
                ),
            )
        return cursor.lastrowid

    # -------------------------------------------------------------- devices
    #: uptime 回退多少秒以上才判定重启。多窗口交错时观察可能乱序到达，
    #: 小幅回退视为乱序抖动而非重启；真实重启后 uptime 从小值重新计起，
    #: 回退幅度远大于该阈值。阈值作为判定参数随世代原因一并留痕。
    RESTART_UPTIME_DELTA = 60

    def _resolve_epoch(
        self,
        connection: sqlite3.Connection,
        device_id: int,
        uptime_presence: str,
        uptime_value: int | None,
        run_id: str,
        delivered_at: str,
    ) -> tuple[sqlite3.Row, str]:
        """为一条观察确定设备重启世代，返回 (世代行, 本次判定原因)。

        世代划分只依赖可核实的打印机证据（printer-up-time 是开机后单调
        递增的秒数，见 RFC 8011），与采集进程是否重启无关，因此跨进程、
        跨窗口的观察都能得到一致结论：

        - 新 uptime 比最近一次 uptime 回退超过
          :data:`RESTART_UPTIME_DELTA` 秒 → 证实发生过重启，开新世代
          （连续性由 uptime 回退证据证实）；小幅回退按多窗口乱序处理，
          沿用当前世代并保留原始 uptime；
        - 新 uptime 不小于最近值 → 开机后秒数仍然单调，即使中间隔着
          采集进程空档，也能证明没有重启，沿用当前世代；
        - uptime 本次未返回/无值 → 没有任何连续性证据，不新开世代，沿用
          当前世代并保留 missing/no-value 口径；若整台设备从未给过
          uptime，首个世代标 continuity_verified=0 待人工核对。
        """
        epoch = connection.execute(
            "SELECT * FROM device_epochs WHERE device_id=? ORDER BY generation DESC LIMIT 1",
            (device_id,),
        ).fetchone()
        last_uptime = epoch["last_uptime"] if epoch is not None else None

        if epoch is None:
            reason = "first_observation"
            detail = "设备首次出现在采集记录中"
            verified = 1 if uptime_presence == "value" else 0
            cursor = connection.execute(
                """
                INSERT INTO device_epochs(device_id,generation,opened_at,opened_run_id,
                    open_reason_code,open_reason_detail,continuity_verified,
                    max_uptime,last_run_id,last_uptime)
                VALUES(?,1,?,?,?,?,?,?,?,?)
                """,
                (
                    device_id,
                    delivered_at,
                    run_id,
                    reason,
                    detail,
                    verified,
                    uptime_value,
                    run_id,
                    uptime_value,
                ),
            )
            return connection.execute(
                "SELECT * FROM device_epochs WHERE epoch_id=?", (cursor.lastrowid,)
            ).fetchone(), "first_observation"

        restarted = (
            uptime_presence == "value"
            and uptime_value is not None
            and last_uptime is not None
            and last_uptime - uptime_value > self.RESTART_UPTIME_DELTA
        )
        if restarted:
            reason = "uptime_went_backwards"
            detail = (
                f"printer-up-time 由最近的 {last_uptime} 回退到 {uptime_value}，"
                f"回退 {last_uptime - uptime_value} 秒（阈值 {self.RESTART_UPTIME_DELTA} 秒），"
                "判定打印机重启，作业号可能复用"
            )
            cursor = connection.execute(
                """
                INSERT INTO device_epochs(device_id,generation,opened_at,opened_run_id,
                    open_reason_code,open_reason_detail,continuity_verified,
                    max_uptime,last_run_id,last_uptime)
                VALUES(?,?,?,?,?,?,1,?,?,?)
                """,
                (
                    device_id,
                    epoch["generation"] + 1,
                    delivered_at,
                    run_id,
                    reason,
                    detail,
                    uptime_value,
                    run_id,
                    uptime_value,
                ),
            )
            return connection.execute(
                "SELECT * FROM device_epochs WHERE epoch_id=?", (cursor.lastrowid,)
            ).fetchone(), "uptime_went_backwards"

        # 没有重启证据：uptime 单调则连续性成立；uptime 缺失则维持原口径。
        if uptime_presence == "value" and uptime_value is not None:
            previous_max = epoch["max_uptime"]
            new_max = uptime_value if previous_max is None else max(previous_max, uptime_value)
            connection.execute(
                "UPDATE device_epochs SET last_run_id=?, last_uptime=?, max_uptime=?, "
                "continuity_verified=1 WHERE epoch_id=?",
                (run_id, uptime_value, new_max, epoch["epoch_id"]),
            )
        else:
            connection.execute(
                "UPDATE device_epochs SET last_run_id=? WHERE epoch_id=?",
                (run_id, epoch["epoch_id"]),
            )
        return (
            connection.execute(
                "SELECT * FROM device_epochs WHERE epoch_id=?", (epoch["epoch_id"],)
            ).fetchone(),
            "continued",
        )

    # ------------------------------------------------------------- ingestion
    def ingest_record(
        self,
        record: dict,
        run_id: str,
        *,
        capture_id: int | None = None,
        body_digest: str | None = None,
        delivered_at: str | None = None,
    ) -> IngestSummary:
        """落库一条来源观察（一个网关的一次 observation 信封）。

        重复送达 (gateway, observation) 时幂等：不重建设备/作业/属性，只
        追加一条送达证据；若原文摘要与首次不同，标记 payload_conflict。
        """
        gateway = _require_text(record, "gateway")
        observation_raw = record.get("observation")
        if isinstance(observation_raw, bool) or not isinstance(
            observation_raw, (str, int)
        ) or not str(observation_raw).strip():
            raise ValueError("observation must be a non-empty string or integer")
        observation = str(observation_raw).strip()
        printer_uuid = _require_text(record, "printer_uuid")
        printer_uri = _optional_text(record.get("printer_uri"))
        groups = record.get("groups")
        if not isinstance(groups, list):
            raise ValueError("groups must be a list")
        delivered_at = delivered_at or utc_now()

        uptime_view = view_attribute(
            [
                {"name": "printer-up-time", "tag": tag, "value": value}
                for tag, value in [
                    (
                        record.get("printer_up_time_tag", "integer"),
                        record.get("printer_up_time"),
                    )
                ]
            ],
            "printer-up-time",
        )
        # printer_up_time 是信封上的桥接字段；缺失时按 missing 口径保留。
        if "printer_up_time" not in record:
            uptime_presence, uptime_tag, uptime_value = "missing", None, None
        else:
            uptime_presence = uptime_view.presence
            uptime_tag = uptime_view.tag
            uptime_value = integer_value(uptime_view)

        raw_text = canonical_json(record)
        raw_digest = digest_of(raw_text)

        with self.transaction() as connection:
            now = delivered_at
            connection.execute(
                "INSERT OR IGNORE INTO devices(printer_uuid,first_seen_at,last_seen_at) VALUES(?,?,?)",
                (printer_uuid, now, now),
            )
            device_row = connection.execute(
                "SELECT * FROM devices WHERE printer_uuid=?", (printer_uuid,)
            ).fetchone()
            device_id = device_row["device_id"]
            connection.execute(
                "UPDATE devices SET last_seen_at=? WHERE device_id=?", (now, device_id)
            )
            if printer_uri is not None:
                connection.execute(
                    "INSERT OR IGNORE INTO device_uris(device_id,printer_uri,first_seen_at,last_seen_at) VALUES(?,?,?,?)",
                    (device_id, printer_uri, now, now),
                )
                uri_row = connection.execute(
                    "SELECT * FROM device_uris WHERE device_id=? AND printer_uri=?",
                    (device_id, printer_uri),
                ).fetchone()
                connection.execute(
                    "UPDATE device_uris SET last_seen_at=? WHERE device_uri_id=?",
                    (now, uri_row["device_uri_id"]),
                )

            existing = connection.execute(
                "SELECT * FROM source_observations WHERE gateway=? AND observation=?",
                (gateway, observation),
            ).fetchone()
            conflict = int(
                existing is not None and existing["raw_digest"] != raw_digest
            )
            if existing is not None:
                connection.execute(
                    """
                    INSERT INTO observation_deliveries(source_observation_id,run_id,capture_id,
                        body_digest,delivered_at,payload_conflict)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (
                        existing["source_observation_id"],
                        run_id,
                        capture_id,
                        body_digest,
                        now,
                        conflict,
                    ),
                )
                return IngestSummary(
                    source_observation_id=existing["source_observation_id"],
                    is_redelivery=True,
                    payload_conflict=bool(conflict),
                    device_id=existing["device_id"],
                    epoch_id=existing["epoch_id"],
                    epoch_generation=connection.execute(
                        "SELECT generation FROM device_epochs WHERE epoch_id=?",
                        (existing["epoch_id"],),
                    ).fetchone()["generation"],
                    epoch_reason="redelivery",
                )

            epoch, epoch_reason = self._resolve_epoch(
                connection, device_id, uptime_presence, uptime_value, run_id, now
            )
            cursor = connection.execute(
                """
                INSERT INTO source_observations(gateway,observation,device_id,epoch_id,
                    printer_uri,uptime_presence,uptime_tag,uptime_value,raw_json,raw_digest,
                    first_delivered_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    gateway,
                    observation,
                    device_id,
                    epoch["epoch_id"],
                    printer_uri,
                    uptime_presence,
                    uptime_tag,
                    uptime_value,
                    raw_text,
                    raw_digest,
                    now,
                ),
            )
            observation_id = cursor.lastrowid
            connection.execute(
                """
                INSERT INTO observation_deliveries(source_observation_id,run_id,capture_id,
                    body_digest,delivered_at,payload_conflict)
                VALUES(?,?,?,?,?,0)
                """,
                (observation_id, run_id, capture_id, body_digest, now),
            )

            linked = 0
            unattributed = 0
            jobs_seen: list[str] = []
            for position, group in enumerate(groups):
                if not isinstance(group, dict) or not isinstance(group.get("attributes"), list):
                    raise ValueError(f"groups[{position}] must be an object with an attributes list")
                group_tag = group.get("tag")
                if not isinstance(group_tag, str) or not group_tag.strip():
                    raise ValueError(f"groups[{position}].tag must be a non-empty string")
                attributes = group["attributes"]
                for index, attribute in enumerate(attributes):
                    if not isinstance(attribute, dict) or not isinstance(attribute.get("name"), str):
                        raise ValueError(f"groups[{position}].attributes[{index}] is malformed")
                    normalize_tag(attribute["tag"])
                group_text = canonical_json(group)
                group_cursor = connection.execute(
                    """
                    INSERT INTO job_groups(source_observation_id,job_row_id,position,
                        group_tag,raw_json,raw_digest)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (observation_id, None, position, group_tag.strip(), group_text, digest_of(group_text)),
                )
                group_id = group_cursor.lastrowid
                for index, attribute in enumerate(attributes):
                    tag = normalize_tag(attribute["tag"])
                    presence = tag if tag in ("no-value", "unknown", "unsupported") else "value"
                    value_json = (
                        None
                        if presence != "value"
                        else canonical_json(attribute.get("value"))
                    )
                    connection.execute(
                        """
                        INSERT INTO group_attributes(job_group_id,position,name,value_tag,
                            presence,value_json)
                        VALUES(?,?,?,?,?,?)
                        """,
                        (group_id, index, attribute["name"], tag, presence, value_json),
                    )

                job_id_view = view_attribute(attributes, "job-id")
                job_id = integer_value(job_id_view)
                if job_id is None:
                    reason = (
                        f"job-id_{job_id_view.presence}"
                        if job_id_view.presence != "value"
                        else "job-id_not_integer"
                    )
                    connection.execute(
                        """
                        INSERT INTO unattributed_groups(source_observation_id,job_group_id,
                            reason_code,reason_detail)
                        VALUES(?,?,?,?)
                        """,
                        (
                            observation_id,
                            group_id,
                            reason,
                            "属性组缺少可用于关联的整数 job-id，原样留存待人工核对",
                        ),
                    )
                    unattributed += 1
                    continue

                job_row = connection.execute(
                    "SELECT * FROM jobs WHERE epoch_id=? AND job_id=?",
                    (epoch["epoch_id"], job_id),
                ).fetchone()
                if job_row is None:
                    job_cursor = connection.execute(
                        "INSERT INTO jobs(device_id,epoch_id,job_id,first_seen_at,last_seen_at) VALUES(?,?,?,?,?)",
                        (device_id, epoch["epoch_id"], job_id, now, now),
                    )
                    job_row_id = job_cursor.lastrowid
                else:
                    job_row_id = job_row["job_row_id"]
                    connection.execute(
                        "UPDATE jobs SET last_seen_at=? WHERE job_row_id=?",
                        (now, job_row_id),
                    )
                connection.execute(
                    "UPDATE job_groups SET job_row_id=? WHERE job_group_id=?",
                    (job_row_id, group_id),
                )
                linked += 1
                jobs_seen.append(f"{printer_uuid}/gen{epoch['generation']}/job{job_id}")

                state_view = view_attribute(attributes, "job-state")
                reasons_view = collect_attribute(attributes, "job-state-reasons")
                completed_view = view_attribute(attributes, "time-at-completed")
                connection.execute(
                    """
                    INSERT INTO job_state_snapshots(job_row_id,job_group_id,
                        source_observation_id,observed_at,state_presence,state_tag,
                        state_value,reasons_presence,reasons_value_json,
                        completed_presence,completed_tag,completed_value_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        job_row_id,
                        group_id,
                        observation_id,
                        now,
                        state_view.presence,
                        state_view.tag,
                        integer_value(state_view),
                        reasons_view.presence,
                        canonical_json(reasons_view.value)
                        if reasons_view.presence == "value"
                        else None,
                        completed_view.presence,
                        completed_view.tag,
                        canonical_json(completed_view.value)
                        if completed_view.presence == "value"
                        else None,
                    ),
                )

        return IngestSummary(
            source_observation_id=observation_id,
            is_redelivery=False,
            payload_conflict=False,
            device_id=device_id,
            epoch_id=epoch["epoch_id"],
            epoch_generation=epoch["generation"],
            epoch_reason=epoch_reason,
            groups=len(groups),
            linked_groups=linked,
            unattributed_groups=unattributed,
            jobs_seen=jobs_seen,
        )

    # ------------------------------------------------------------- accessors
    def devices(self) -> list[dict]:
        rows = self.connection.execute(
            "SELECT * FROM devices ORDER BY device_id"
        ).fetchall()
        result = []
        for row in rows:
            uris = [
                dict(item)
                for item in self.connection.execute(
                    "SELECT printer_uri,first_seen_at,last_seen_at FROM device_uris WHERE device_id=? ORDER BY device_uri_id",
                    (row["device_id"],),
                ).fetchall()
            ]
            epochs = [
                dict(item)
                for item in self.connection.execute(
                    "SELECT * FROM device_epochs WHERE device_id=? ORDER BY generation",
                    (row["device_id"],),
                ).fetchall()
            ]
            result.append({**dict(row), "uris": uris, "epochs": epochs})
        return result

    def raw_observation(self, source_observation_id: int) -> dict:
        row = self.connection.execute(
            "SELECT raw_json,raw_digest FROM source_observations WHERE source_observation_id=?",
            (source_observation_id,),
        ).fetchone()
        if row is None:
            raise KeyError(source_observation_id)
        payload = json.loads(row["raw_json"])
        if digest_of(canonical_json(payload)) != row["raw_digest"]:
            raise ValueError("stored observation digest mismatch")
        return payload
