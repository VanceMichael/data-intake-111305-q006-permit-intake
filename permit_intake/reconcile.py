"""作业对账：把证据库解释成窗口可核对的办件清单。

本模块只做只读解释，不产生布尔式“成功”结论：

- ``confirmed_processed``：存在 job-state=9 (completed) 的属性组证据，
  并尽量给出可核实的处理时间；
- ``unverified``：没有 completed 终态证据的作业，包括仍在队列、取消/
  中止、以及从查询消失但无法区分原因（留存到期、查询权限变化、本次
  获取不完整）的作业，逐条列出待人工核对，系统不替人重打；
- ``unattributed_groups``：无法关联到 job-id 的原始属性组，同样保留。

每个结论都附带指向原始属性组/观察/抓取尝试的证据引用，可一路回溯到
原始归档正文。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from .ipp import JOB_STATE_NAMES, TERMINAL_JOB_STATES
from .jobstore import JobStore

REASON_LABELS = {
    "completed_observed": "观察到 IPP completed 终态",
    "still_in_queue": "最近一次完整获取仍能观察到该作业，未见终态",
    "terminal_without_completion": "观察到取消/中止终态，不代表纸张已输出",
    "absent_after_complete_fetch": "最近一次完整获取已无该作业，但缺少终态证据（留存到期或查询权限变化均可能），保持待核",
    "absence_unverified_incomplete_fetch": "作业不再出现，但最近获取不完整，不能据此认定完成",
    "never_observed_complete_fetch": "尚无完整获取可用于判断作业是否仍在队列",
    "epoch_continuity_unverified": "作业所在重启世代的连续性未经证实，需人工确认是否与历史办件混同",
}

TERMINAL_REASON = {
    7: "terminal_without_completion",
    8: "terminal_without_completion",
}


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _latest_complete_fetch(store: JobStore, gateway: str) -> dict | None:
    row = store.connection.execute(
        """
        SELECT * FROM fetch_attempts
        WHERE gateway=? AND complete=1 AND decode_ok=1
        ORDER BY fetch_attempt_id DESC LIMIT 1
        """,
        (gateway,),
    ).fetchone()
    return dict(row) if row is not None else None


def _latest_fetch(store: JobStore, gateway: str) -> dict | None:
    row = store.connection.execute(
        "SELECT * FROM fetch_attempts WHERE gateway=? ORDER BY fetch_attempt_id DESC LIMIT 1",
        (gateway,),
    ).fetchone()
    return dict(row) if row is not None else None


def _completed_wall_time(store: JobStore, job_row_id: int) -> dict:
    """用 time-at-completed（IPP 打印机开机秒数）推导可核实的墙上时间。

    推导需要同一设备上带整数 printer-up-time 的观察；两者之差与观察
    送达时间组合得到 UTC 估计。任一证据缺失或不一致时，只保留原始
    IPP 时间并标记无法核实墙上时间，绝不编造处理时间。
    """
    snapshot = store.connection.execute(
        """
        SELECT s.*, o.uptime_value, o.uptime_presence
        FROM job_state_snapshots s
        JOIN source_observations o ON o.source_observation_id=s.source_observation_id
        WHERE s.job_row_id=? AND s.state_value=9
        ORDER BY s.snapshot_id LIMIT 1
        """,
        (job_row_id,),
    ).fetchone()
    if snapshot is None:
        return {"status": "no_completed_snapshot"}

    completed_presence = snapshot["completed_presence"]
    if completed_presence != "value":
        return {
            "status": "ipp_time_unavailable",
            "presence": completed_presence,
            "detail": "time-at-completed 未返回可取值，无法给出处理时间",
            "snapshot_id": snapshot["snapshot_id"],
            "source_observation_id": snapshot["source_observation_id"],
        }
    ipp_completed = json.loads(snapshot["completed_value_json"])
    if isinstance(ipp_completed, bool) or not isinstance(ipp_completed, int) or ipp_completed < 0:
        return {
            "status": "ipp_time_not_integer",
            "raw": snapshot["completed_value_json"],
            "snapshot_id": snapshot["snapshot_id"],
            "source_observation_id": snapshot["source_observation_id"],
        }

    # 取同设备、时间不晚于该快照观察、uptime 为整数的观察作为时钟基准。
    basis = store.connection.execute(
        """
        SELECT o.source_observation_id, o.uptime_value, d.delivered_at
        FROM source_observations o
        JOIN observation_deliveries d ON d.source_observation_id=o.source_observation_id
        JOIN jobs j ON j.device_id=o.device_id
        WHERE j.job_row_id=? AND o.uptime_presence='value' AND o.uptime_value IS NOT NULL
          AND o.source_observation_id<=?
        ORDER BY o.source_observation_id DESC, d.delivery_id LIMIT 1
        """,
        (job_row_id, snapshot["source_observation_id"]),
    ).fetchone()
    if basis is None:
        return {
            "status": "wall_time_unverifiable",
            "ipp_time_at_completed_seconds": ipp_completed,
            "detail": "缺少带 printer-up-time 的观察，无法把开机秒数换算成墙上时间",
            "snapshot_id": snapshot["snapshot_id"],
            "source_observation_id": snapshot["source_observation_id"],
        }

    observed_at = _parse_ts(snapshot["observed_at"])
    # time-at-completed 与基准 uptime 同为开机后秒数，差值即处理完成距
    # 基准观察的秒数；完成时刻不应晚于观察送达时刻。
    elapsed = basis["uptime_value"] - ipp_completed
    estimated = _parse_ts(basis["delivered_at"]) - timedelta(seconds=elapsed)
    consistent = estimated <= observed_at + timedelta(seconds=1)
    return {
        "status": "estimated" if consistent else "estimated_inconsistent",
        "ipp_time_at_completed_seconds": ipp_completed,
        "estimated_completed_at": _iso(estimated),
        "clock_basis_observation_id": basis["source_observation_id"],
        "clock_basis_uptime_seconds": basis["uptime_value"],
        "clock_basis_delivered_at": basis["delivered_at"],
        "observed_at": snapshot["observed_at"],
        "monotonic_check": "pass" if consistent else "fail: completion later than observation",
        "note": "估计值由 IPP 开机秒数与观察送达时间推导，原始秒数同时保留可复核",
    }


def _snapshot_history(store: JobStore, job_row_id: int) -> list[dict]:
    rows = store.connection.execute(
        """
        SELECT s.snapshot_id, s.source_observation_id, s.observed_at,
               s.state_presence, s.state_tag, s.state_value,
               s.reasons_presence, s.reasons_value_json,
               s.completed_presence, s.completed_value_json, s.job_group_id,
               o.gateway, o.observation
        FROM job_state_snapshots s
        JOIN source_observations o ON o.source_observation_id=s.source_observation_id
        WHERE s.job_row_id=? ORDER BY s.snapshot_id
        """,
        (job_row_id,),
    ).fetchall()
    history = []
    for row in rows:
        item = dict(row)
        if item["state_value"] is not None:
            item["state_name"] = JOB_STATE_NAMES.get(item["state_value"], "unknown-enum")
        if item["reasons_value_json"] is not None:
            item["reasons_value"] = json.loads(item["reasons_value_json"])
        item.pop("reasons_value_json", None)
        history.append(item)
    return history


def _evidence_refs(store: JobStore, job_row_id: int) -> dict:
    captures = store.connection.execute(
        """
        SELECT DISTINCT d.capture_id, d.body_digest, d.delivered_at, o.gateway, o.observation
        FROM observation_deliveries d
        JOIN source_observations o ON o.source_observation_id=d.source_observation_id
        JOIN job_groups g ON g.source_observation_id=o.source_observation_id
        WHERE g.job_row_id=? AND d.capture_id IS NOT NULL
        ORDER BY d.delivery_id
        """,
        (job_row_id,),
    ).fetchall()
    groups = store.connection.execute(
        """
        SELECT g.job_group_id, g.source_observation_id, g.position, g.raw_digest
        FROM job_groups g WHERE g.job_row_id=? ORDER BY g.job_group_id
        """,
        (job_row_id,),
    ).fetchall()
    return {
        "raw_groups": [dict(row) for row in groups],
        "archive_captures": [dict(row) for row in captures],
    }


def _classify(store: JobStore, job: dict) -> tuple[str, str, dict | None]:
    """返回 (分区, 原因码, 时间证据)。不输出任何布尔成功值。"""
    snapshots = store.connection.execute(
        "SELECT * FROM job_state_snapshots WHERE job_row_id=? ORDER BY snapshot_id",
        (job["job_row_id"],),
    ).fetchall()
    latest = snapshots[-1]
    gateway = store.connection.execute(
        """
        SELECT o.gateway FROM source_observations o
        JOIN job_groups g ON g.source_observation_id=o.source_observation_id
        WHERE g.job_row_id=? ORDER BY g.job_group_id LIMIT 1
        """,
        (job["job_row_id"],),
    ).fetchone()["gateway"]

    if latest["state_value"] == 9:
        return "confirmed_processed", "completed_observed", _completed_wall_time(
            store, job["job_row_id"]
        )

    if latest["state_value"] in TERMINAL_JOB_STATES:
        return "unverified", TERMINAL_REASON[latest["state_value"]], None

    # 非终态：判断作业当前是否还能被观察到。
    latest_complete = _latest_complete_fetch(store, gateway)
    if latest_complete is None or latest_complete["capture_id"] is None:
        return "unverified", "never_observed_complete_fetch", None

    seen_in_latest_complete = store.connection.execute(
        """
        SELECT 1
        FROM job_groups g
        JOIN source_observations o ON o.source_observation_id=g.source_observation_id
        JOIN observation_deliveries d ON d.source_observation_id=o.source_observation_id
        WHERE g.job_row_id=? AND d.capture_id=?
        LIMIT 1
        """,
        (job["job_row_id"], latest_complete["capture_id"]),
    ).fetchone()
    if seen_in_latest_complete:
        return "unverified", "still_in_queue", None

    latest_any = _latest_fetch(store, gateway)
    if latest_any is not None and not latest_any["complete"]:
        return "unverified", "absence_unverified_incomplete_fetch", None
    return "unverified", "absent_after_complete_fetch", None


def build_report(store: JobStore) -> dict[str, Any]:
    """生成对账报告（纯只读），三个分区各自列出依据。"""
    devices = store.devices()
    jobs_rows = store.connection.execute(
        """
        SELECT j.*, d.printer_uuid, e.generation, e.continuity_verified,
               e.open_reason_code, e.open_reason_detail
        FROM jobs j
        JOIN devices d ON d.device_id=j.device_id
        JOIN device_epochs e ON e.epoch_id=j.epoch_id
        ORDER BY j.job_row_id
        """
    ).fetchall()

    confirmed: list[dict] = []
    unverified: list[dict] = []
    for job_row in jobs_rows:
        job = dict(job_row)
        section, reason_code, timing = _classify(store, job)
        entry = {
            "job_identity": {
                "printer_uuid": job["printer_uuid"],
                "epoch_generation": job["generation"],
                "job_id": job["job_id"],
                "composite_key": f"{job['printer_uuid']}/gen{job['generation']}/job{job['job_id']}",
            },
            "epoch_evidence": {
                "continuity_verified": bool(job["continuity_verified"]),
                "open_reason_code": job["open_reason_code"],
                "open_reason_detail": job["open_reason_detail"],
            },
            "first_seen_at": job["first_seen_at"],
            "last_seen_at": job["last_seen_at"],
            "reason_code": reason_code,
            "reason": REASON_LABELS[reason_code],
            "state_history": _snapshot_history(store, job["job_row_id"]),
            "evidence": _evidence_refs(store, job["job_row_id"]),
        }
        if timing is not None:
            entry["processing_time"] = timing
        if not job["continuity_verified"]:
            entry["caveats"] = [REASON_LABELS["epoch_continuity_unverified"]]
        if section == "confirmed_processed":
            confirmed.append(entry)
        else:
            unverified.append(entry)

    unattributed_rows = store.connection.execute(
        """
        SELECT u.*, o.gateway, o.observation, g.raw_json, g.raw_digest
        FROM unattributed_groups u
        JOIN source_observations o ON o.source_observation_id=u.source_observation_id
        JOIN job_groups g ON g.job_group_id=u.job_group_id
        ORDER BY u.unattributed_group_id
        """
    ).fetchall()
    unattributed = []
    for row in unattributed_rows:
        item = dict(row)
        item["raw_group"] = json.loads(item.pop("raw_json"))
        unattributed.append(item)

    conflicts = [
        dict(row)
        for row in store.connection.execute(
            "SELECT * FROM observation_deliveries WHERE payload_conflict=1 ORDER BY delivery_id"
        ).fetchall()
    ]
    fetch_health = [
        {
            "gateway": gateway,
            "latest_attempt": _latest_fetch(store, gateway),
            "latest_complete_attempt": _latest_complete_fetch(store, gateway),
        }
        for gateway in [
            row["gateway"]
            for row in store.connection.execute(
                "SELECT DISTINCT gateway FROM fetch_attempts ORDER BY gateway"
            ).fetchall()
        ]
    ]

    return {
        "generated_at": store.connection.execute(
            "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')"
        ).fetchone()[0],
        "policy": {
            "completion_requires_ipp_terminal_state": True,
            "disappearance_is_not_completion": True,
            "no_automatic_reprint": True,
            "value_tag_distinctions": ["value", "no-value", "unknown", "unsupported", "missing"],
            "terminal_states": {
                code: JOB_STATE_NAMES[code] for code in TERMINAL_JOB_STATES
            },
        },
        "counts": {
            "confirmed_processed": len(confirmed),
            "unverified": len(unverified),
            "unattributed_groups": len(unattributed),
            "payload_conflicts": len(conflicts),
        },
        "devices": devices,
        "fetch_health": fetch_health,
        "confirmed_processed": confirmed,
        "unverified": unverified,
        "unattributed_groups": unattributed,
        "payload_conflicts": conflicts,
    }
