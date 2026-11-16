"""抓取编排：一次轮询如何同时写入原始归档与作业证据库。

轮询由多个窗口（gateway）各自发起、可能交错；本模块保证：

- 原始响应仍由 :class:`~permit_intake.archive.Archive` 只追加保存，作业库
  通过 capture_id 与正文摘要回指原文；
- 网络失败、HTTP 非 200、解码失败、正文不完整都会在 ``fetch_attempts``
  留痕，且不会被当成“队列里没有作业”；
- 同一份归档可以在新的证据库上 :func:`replay_archive` 重放，过程幂等。
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from http.client import IncompleteRead
from urllib.error import URLError

from .archive import Archive
from .client import Client
from .decoding import records as decode_records
from .jobstore import JobStore


@dataclass
class PollResult:
    gateway: str
    ok: bool
    capture_id: int | None = None
    http_status: int | None = None
    record_count: int = 0
    ingested: list[dict] = field(default_factory=list)
    error_code: str | None = None
    error_detail: str | None = None

    def as_dict(self) -> dict:
        return {
            "gateway": self.gateway,
            "ok": self.ok,
            "capture_id": self.capture_id,
            "http_status": self.http_status,
            "record_count": self.record_count,
            "ingested": self.ingested,
            "error_code": self.error_code,
            "error_detail": self.error_detail,
        }


def poll_once(
    client: Client,
    archive: Archive,
    store: JobStore,
    run_id: str,
    gateway: str,
    path: str = "/feed",
) -> PollResult:
    """执行一次轮询并双写归档与作业库；任何失败都记录后返回，不抛出。"""
    url = client.base_url + path.lstrip("/")
    try:
        response = client.fetch(path)
    except IncompleteRead as error:
        store.record_fetch(
            run_id, gateway, url=url, complete=False, decode_ok=False,
            error_code="incomplete_read", error_detail=str(error),
        )
        return PollResult(gateway, False, error_code="incomplete_read", error_detail=str(error))
    except (URLError, OSError) as error:
        store.record_fetch(
            run_id, gateway, url=url, complete=False, decode_ok=False,
            error_code="network_error", error_detail=str(error),
        )
        return PollResult(gateway, False, error_code="network_error", error_detail=str(error))

    capture_id = archive.append(
        gateway, response.url, response.status, response.headers, response.body
    )
    body_digest = hashlib.sha256(response.body).hexdigest()

    if response.status != 200:
        store.record_fetch(
            run_id, gateway, url=response.url, capture_id=capture_id,
            http_status=response.status, complete=True, decode_ok=False,
            error_code="http_non_200",
            error_detail=f"HTTP {response.status}，本次获取不产生作业观察",
        )
        return PollResult(
            gateway, False, capture_id=capture_id, http_status=response.status,
            error_code="http_non_200", error_detail=f"HTTP {response.status}",
        )

    try:
        decoded = decode_records(response.body, response.headers.get("content-type", ""))
    except (ValueError, UnicodeDecodeError) as error:
        store.record_fetch(
            run_id, gateway, url=response.url, capture_id=capture_id,
            http_status=response.status, complete=True, decode_ok=False,
            error_code="decode_failed", error_detail=str(error),
        )
        return PollResult(
            gateway, False, capture_id=capture_id, http_status=response.status,
            error_code="decode_failed", error_detail=str(error),
        )

    ingested = []
    for record in decoded:
        summary = store.ingest_record(
            record, run_id, capture_id=capture_id, body_digest=body_digest
        )
        ingested.append(
            {
                "source_observation_id": summary.source_observation_id,
                "redelivery": summary.is_redelivery,
                "payload_conflict": summary.payload_conflict,
                "epoch_generation": summary.epoch_generation,
                "epoch_reason": summary.epoch_reason,
                "groups": summary.groups,
                "linked_groups": summary.linked_groups,
                "unattributed_groups": summary.unattributed_groups,
                "jobs_seen": summary.jobs_seen,
            }
        )
    store.record_fetch(
        run_id, gateway, url=response.url, capture_id=capture_id,
        http_status=response.status, complete=True, decode_ok=True,
        record_count=len(decoded),
    )
    return PollResult(
        gateway, True, capture_id=capture_id, http_status=response.status,
        record_count=len(decoded), ingested=ingested,
    )


def replay_archive(
    archive: Archive,
    store: JobStore,
    run_id: str,
    source: str | None = None,
) -> list[dict]:
    """从原始归档重放全部（或指定 source 的）成功响应，重建作业证据。

    重放对同一证据库幂等：重复观察只追加送达记录。归档里没有成功解码
    依据的抓取不会凭空生成作业观察。
    """
    results = []
    for capture in archive.captures(source):
        if capture["status"] != 200:
            continue
        body = archive.body(capture["id"])
        content_type = ""
        for name, value in capture["headers"].items():
            if name.lower() == "content-type":
                content_type = value
        try:
            decoded = decode_records(body, content_type)
        except (ValueError, UnicodeDecodeError):
            store.record_fetch(
                run_id, capture["source"], url=capture["url"], capture_id=capture["id"],
                http_status=capture["status"], complete=True, decode_ok=False,
                error_code="decode_failed_on_replay",
            )
            continue
        ingested = []
        for record in decoded:
            summary = store.ingest_record(
                record,
                run_id,
                capture_id=capture["id"],
                body_digest=hashlib.sha256(body).hexdigest(),
            )
            ingested.append(
                {
                    "source_observation_id": summary.source_observation_id,
                    "redelivery": summary.is_redelivery,
                    "epoch_generation": summary.epoch_generation,
                    "epoch_reason": summary.epoch_reason,
                }
            )
        store.record_fetch(
            run_id, capture["source"], url=capture["url"], capture_id=capture["id"],
            http_status=capture["status"], complete=True, decode_ok=True,
            record_count=len(decoded),
        )
        results.append({"capture_id": capture["id"], "ingested": ingested})
    return results
