import argparse
import json
import tempfile
from pathlib import Path

from .archive import Archive
from .client import Client, collect
from .decoding import records
from .ledger import JobLedger
from .origin import serve


def capture(base_url, archive_path, source, path):
    archive = Archive(archive_path)
    try:
        capture_id, response = collect(Client(base_url), archive, source, path)
        result = {"capture_id": capture_id, "status": response.status, "bytes": len(response.body), "url": response.url}
        if response.status == 200:
            result["source_records"] = records(response.body, response.headers.get("content-type", ""))
        result["archived_observations"] = len(archive.captures())
        return result
    finally:
        archive.close()


def ingest(archive_path, ledger_path, source):
    archive = Archive(archive_path)
    ledger = JobLedger(ledger_path)
    try:
        summaries = ledger.ingest_all(archive, source)
        fresh = [item for item in summaries if not item["skipped"]]
        return {
            "captures_seen": len(summaries),
            "captures_ingested": len(fresh),
            "captures_skipped": len(summaries) - len(fresh),
            "records": sum(item["records"] for item in fresh),
            "jobs_seen": sum(item["jobs_seen"] for item in fresh),
            "absences": sum(item["absences"] for item in fresh),
            "redeliveries": sum(item["redeliveries"] for item in fresh),
            "unkeyed": sum(item["unkeyed"] for item in fresh),
            "issues": sum(item["issues"] for item in fresh),
            "captures": summaries,
        }
    finally:
        ledger.close()
        archive.close()


def report(archive_path, ledger_path, device, job_id):
    archive = Archive(archive_path)
    ledger = JobLedger(ledger_path)
    try:
        return ledger.report(archive, device=device, job_id=job_id)
    finally:
        ledger.close()
        archive.close()


def main():
    parser = argparse.ArgumentParser(description="来源响应接收与归档")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("demo")
    collector = subcommands.add_parser("capture")
    collector.add_argument("--base-url", required=True)
    collector.add_argument("--archive", type=Path, required=True)
    collector.add_argument("--source", required=True)
    collector.add_argument("--path", default="/feed")
    ingester = subcommands.add_parser("ingest", help="把归档抓取摄入作业台账（幂等，可重复执行）")
    ingester.add_argument("--archive", type=Path, required=True)
    ingester.add_argument("--ledger", type=Path, required=True)
    ingester.add_argument("--source")
    reporter = subcommands.add_parser("report", help="输出作业回执与待核清单")
    reporter.add_argument("--archive", type=Path, required=True)
    reporter.add_argument("--ledger", type=Path, required=True)
    reporter.add_argument("--device")
    reporter.add_argument("--job-id", type=int)
    server = subcommands.add_parser("serve")
    server.add_argument("--scenario", type=Path)
    args = parser.parse_args()
    if args.command == "demo":
        with tempfile.TemporaryDirectory() as temporary, serve() as origin:
            result = capture(origin.base_url, Path(temporary) / "archive.sqlite3", "local-source", "/feed")
            print(json.dumps({"operation": "raw_capture", **result}, ensure_ascii=False, indent=2))
    elif args.command == "capture":
        print(json.dumps(capture(args.base_url, args.archive, args.source, args.path), ensure_ascii=False, indent=2))
    elif args.command == "ingest":
        print(json.dumps(ingest(args.archive, args.ledger, args.source), ensure_ascii=False, indent=2))
    elif args.command == "report":
        print(json.dumps(report(args.archive, args.ledger, args.device, args.job_id), ensure_ascii=False, indent=2))
    else:
        from .origin import load_scenario
        with serve(load_scenario(args.scenario)) as origin:
            print(json.dumps({"base_url": origin.base_url}, ensure_ascii=False), flush=True)
            try:
                import threading
                threading.Event().wait()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
