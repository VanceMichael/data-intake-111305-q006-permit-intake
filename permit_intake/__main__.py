import argparse
import json
import tempfile
from pathlib import Path

from .archive import Archive
from .client import Client, collect
from .decoding import records
from .ingest import poll_once, replay_archive
from .jobstore import JobStore
from .origin import serve
from .reconcile import build_report


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


def poll(base_url, archive_path, store_path, gateway, path):
    archive = Archive(archive_path)
    store = JobStore(store_path)
    try:
        run_id = store.start_run(note="poll")
        result = poll_once(
            Client(base_url), archive, store, run_id, gateway, path
        ).as_dict()
        result["run_id"] = run_id
        result["report_counts"] = build_report(store)["counts"]
        return result
    finally:
        store.close()
        archive.close()


def replay(archive_path, store_path, source):
    archive = Archive(archive_path)
    store = JobStore(store_path)
    try:
        run_id = store.start_run(note="replay")
        replayed = replay_archive(archive, store, run_id, source)
        return {
            "run_id": run_id,
            "replayed_captures": len(replayed),
            "details": replayed,
            "report_counts": build_report(store)["counts"],
        }
    finally:
        store.close()
        archive.close()


def report(store_path):
    store = JobStore(store_path)
    try:
        return build_report(store)
    finally:
        store.close()


def demo_jobs(scenario_path=None):
    from .origin import load_scenario

    scenario = load_scenario(scenario_path) if scenario_path else load_scenario()
    with tempfile.TemporaryDirectory() as temporary:
        archive_path = Path(temporary) / "raw.sqlite3"
        store_path = Path(temporary) / "jobs.sqlite3"
        archive = Archive(archive_path)
        store = JobStore(store_path)
        try:
            run_id = store.start_run(note="demo-jobs")
            with serve(scenario) as origin:
                client = Client(origin.base_url)
                feed = scenario.get("routes", {}).get("/feed")
                steps = len(feed) if isinstance(feed, list) else 1
                polls = [
                    poll_once(client, archive, store, run_id, "window-a", "/feed").as_dict()
                    for _ in range(steps)
                ]
            reconciliation = build_report(store)
            return {
                "operation": "job_collection_demo",
                "polls": polls,
                "report_counts": reconciliation["counts"],
                "confirmed_processed": [
                    {
                        "job": entry["job_identity"]["composite_key"],
                        "processing_time": entry.get("processing_time"),
                    }
                    for entry in reconciliation["confirmed_processed"]
                ],
                "unverified": [
                    {
                        "job": entry["job_identity"]["composite_key"],
                        "reason_code": entry["reason_code"],
                        "reason": entry["reason"],
                    }
                    for entry in reconciliation["unverified"]
                ],
                "unattributed_groups": [
                    {"reason_code": item["reason_code"], "gateway": item["gateway"],
                     "observation": item["observation"]}
                    for item in reconciliation["unattributed_groups"]
                ],
            }
        finally:
            store.close()
            archive.close()


def main():
    parser = argparse.ArgumentParser(description="来源响应接收与 IPP 作业采集对账")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("demo")
    subcommands.add_parser("demo-jobs")
    collector = subcommands.add_parser("capture")
    collector.add_argument("--base-url", required=True)
    collector.add_argument("--archive", type=Path, required=True)
    collector.add_argument("--source", required=True)
    collector.add_argument("--path", default="/feed")
    poller = subcommands.add_parser("poll")
    poller.add_argument("--base-url", required=True)
    poller.add_argument("--archive", type=Path, required=True)
    poller.add_argument("--store", type=Path, required=True)
    poller.add_argument("--gateway", required=True)
    poller.add_argument("--path", default="/feed")
    replayer = subcommands.add_parser("replay")
    replayer.add_argument("--archive", type=Path, required=True)
    replayer.add_argument("--store", type=Path, required=True)
    replayer.add_argument("--source")
    reporter = subcommands.add_parser("report")
    reporter.add_argument("--store", type=Path, required=True)
    server = subcommands.add_parser("serve")
    server.add_argument("--scenario", type=Path)
    args = parser.parse_args()
    if args.command == "demo":
        with tempfile.TemporaryDirectory() as temporary, serve() as origin:
            result = capture(origin.base_url, Path(temporary) / "archive.sqlite3", "local-source", "/feed")
            print(json.dumps({"operation": "raw_capture", **result}, ensure_ascii=False, indent=2))
    elif args.command == "demo-jobs":
        print(json.dumps(demo_jobs(), ensure_ascii=False, indent=2))
    elif args.command == "capture":
        print(json.dumps(capture(args.base_url, args.archive, args.source, args.path), ensure_ascii=False, indent=2))
    elif args.command == "poll":
        print(json.dumps(poll(args.base_url, args.archive, args.store, args.gateway, args.path), ensure_ascii=False, indent=2))
    elif args.command == "replay":
        print(json.dumps(replay(args.archive, args.store, args.source), ensure_ascii=False, indent=2))
    elif args.command == "report":
        print(json.dumps(report(args.store), ensure_ascii=False, indent=2))
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
