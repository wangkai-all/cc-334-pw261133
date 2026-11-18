#!/usr/bin/env python3
"""End-to-end scenario suite against a running stack (portal/collector/worker/vbrowser).

Run after scripts/start-all.sh. Each scenario asserts the correlation invariants
from docs/protocol.md: no filename/recency pairing, three-factor verification,
cancelled-task late files never overwrite, idempotent ACK loss recovery.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

BASE = {
    "portal": "http://127.0.0.1:8080",
    "collector": "http://127.0.0.1:8090",
    "worker": "http://127.0.0.1:8091",
    "vb": "http://127.0.0.1:8095",
}
PASS, FAIL = [], []


def call(service: str, method: str, path: str, body=None):
    url = BASE[service] + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def check(name: str, cond: bool, detail: str = ""):
    (PASS if cond else FAIL).append(name)
    print(f"{'PASS' if cond else 'FAIL'}  {name}" + (f"  — {detail}" if detail and not cond else ""))


def make_task(detail: str, report="sales", region="north"):
    _, t = call("portal", "POST", "/api/tasks",
                {"filter": {"report": report, "region": region,
                            "period": "2026-11", "detail": detail}})
    return t


def vb_download(task):
    _, out = call("vb", "POST", "/download", {"task": {
        "task_id": task["task_id"], "token": task["token"],
        "filter": task["filter"], "filename": task["filename"]}})
    return out


def wait_ledger(did, want=None, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, led = call("worker", "GET", "/ledger")
        for row in led.get("ingests", []):
            if row["download_id"] == did and (want is None or row["status"] == want):
                return row
        time.sleep(0.4)
    return None


def wait_envelope(did, want=None, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, env = call("collector", "GET", f"/api/envelopes/{did}")
        if code == 200 and (want is None or env["status"] == want):
            return env
        time.sleep(0.4)
    return None


def scenario_same_name_concurrent():
    print("\n=== 1. 同名并发下载：不得凭文件名/最近点击配对 ===")
    t1, t2 = make_task("concurrent-A", region="north"), make_task("concurrent-B", region="south")
    assert t1["filename"] == t2["filename"], "fixture must share a filename"
    d1 = vb_download(t1)
    d2 = vb_download(t2)
    r1 = wait_ledger(d1["download_id"], "ingested")
    r2 = wait_ledger(d2["download_id"], "ingested")
    check("two distinct download_ids", d1["download_id"] != d2["download_id"])
    check("both ingested", bool(r1 and r2))
    if r1 and r2:
        check("file A stored under task A", r1["task_id"] == t1["task_id"] and r1["library_path"])
        check("file B stored under task B", r2["task_id"] == t2["task_id"])
        check("separate content-addressed paths",
              r1["library_path"] != r2["library_path"])
        check("filters preserved on ledger rows",
              json.loads(r1["filter_json"])["region"] == "north"
              and json.loads(r2["filter_json"])["region"] == "south")


def scenario_cancel_then_new():
    print("\n=== 2. 取消旧任务后立即新建：迟到文件不得覆盖新结果 ===")
    import threading
    old = make_task("cancel-old")
    # Deterministic in-flight window: arm the gate, start the download (it
    # blocks while established), cancel+regenerate, then release the bytes.
    call("portal", "POST", "/api/hold/arm", {})
    call("portal", "PUT", "/api/control", {"download_hold": 1})
    late_box = {}

    def start_late():
        late_box["out"] = vb_download(old)

    th = threading.Thread(target=start_late)
    th.start()
    # Wait deterministically until the download is actually parked on the gate
    # rather than guessing a fixed sleep.
    parked = False
    for _ in range(50):
        _, st = call("portal", "GET", "/api/hold/status")
        if st.get("waiting", 0) >= 1:
            parked = True
            break
        time.sleep(0.1)
    check("old download is parked in-flight before cancel", parked)
    _, regen = call("portal", "POST", f"/api/tasks/{old['task_id']}/regenerate", {})
    new = regen["new_task"]
    call("portal", "POST", "/api/hold/release", {})
    th.join()
    call("portal", "PUT", "/api/control", {"download_hold": 0})
    late = late_box["out"]
    if "download_id" not in late:
        check("late in-flight download completes", False, str(late))
        return
    fresh = vb_download(new)
    r_late = wait_ledger(late["download_id"])
    r_fresh = wait_ledger(fresh["download_id"], "ingested")
    check("late file from cancelled task quarantined",
          r_late is not None and r_late["status"] == "quarantined"
          and "task_" in (r_late["reason"] or ""),
          r_late and r_late.get("reason"))
    check("new task file ingested normally", bool(r_fresh))
    if r_fresh:
        check("new result belongs to the new task id", r_fresh["task_id"] == new["task_id"])
    # the cancelled task must have no library entry
    _, led = call("worker", "GET", "/ledger")
    old_ingested = [r for r in led["ingests"]
                    if r["task_id"] == old["task_id"] and r["status"] == "ingested"]
    check("cancelled task has zero library rows", old_ingested == [])


def scenario_tamper():
    print("\n=== 3. 内容被替换：三方哈希核对失败 -> 隔离 ===")
    call("portal", "PUT", "/api/control", {"tamper_download": 1})
    t = make_task("tampered")
    d = vb_download(t)
    row = wait_ledger(d["download_id"], "quarantined")
    call("portal", "PUT", "/api/control", {"tamper_download": 0})
    check("tampered content quarantined",
          bool(row) and "content_hash_mismatch" in (row["reason"] or ""),
          row and row.get("reason"))
    check("quarantine evidence retained on disk",
          bool(row and row.get("quarantine_path") and os.path.isfile(row["quarantine_path"])))


def scenario_ack_loss():
    print("\n=== 4. 入库成功但 ACK 丢失：租约重投 + 幂等 ===")
    call("collector", "PUT", "/api/control",
         {"drop_ack": 1, "lease_seconds": 3})
    t = make_task("ack-loss")
    d = vb_download(t)
    did = d["download_id"]
    row = wait_ledger(did, "ingested")
    check("ingestion committed despite lost ACK", bool(row))
    # Keep dropping ACKs across a lease cycle so redelivery is forced, and
    # confirm the worker takes the idempotent path rather than double-ingesting.
    deadline = time.time() + 12
    redelivered = False
    while time.time() < deadline:
        _, led = call("worker", "GET", "/ledger")
        if any("redelivery" in line and did in line for line in led.get("worker_log", [])):
            redelivered = True
            break
        time.sleep(1)
    check("lease expiry triggered redelivery (idempotent path logged)", redelivered)
    # Stop dropping receipts but keep the short lease so the next redelivery
    # is picked up quickly and finally ACKs.
    call("collector", "PUT", "/api/control", {"drop_ack": 0, "lease_seconds": 3})
    env = wait_envelope(did, "acked", timeout=15)
    call("collector", "PUT", "/api/control", {"lease_seconds": 30})
    check("redelivery eventually ACKed once receipts flow (self-healed)", bool(env))
    _, led = call("worker", "GET", "/ledger")
    same = [r for r in led["ingests"] if r["download_id"] == did]
    check("exactly one ledger row (no duplicate ingest)", len(same) == 1)
    check("envelope shows multiple delivery attempts", env and env["attempts"] >= 2)


def scenario_browser_exit():
    print("\n=== 5. 浏览器在上传前退出：自描述文件 + 重启恢复 ===")
    t = make_task("browser-exit")
    _, crashed = call("vb", "POST", "/crash-before-post", {"task": {
        "task_id": t["task_id"], "token": t["token"],
        "filter": t["filter"], "filename": t["filename"]}})
    did = crashed["download_id"]
    # nothing ingested while the browser is "dead"
    time.sleep(2)
    _, led = call("worker", "GET", "/ledger")
    check("no ingest while browser is dead",
          all(r["download_id"] != did for r in led["ingests"]))
    # file really exists on disk and carries its own manifest trailer
    import glob
    on_disk = glob.glob(f"runtime/vbrowser-downloads/{did}__*")
    check("self-describing file left on disk",
          bool(on_disk) and b"__MANIFEST__" in open(on_disk[0], "rb").read())
    _, rec = call("vb", "POST", "/restart-recover", {})
    recovered_ids = [x["download_id"] for x in rec.get("recovered", [])]
    check("restart replays the orphaned file", did in recovered_ids)
    row = wait_ledger(did, "ingested")
    check("recovered file ingested after restart", bool(row))


def scenario_sse_duplicates():
    print("\n=== 6. 重复/乱序 SSE 通知：去重不产生重复下载 ===")
    call("portal", "PUT", "/api/control", {"sse_duplicate": 3, "sse_delay_ms": 200})
    t = make_task("sse-dup")
    time.sleep(1.5)
    call("portal", "PUT", "/api/control", {"sse_duplicate": 0, "sse_delay_ms": 0})
    # manual download (auto mode is off); duplicate events must not confuse state
    d = vb_download(t)
    row = wait_ledger(d["download_id"], "ingested")
    check("single ingest despite duplicated notifications", bool(row))
    _, envs = call("collector", "GET", "/api/envelopes")
    same = [e for e in envs["envelopes"] if e["download_id"] == d["download_id"]]
    check("one envelope row (event dedup by download_id)", len(same) == 1)


def scenario_filter_mismatch():
    print("\n=== 7. 伪造筛选条件：filter 核对失败 -> 隔离 ===")
    t = make_task("filter-check")
    # hand the browser a lying filter; task/token still valid
    lying = dict(t)
    lying["filter"] = {**t["filter"], "region": "west"}
    d = vb_download(lying)
    row = wait_ledger(d["download_id"], "quarantined")
    check("filter mismatch quarantined",
          bool(row) and "filter_mismatch" in (row["reason"] or ""),
          row and row.get("reason"))


def scenario_notification_before_file():
    print("\n=== 8. 通知先到、文件后到：awaiting_file -> ready 正常收敛 ===")
    t = make_task("notify-first")
    d = vb_download(t)
    did = d["download_id"]
    # The collector must tolerate the completed event existing before/without
    # the envelope for a grace period, and the envelope finalizes to ready.
    env = wait_envelope(did, "acked", timeout=15)
    check("envelope converged to acked", bool(env))
    _, ev = call("collector", "GET", f"/api/events/{did}")
    kinds = [e["kind"] for e in ev.get("events", [])]
    check("both created and completed notifications recorded",
          "created" in kinds and "completed" in kinds, str(kinds))


def scenario_orphan_partial():
    print("\n=== 9. 上传中断残留 .part：对账识别为 orphan_partial ===")
    spool = "runtime/collector/spool"
    os.makedirs(spool, exist_ok=True)
    fake_id = "deadbeef-0000-4000-8000-000000000001"
    part = os.path.join(spool, f"{fake_id}.part")
    with open(part, "wb") as f:
        f.write(b"partial,upload\n")
    # backdate beyond the orphan threshold (20s)
    old = time.time() - 30
    os.utime(part, (old, old))
    _, recon = call("collector", "POST", "/api/reconcile", {})
    check("reconcile flags the orphan .part", fake_id in recon.get("orphan_parts", []))
    env = wait_envelope(fake_id, "orphan_partial", timeout=5)
    check("orphan envelope recorded with reason",
          bool(env) and "interrupted" in (env.get("last_error") or ""))


def main():
    scenario_same_name_concurrent()
    scenario_cancel_then_new()
    scenario_tamper()
    scenario_ack_loss()
    scenario_browser_exit()
    scenario_sse_duplicates()
    scenario_filter_mismatch()
    scenario_notification_before_file()
    scenario_orphan_partial()
    print(f"\n==== {len(PASS)} passed, {len(FAIL)} failed ====")
    if FAIL:
        print("failed:", FAIL)
        sys.exit(1)


if __name__ == "__main__":
    main()
