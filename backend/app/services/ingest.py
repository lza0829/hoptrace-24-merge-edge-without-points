import json
import uuid
from datetime import datetime, timezone

from app.db import FIXTURES_DIR, connect
from app.modules import merge_leg_proof as merge_svc
from app.services import projection


def run_batch(payload: dict | None = None) -> dict:
    """基座装载：按夹具追加；乱序重排 / 旁路 / 重复隔离留给功能题空桩。

    已合并成员段视为冻结：其新点不再落正式线、不再改主单投影。
    传入 payload 时按内存批次装载（夹具 runner / 验收用），否则读 ingest_batch.json。
    """
    if payload is None:
        path = FIXTURES_DIR / "ingest_batch.json"
        if not path.exists():
            return {"ok": False, "error": "缺少 ingest_batch.json"}
        payload = json.loads(path.read_text(encoding="utf-8"))
    batch_id = payload.get("batch_id") or f"B-{uuid.uuid4().hex[:8]}"
    cutoff = payload.get("cutoff_ts")
    hops = payload.get("hops", [])

    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        written = 0
        skipped_merged: list[str] = []
        touched: set[str] = set()
        for hop in hops:
            wid = hop["waybill_id"]
            # 成员关闸即冻结：已并入成员段的新点不落正式线、不改主单投影，
            # 也不允许经成员自身键重算投影（否则主单投影源证明会被成员状态带偏）。
            if merge_svc.timeline_owner(conn, wid) != wid or merge_svc.member_gate(conn, wid):
                skipped_merged.append(wid)
                continue
            exists = conn.execute("SELECT 1 FROM waybills WHERE id=?", (wid,)).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO waybills(id, title, status_proj, created_at) VALUES (?,?,?,?)",
                    (wid, hop.get("waybill_title") or wid, "未知", _now()),
                )
            conn.execute(
                """
                INSERT OR IGNORE INTO hops(id, waybill_id, event_ts, location_code, event_code, seq, channel)
                VALUES (?,?,?,?,?,?, 'primary')
                """,
                (
                    hop["id"],
                    wid,
                    hop["event_ts"],
                    hop["location_code"],
                    hop["event_code"],
                    hop.get("seq"),
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                written += 1
                touched.add(wid)
        for wid in touched:
            projection.recompute(conn, wid, write_proof=True)
        report = {
            "batch_id": batch_id,
            "cutoff_ts": cutoff,
            "written": written,
            "input": len(hops),
            "skipped_merged_member": sorted(set(skipped_merged)),
        }
        conn.execute(
            "INSERT OR REPLACE INTO ingest_batches(id, cutoff_ts, created_at, report_json) VALUES (?,?,?,?)",
            (batch_id, cutoff, _now(), json.dumps(report, ensure_ascii=False)),
        )
        conn.commit()
        return {"ok": True, "report": report}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
