import json
from pathlib import Path

from app.db import FIXTURES_DIR, connect


SCHEMA = """
CREATE TABLE IF NOT EXISTS waybills (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  status_proj TEXT NOT NULL DEFAULT '未知',
  created_at TEXT NOT NULL,
  merged_into TEXT,
  merged_at TEXT
);

CREATE TABLE IF NOT EXISTS hops (
  id TEXT PRIMARY KEY,
  waybill_id TEXT NOT NULL REFERENCES waybills(id),
  event_ts TEXT NOT NULL,
  location_code TEXT NOT NULL,
  event_code TEXT NOT NULL,
  seq INTEGER,
  channel TEXT NOT NULL DEFAULT 'primary',
  retracted INTEGER NOT NULL DEFAULT 0,
  UNIQUE(waybill_id, event_ts, location_code, event_code)
);

CREATE TABLE IF NOT EXISTS quarantine (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  waybill_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  member_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_batches (
  id TEXT PRIMARY KEY,
  cutoff_ts TEXT,
  created_at TEXT NOT NULL,
  report_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS status_proofs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  waybill_id TEXT NOT NULL,
  last_hop_id TEXT,
  mapped_code TEXT,
  proj_value TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- 合并批次：一次合并尝试一行，成功 / 失败均留痕。
CREATE TABLE IF NOT EXISTS merge_batches (
  id TEXT PRIMARY KEY,
  master_id TEXT NOT NULL,
  status TEXT NOT NULL,              -- committed / rolled_back
  error TEXT,
  member_ids_json TEXT NOT NULL DEFAULT '[]',
  merged_count INTEGER NOT NULL DEFAULT 0,
  proj_before_json TEXT NOT NULL DEFAULT '{}',
  proj_after_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

-- 合并边：主单 ← 成员段，可查且不可重复合并。
CREATE TABLE IF NOT EXISTS merge_edges (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES merge_batches(id),
  master_id TEXT NOT NULL REFERENCES waybills(id),
  member_id TEXT NOT NULL REFERENCES waybills(id),
  merged_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  UNIQUE(member_id)                  -- 一个成员段只能并入一个主单一次
);
CREATE INDEX IF NOT EXISTS idx_merge_edges_master ON merge_edges(master_id);

-- 并入主单正式时间线的成员点（来源可溯）。
CREATE TABLE IF NOT EXISTS merge_hops (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES merge_batches(id),
  master_id TEXT NOT NULL,
  member_id TEXT NOT NULL,
  hop_id TEXT NOT NULL,
  source_hop_id TEXT NOT NULL,
  event_ts TEXT NOT NULL,
  UNIQUE(master_id, hop_id)
);
CREATE INDEX IF NOT EXISTS idx_merge_hops_batch ON merge_hops(batch_id);
CREATE INDEX IF NOT EXISTS idx_merge_hops_hop ON merge_hops(hop_id);
"""

EVENT_STATUS = {
    "PICKED": "已揽收",
    "IN_TRANSIT": "运输中",
    "ARRIVED_HUB": "到站",
    "OUT_FOR_DELIVERY": "派送中",
    "DELIVERED": "已签收",
    "EXCEPTION": "异常",
}


def init_db():
    conn = connect()
    conn.executescript(SCHEMA)
    _migrate(conn)
    n = conn.execute("SELECT COUNT(*) AS c FROM waybills").fetchone()["c"]
    if n == 0:
        _seed_fixtures(conn)
    conn.commit()
    conn.close()


def _migrate(conn):
    """旧库补齐合并相关列（CREATE IF NOT EXISTS 不会给已有表加列）。"""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(waybills)").fetchall()}
    if "merged_into" not in cols:
        conn.execute("ALTER TABLE waybills ADD COLUMN merged_into TEXT")
    if "merged_at" not in cols:
        conn.execute("ALTER TABLE waybills ADD COLUMN merged_at TEXT")


def _seed_fixtures(conn):
    fixture = FIXTURES_DIR / "seed_hops.json"
    if not fixture.exists():
        return
    data = json.loads(fixture.read_text(encoding="utf-8"))
    for wb in data.get("waybills", []):
        conn.execute(
            "INSERT INTO waybills(id, title, status_proj, created_at) VALUES (?,?,?,?)",
            (wb["id"], wb["title"], wb.get("status_proj", "未知"), wb["created_at"]),
        )
    for hop in data.get("hops", []):
        conn.execute(
            "INSERT INTO hops(id, waybill_id, event_ts, location_code, event_code, seq, channel) VALUES (?,?,?,?,?,?,?)",
            (
                hop["id"],
                hop["waybill_id"],
                hop["event_ts"],
                hop["location_code"],
                hop["event_code"],
                hop.get("seq"),
                hop.get("channel", "primary"),
            ),
        )
    _refresh_projections(conn)


def _refresh_projections(conn):
    """与装载 / 合并共用同一投影口径，避免证明数字各算一套。"""
    from app.services import projection as projection_svc

    ids = [
        r["id"] for r in conn.execute("SELECT id FROM waybills ORDER BY id").fetchall()
    ]
    for wid in ids:
        projection_svc.recompute(conn, wid, write_proof=True)


def map_status(event_code: str) -> str:
    return EVENT_STATUS.get(event_code, "未知")
