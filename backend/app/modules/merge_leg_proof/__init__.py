"""多段并单：把成员段正式有效点按 (event_ts, id) 并入主单正式时间线。

落库形态（择一：独立合并边表）：
- merge_edges   主单 ← 成员段的合并边，可查且成员唯一；
- merge_batches 合并批次证明（成功 / 失败均留痕）；
- merge_hops    并入点溯源：批次 → 成员源点 → 主单正式 hop。

并入方式：成员段每个正式有效点复制为主单名下的 hops 行（新 id，保留源 id 溯源），
因此主单时间线 / 末点投影天然来自同一张 hops 表，不会出现「边可查但时间线半截」。
成员行落 merged_into 后，其后续点位不会再回流主单（并入的是不可变快照）。

一致性路径（择一：同事务失败闭）：
一次事务内完成「并入点 + 写边 + 标记成员 + 重算投影」，任一步异常整笔 ROLLBACK；
失败尝试另落一条 rolled_back 批次供审计，但边、并入点、投影零残留。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from app.db import connect
from app.services import projection

STUB = False


class MergeError(Exception):
    """合并前置校验失败（可预期的业务拒绝）。"""


class MergeFault(Exception):
    """事务中途注入的故障，用于验证失败闭。"""


# ---------------------------------------------------------------- helpers


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _member_ids(member_ids) -> list[str]:
    out: list[str] = []
    for mid in member_ids or []:
        mid = (mid or "").strip()
        if mid and mid not in out:
            out.append(mid)
    return out


def _valid_hops(conn, waybill_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, waybill_id, event_ts, location_code, event_code, seq
        FROM hops
        WHERE waybill_id=? AND channel='primary' AND retracted=0
        ORDER BY event_ts ASC, id ASC
        """,
        (waybill_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _member_points(conn, member_ids: list[str]) -> list[dict]:
    """成员段正式有效点，全量按 (event_ts, 源 id) 归并。"""
    points: list[dict] = []
    for mid in member_ids:
        for hop in _valid_hops(conn, mid):
            points.append({**hop, "member_id": mid})
    points.sort(key=lambda p: (p["event_ts"], p["id"]))
    return points


def _validate(conn, master_id: str, member_ids: list[str]) -> None:
    if not master_id:
        raise MergeError("缺少主单")
    if not member_ids:
        raise MergeError("至少需要一段成员")
    ids = [master_id, *member_ids]
    rows = {
        r["id"]: r
        for r in conn.execute(
            f"SELECT id, title, status_proj, merged_into FROM waybills WHERE id IN ({','.join('?' * len(ids))})",
            ids,
        ).fetchall()
    }
    if master_id not in rows:
        raise MergeError(f"主单不存在：{master_id}")
    if rows[master_id]["merged_into"]:
        raise MergeError(f"主单自身已被并入：{master_id}")
    if master_id in member_ids:
        raise MergeError("主单不能作为自己的成员段")
    for mid in member_ids:
        if mid not in rows:
            raise MergeError(f"成员段不存在：{mid}")
        if rows[mid]["merged_into"]:
            raise MergeError(f"成员段已合并：{mid} → {rows[mid]['merged_into']}")
        edge = conn.execute(
            "SELECT master_id FROM merge_edges WHERE member_id=?", (mid,)
        ).fetchone()
        if edge:
            raise MergeError(f"成员段已有合并边：{mid} → {edge['master_id']}")
        # 成员段自身若已是主单，其并入点会二次复制，边归属将失真；直接拒绝传递合并。
        has_members = conn.execute(
            "SELECT 1 FROM merge_edges WHERE master_id=? LIMIT 1", (mid,)
        ).fetchone()
        if has_members:
            raise MergeError(f"成员段自身已并入了其他段，不支持传递合并：{mid}")
        if not _valid_hops(conn, mid):
            raise MergeError(f"成员段无正式有效点：{mid}")


def _merged_hop_id(master_id: str, member_id: str, source_hop_id: str) -> str:
    return f"{source_hop_id}@{member_id}>{master_id}"


def _assert_no_duplicate_points(conn, master_id: str, points: list[dict]) -> None:
    """hops 有 (waybill_id, event_ts, location_code, event_code) 唯一约束。

    并入后成员点归属主单，若与主单原点或彼此四字段相同，插入会撞约束。
    这里提前给出明确拒绝，避免裸 IntegrityError，也避免半截写入。
    """
    existing = {
        (r["event_ts"], r["location_code"], r["event_code"])
        for r in conn.execute(
            """
            SELECT event_ts, location_code, event_code FROM hops
            WHERE waybill_id=? AND channel='primary' AND retracted=0
            """,
            (master_id,),
        ).fetchall()
    }
    dup_master: list[str] = []
    dup_inner: list[str] = []
    seen: set[tuple] = set()
    for p in points:
        key = (p["event_ts"], p["location_code"], p["event_code"])
        if key in existing:
            dup_master.append(p["id"])
        elif key in seen:
            dup_inner.append(p["id"])
        seen.add(key)
    if dup_master or dup_inner:
        bits = []
        if dup_master:
            bits.append(f"与主单原点重复：{', '.join(dup_master)}")
        if dup_inner:
            bits.append(f"成员段之间重复：{', '.join(dup_inner)}")
        raise MergeError("并入点撞唯一约束，" + "；".join(bits))


# ---------------------------------------------------------------- read models


def _batch_row(conn, batch_id: str):
    return conn.execute("SELECT * FROM merge_batches WHERE id=?", (batch_id,)).fetchone()


def batch_proof(conn, batch_id: str) -> dict | None:
    """按合并批次查询只读证明；成功 / 失败批次均可查。"""
    row = _batch_row(conn, batch_id)
    if not row:
        return None
    member_ids = json.loads(row["member_ids_json"] or "[]")
    edges = {
        e["member_id"]: e
        for e in conn.execute(
            "SELECT member_id, master_id, merged_count FROM merge_edges WHERE batch_id=?",
            (batch_id,),
        ).fetchall()
    }
    members = []
    for mid in member_ids:
        wb = conn.execute(
            "SELECT id, title, merged_into FROM waybills WHERE id=?", (mid,)
        ).fetchone()
        members.append(
            {
                "id": mid,
                "title": wb["title"] if wb else None,
                "merged_into": wb["merged_into"] if wb else None,
                "merged_count": edges[mid]["merged_count"] if mid in edges else 0,
            }
        )
    hop_ids = [
        r["hop_id"]
        for r in conn.execute(
            "SELECT hop_id FROM merge_hops WHERE batch_id=? ORDER BY event_ts, hop_id",
            (batch_id,),
        ).fetchall()
    ]
    master = conn.execute(
        "SELECT id, title, status_proj, merged_into FROM waybills WHERE id=?",
        (row["master_id"],),
    ).fetchone()
    return {
        "batch_id": row["id"],
        "status": row["status"],
        "ok": row["status"] == "committed",
        "error": row["error"],
        "master": dict(master) if master else {"id": row["master_id"]},
        "members": members,
        "merged_count": row["merged_count"],
        "merged_hop_ids": hop_ids,
        "projection_before": json.loads(row["proj_before_json"] or "{}"),
        "projection_after": json.loads(row["proj_after_json"] or "{}"),
        "created_at": row["created_at"],
    }


def list_batches(conn, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM merge_batches ORDER BY created_at DESC, id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "batch_id": r["id"],
            "master_id": r["master_id"],
            "status": r["status"],
            "error": r["error"],
            "member_ids": json.loads(r["member_ids_json"] or "[]"),
            "merged_count": r["merged_count"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def merge_summary(conn, waybill_id: str) -> dict | None:
    """测绘板用的合并摘要，数字与批次证明同源（都回查 merge_edges / merge_hops）。"""
    wb = conn.execute(
        "SELECT id, merged_into, merged_at FROM waybills WHERE id=?", (waybill_id,)
    ).fetchone()
    if not wb:
        return None
    if wb["merged_into"]:
        edge = conn.execute(
            "SELECT batch_id, master_id, merged_count, created_at FROM merge_edges WHERE member_id=?",
            (waybill_id,),
        ).fetchone()
        return {
            "role": "member",
            "merged_into": wb["merged_into"],
            "merged_at": wb["merged_at"],
            "batch_id": edge["batch_id"] if edge else None,
            "merged_count": edge["merged_count"] if edge else 0,
        }
    # 主单可多次并单：汇总全部合并边，数字仍从批次证明回查，不另算。
    edges = conn.execute(
        "SELECT batch_id, member_id, created_at FROM merge_edges WHERE master_id=? ORDER BY id ASC",
        (waybill_id,),
    ).fetchall()
    if not edges:
        return {"role": "master", "batch_id": None, "members": [], "merged_count": 0}
    batch_ids: list[str] = []
    members: list[dict] = []
    total = 0
    first_proj_before = None
    last_proof = None
    for edge in edges:
        if edge["batch_id"] in batch_ids:
            continue
        batch_ids.append(edge["batch_id"])
        proof = batch_proof(conn, edge["batch_id"])
        if not proof:
            continue
        last_proof = proof
        total += proof["merged_count"]
        if first_proj_before is None:
            first_proj_before = proof["projection_before"]
        members.extend(proof["members"])
    return {
        "role": "master",
        "batch_id": last_proof["batch_id"] if last_proof else None,
        "batch_ids": batch_ids,
        "merged_count": total,
        "members": members,
        "projection_before": first_proj_before,
        "projection_after": last_proof["projection_after"] if last_proof else None,
        "created_at": edges[-1]["created_at"],
    }


def timeline_owner(conn, waybill_id: str) -> str:
    """成员段的时间线归属：已合并则归主单，否则归自己。

    合并后成员段的正式线不可再独立漂移，读取需回到主单口径。
    """
    row = conn.execute(
        "SELECT merged_into FROM waybills WHERE id=?", (waybill_id,)
    ).fetchone()
    if row and row["merged_into"]:
        return row["merged_into"]
    return waybill_id


# ---------------------------------------------------------------- merge


def merge_legs(master_id: str, member_ids, *, fail_after: int | None = None) -> dict:
    """执行一次合并，返回批次证明。失败时 ok=False 且正式线未动。"""
    member_ids = _member_ids(member_ids)
    batch_id = f"MB-{uuid.uuid4().hex[:10]}"
    created_at = _now()
    conn = connect()
    conn.isolation_level = None  # 手动显式事务
    proj_before: dict = {}
    try:
        conn.execute("BEGIN IMMEDIATE")
        _validate(conn, master_id, member_ids)
        proj_before = projection.snapshot(conn, master_id)
        points = _member_points(conn, member_ids)
        if not points:
            raise MergeError("成员段没有可并入的正式点")
        _assert_no_duplicate_points(conn, master_id, points)

        # 批次头先落（边 / 点有外键指向它），末尾回填计数与投影。
        conn.execute(
            """
            INSERT INTO merge_batches(id, master_id, status, error, member_ids_json,
                                      merged_count, proj_before_json, proj_after_json, created_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                batch_id,
                master_id,
                "committed",
                None,
                _json(member_ids),
                0,
                _json(proj_before),
                _json(proj_before),
                created_at,
            ),
        )

        per_member: dict[str, int] = {mid: 0 for mid in member_ids}
        for p in points:
            new_id = _merged_hop_id(master_id, p["member_id"], p["id"])
            conn.execute(
                """
                INSERT INTO hops(id, waybill_id, event_ts, location_code, event_code, seq, channel, retracted)
                VALUES (?,?,?,?,?,?, 'primary', 0)
                """,
                (
                    new_id,
                    master_id,
                    p["event_ts"],
                    p["location_code"],
                    p["event_code"],
                    p["seq"],
                ),
            )
            conn.execute(
                """
                INSERT INTO merge_hops(batch_id, master_id, member_id, hop_id, source_hop_id, event_ts)
                VALUES (?,?,?,?,?,?)
                """,
                (batch_id, master_id, p["member_id"], new_id, p["id"], p["event_ts"]),
            )
            per_member[p["member_id"]] += 1
            if fail_after is not None and sum(per_member.values()) >= fail_after:
                raise MergeFault(f"注入故障：并入第 {sum(per_member.values())} 个成员点后中止")

        for mid in member_ids:
            conn.execute(
                """
                INSERT INTO merge_edges(batch_id, master_id, member_id, merged_count, created_at)
                VALUES (?,?,?,?,?)
                """,
                (batch_id, master_id, mid, per_member[mid], created_at),
            )
            conn.execute(
                "UPDATE waybills SET merged_at=? WHERE id=?",
                (created_at, mid),
            )

        proj = projection.recompute(conn, master_id, write_proof=True)
        total = sum(per_member.values())
        conn.execute(
            """
            UPDATE merge_batches
               SET status='committed', merged_count=?, proj_after_json=?
             WHERE id=?
            """,
            (total, _json(proj["after"]), batch_id),
        )
        conn.execute("COMMIT")
        return batch_proof(conn, batch_id)
    except Exception as exc:  # noqa: BLE001 — 失败闭：任何异常都整笔回滚
        conn.rollback()
        unchanged = projection.snapshot(conn, master_id) if master_id else {}
        if not proj_before:
            proj_before = unchanged
        # 失败批次单独落痕，但 merge_edges / merge_hops / hops 零残留。
        conn.execute(
            """
            INSERT OR REPLACE INTO merge_batches(id, master_id, status, error, member_ids_json,
                                                 merged_count, proj_before_json, proj_after_json, created_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                batch_id,
                master_id,
                "rolled_back",
                str(exc),
                _json(member_ids),
                0,
                _json(proj_before),
                _json(unchanged),
                created_at,
            ),
        )
        conn.commit()
        return batch_proof(conn, batch_id)
    finally:
        conn.close()


# ---------------------------------------------------------------- fixture runner


def _fixture_path():
    from app.db import FIXTURES_DIR

    return FIXTURES_DIR / "merge_batch.json"


def run_fixture(mode: str = "success", run_tag: str | None = None) -> dict:
    """装载合并夹具并跑一次合并，供成功 / 失败回滚对照。

    夹具是模板，每次运行生成 run 级 id，互不覆盖，数字可直接对照。
    """
    path = _fixture_path()
    if not path.exists():
        raise MergeError("缺少 merge_batch.json")
    template = json.loads(path.read_text(encoding="utf-8"))
    tag = run_tag or uuid.uuid4().hex[:6]
    master_tpl = template["master"]
    member_tpls = template.get("members", [])
    if len(member_tpls) < 2:
        raise MergeError("合并夹具至少需要两段成员")
    master_id = f"{master_tpl['id']}-{tag}"
    member_ids = [f"{m['id']}-{tag}" for m in member_tpls]

    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _insert_fixture_waybill(conn, master_tpl, master_id, tag)
        for tpl, mid in zip(member_tpls, member_ids):
            _insert_fixture_waybill(conn, tpl, mid, tag)
        for wid in [master_id, *member_ids]:
            projection.recompute(conn, wid, write_proof=True)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    fail_after = 1 if mode == "failure" else None
    proof = merge_legs(master_id, member_ids, fail_after=fail_after)
    proof["fixture"] = {
        "mode": mode,
        "run_tag": tag,
        "master_id": master_id,
        "member_ids": member_ids,
    }
    return proof


def _insert_fixture_waybill(conn, tpl: dict, wid: str, tag: str) -> None:
    conn.execute(
        "INSERT INTO waybills(id, title, status_proj, created_at) VALUES (?,?,?,?)",
        (wid, tpl.get("title") or wid, "未知", _now()),
    )
    for hop in tpl.get("hops", []):
        conn.execute(
            """
            INSERT INTO hops(id, waybill_id, event_ts, location_code, event_code, seq, channel)
            VALUES (?,?,?,?,?,?, 'primary')
            """,
            (
                f"{hop['id']}-{tag}",
                wid,
                hop["event_ts"],
                hop["location_code"],
                hop["event_code"],
                hop.get("seq"),
            ),
        )

def apply_member_ingest_to_master(conn, member_id: str, master_id: str, proj: str) -> None:
    conn.execute("UPDATE waybills SET status_proj=? WHERE id=?", (proj, master_id))
