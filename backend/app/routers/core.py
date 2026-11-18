from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import connect
from app.modules import merge_leg_proof as merge_svc
from app.services import ingest as ingest_svc
from app.services import timeline as timeline_svc

router = APIRouter(prefix="/api")


class MergeRequest(BaseModel):
    master_id: str
    member_ids: list[str]


@router.get("/overview")
def overview():
    conn = connect()
    try:
        return {
            "waybills": conn.execute("SELECT COUNT(*) AS c FROM waybills").fetchone()["c"],
            "hops_primary": conn.execute(
                "SELECT COUNT(*) AS c FROM hops WHERE channel='primary' AND retracted=0"
            ).fetchone()["c"],
            "hops_sidecar": conn.execute(
                "SELECT COUNT(*) AS c FROM hops WHERE channel='sidecar'"
            ).fetchone()["c"],
            "quarantine": conn.execute("SELECT COUNT(*) AS c FROM quarantine").fetchone()["c"],
        }
    finally:
        conn.close()


@router.get("/waybills")
def list_waybills():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT id, title, status_proj, created_at FROM waybills ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/waybills/{waybill_id}/timeline")
def waybill_timeline(waybill_id: str):
    conn = connect()
    try:
        wb = conn.execute("SELECT * FROM waybills WHERE id=?", (waybill_id,)).fetchone()
        if not wb:
            raise HTTPException(404, "运单不存在")
        hops = timeline_svc.list_primary(conn, waybill_id)
        proof = conn.execute(
            "SELECT * FROM status_proofs WHERE waybill_id=? ORDER BY id DESC LIMIT 1",
            (waybill_id,),
        ).fetchone()
        merge = merge_svc.merge_summary(conn, waybill_id)
        return {
            "waybill": dict(wb),
            "hops": hops,
            "proof": dict(proof) if proof else None,
            "merge": merge,
        }
    finally:
        conn.close()


@router.get("/quarantine")
def list_quarantine():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT id, waybill_id, reason, member_json, created_at FROM quarantine ORDER BY id DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/sidecar")
def list_sidecar():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT id, waybill_id, event_ts, location_code, event_code, channel FROM hops WHERE channel='sidecar' ORDER BY event_ts"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.post("/ingest/run")
def run_ingest():
    return ingest_svc.run_batch()


@router.get("/status-proofs")
def status_proofs(waybill_id: str | None = None):
    conn = connect()
    try:
        if waybill_id:
            rows = conn.execute(
                "SELECT * FROM status_proofs WHERE waybill_id=? ORDER BY id DESC",
                (waybill_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM status_proofs ORDER BY id DESC LIMIT 50"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------- 合并 / 并单


@router.post("/merge/run")
def run_merge(req: MergeRequest):
    return merge_svc.merge_legs(req.master_id, req.member_ids)


@router.post("/merge/fixture")
def run_merge_fixture(mode: str = "success"):
    """装载合并夹具并跑一次合并；mode=success|failure 用于成功 / 回滚对照。"""
    if mode not in ("success", "failure"):
        raise HTTPException(400, "mode 仅支持 success / failure")
    try:
        return merge_svc.run_fixture(mode)
    except merge_svc.MergeError as e:
        raise HTTPException(400, str(e)) from e


@router.get("/merge/batches")
def merge_batches(limit: int = 50):
    conn = connect()
    try:
        return merge_svc.list_batches(conn, limit=limit)
    finally:
        conn.close()


@router.get("/merge/batches/{batch_id}")
def merge_batch_proof(batch_id: str):
    """按合并批次查询只读证明：主单、成员、并入点数、前后投影。"""
    conn = connect()
    try:
        proof = merge_svc.batch_proof(conn, batch_id)
        if not proof:
            raise HTTPException(404, "合并批次不存在")
        return proof
    finally:
        conn.close()
