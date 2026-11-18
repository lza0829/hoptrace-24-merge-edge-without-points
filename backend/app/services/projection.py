"""状态投影：从正式时间线末点派生，供装载与合并共用同一套口径。"""

EVENT_STATUS = {
    "PICKED": "已揽收",
    "IN_TRANSIT": "运输中",
    "ARRIVED_HUB": "到站",
    "OUT_FOR_DELIVERY": "派送中",
    "DELIVERED": "已签收",
    "EXCEPTION": "异常",
}


def map_status(event_code: str) -> str:
    return EVENT_STATUS.get(event_code, "未知")


def last_valid_hop(conn, waybill_id: str):
    return conn.execute(
        """
        SELECT id, event_ts, event_code
        FROM hops
        WHERE waybill_id=? AND channel='primary' AND retracted=0
        ORDER BY event_ts DESC, id DESC
        LIMIT 1
        """,
        (waybill_id,),
    ).fetchone()


def snapshot(conn, waybill_id: str) -> dict:
    """读取当前投影快照（不改库）。"""
    wb = conn.execute(
        "SELECT id, status_proj FROM waybills WHERE id=?", (waybill_id,)
    ).fetchone()
    if not wb:
        return {
            "waybill_id": waybill_id,
            "status_proj": None,
            "last_hop_id": None,
            "last_event_ts": None,
            "mapped_code": None,
        }
    last = last_valid_hop(conn, waybill_id)
    return {
        "waybill_id": waybill_id,
        "status_proj": wb["status_proj"],
        "last_hop_id": last["id"] if last else None,
        "last_event_ts": last["event_ts"] if last else None,
        "mapped_code": last["event_code"] if last else None,
    }


def recompute(conn, waybill_id: str, *, write_proof: bool = True) -> dict:
    """按 (event_ts, id) 末点重算投影；调用方负责事务。

    前置闸：已并入主单的成员段是冻结快照，不得再经自身键独立重算投影，
    否则成员状态会与主单投影源证明分叉。
    """
    from app.modules.merge_leg_proof import assert_member_gate_open

    assert_member_gate_open(conn, waybill_id)
    before = snapshot(conn, waybill_id)
    last = last_valid_hop(conn, waybill_id)
    status = map_status(last["event_code"]) if last else "未知"
    conn.execute(
        "UPDATE waybills SET status_proj=? WHERE id=?", (status, waybill_id)
    )
    if write_proof:
        conn.execute(
            """
            INSERT INTO status_proofs(waybill_id, last_hop_id, mapped_code, proj_value, created_at)
            VALUES (?,?,?,?,datetime('now'))
            """,
            (
                waybill_id,
                last["id"] if last else None,
                last["event_code"] if last else None,
                status,
            ),
        )
    after = {
        "waybill_id": waybill_id,
        "status_proj": status,
        "last_hop_id": last["id"] if last else None,
        "last_event_ts": last["event_ts"] if last else None,
        "mapped_code": last["event_code"] if last else None,
    }
    return {"before": before, "after": after}
