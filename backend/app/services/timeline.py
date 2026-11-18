"""正式时间线读取：主单原生点与并入点统一按 (event_ts, id) 排序。"""


def list_primary(conn, waybill_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT h.id, h.waybill_id, h.event_ts, h.location_code, h.event_code,
               h.seq, h.channel, h.retracted,
               m.batch_id AS merge_batch_id,
               m.member_id AS source_member_id,
               m.source_hop_id AS source_hop_id
        FROM hops h
        LEFT JOIN merge_hops m ON m.hop_id = h.id
        WHERE h.waybill_id=? AND h.channel='primary' AND h.retracted=0
        ORDER BY h.event_ts ASC, h.id ASC
        """,
        (waybill_id,),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["merged"] = item["merge_batch_id"] is not None
        out.append(item)
    return out
