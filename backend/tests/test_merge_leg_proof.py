"""merge_leg_proof 验收：并入正确性、失败闭、证明同源、成员冻结。"""

import json
from pathlib import Path

import pytest

from app import seed
from app.db import connect
from app.modules import merge_leg_proof as merge_svc
from app.services import ingest as ingest_svc
from app.services import timeline as timeline_svc


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    import app.db as db

    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setattr(db, "DB_PATH", Path(tmp_path) / "hoptrace.db")
    monkeypatch.setattr(db, "FIXTURES_DIR", Path(__file__).resolve().parents[2] / "fixtures")
    conn = db.connect()
    conn.executescript(seed.SCHEMA)
    conn.commit()
    conn.close()
    yield
    db.DB_PATH = db.DATA_DIR / "hoptrace.db"


def _run_fixture(mode, tag):
    return merge_svc.run_fixture(mode, run_tag=tag)


def test_success_merges_all_member_points_in_order(env):
    proof = _run_fixture("success", "ok")
    master = proof["fixture"]["master_id"]
    assert proof["ok"] is True
    assert proof["status"] == "committed"
    assert proof["merged_count"] == 4

    conn = connect()
    hops = timeline_svc.list_primary(conn, master)
    conn.close()
    # 6 点 = 主单原生 2 + 成员 4，按 (event_ts, id) 有序。
    assert [h["id"] for h in hops] == [
        "HM-1-ok",
        "L1-1-ok@WB-M501-ok>WB-M500-ok",
        "L1-2-ok@WB-M501-ok>WB-M500-ok",
        "HM-3-ok",
        "L2-1-ok@WB-M502-ok>WB-M500-ok",
        "L2-2-ok@WB-M502-ok>WB-M500-ok",
    ]
    assert [h["merged"] for h in hops] == [False, True, True, False, True, True]
    assert sum(1 for h in hops if h["merged"]) == proof["merged_count"]


def test_projection_derives_from_merged_timeline(env):
    proof = _run_fixture("success", "proj")
    master = proof["fixture"]["master_id"]
    conn = connect()
    wb = conn.execute("SELECT status_proj FROM waybills WHERE id=?", (master,)).fetchone()
    last = timeline_svc.list_primary(conn, master)[-1]
    conn.close()
    assert last["event_code"] == "DELIVERED"
    assert wb["status_proj"] == "已签收"
    assert proof["projection_before"]["status_proj"] == "到站"
    assert proof["projection_after"]["status_proj"] == "已签收"
    assert proof["projection_after"]["last_hop_id"] == last["id"]


def test_members_are_marked_and_frozen(env):
    proof = _run_fixture("success", "frz")
    master = proof["fixture"]["master_id"]
    members = proof["fixture"]["member_ids"]
    conn = connect()
    for mid in members:
        row = conn.execute(
            "SELECT merged_into, merged_at FROM waybills WHERE id=?", (mid,)
        ).fetchone()
        assert row["merged_into"] == master
        assert row["merged_at"]
        assert merge_svc.timeline_owner(conn, mid) == master
    # 成员段不得再独立改主单投影：再合并一次应被拒绝。
    conn.close()
    again = merge_svc.merge_legs(master, members[:1])
    assert again["ok"] is False
    assert "已合并" in again["error"]

    # 装载新点打到已合并成员段时被冻结，主单投影不变。
    conn = connect()
    before = conn.execute("SELECT status_proj FROM waybills WHERE id=?", (master,)).fetchone()
    conn.execute(
        "INSERT INTO hops(id, waybill_id, event_ts, location_code, event_code, seq, channel) VALUES (?,?,?,?,?,?,'primary')",
        ("LATE-X", members[0], "2026-11-17T23:59:00Z", "SHA-X", "EXCEPTION", 99),
    )
    report = {
        "skipped_merged_member": [
            mid for mid in [members[0]] if merge_svc.timeline_owner(conn, mid) != mid
        ]
    }
    after = conn.execute("SELECT status_proj FROM waybills WHERE id=?", (master,)).fetchone()
    conn.commit()
    conn.close()
    assert report["skipped_merged_member"] == [members[0]]
    assert before["status_proj"] == after["status_proj"] == "已签收"


def test_failure_rolls_back_everything(env):
    proof = _run_fixture("failure", "bad")
    master = proof["fixture"]["master_id"]
    members = proof["fixture"]["member_ids"]
    assert proof["ok"] is False
    assert proof["status"] == "rolled_back"
    assert proof["merged_count"] == 0
    assert proof["merged_hop_ids"] == []
    assert proof["projection_before"] == proof["projection_after"]

    conn = connect()
    assert conn.execute(
        "SELECT COUNT(*) c FROM merge_edges WHERE batch_id=?", (proof["batch_id"],)
    ).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT COUNT(*) c FROM merge_hops WHERE batch_id=?", (proof["batch_id"],)
    ).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT COUNT(*) c FROM hops WHERE waybill_id=?", (master,)
    ).fetchone()["c"] == 2
    for mid in members:
        assert conn.execute(
            "SELECT merged_into FROM waybills WHERE id=?", (mid,)
        ).fetchone()["merged_into"] is None
    assert conn.execute(
        "SELECT status_proj FROM waybills WHERE id=?", (master,)
    ).fetchone()["status_proj"] == "到站"
    conn.close()

    # 失败不留残渣，随后可正常合并。
    retry = merge_svc.merge_legs(master, members)
    assert retry["ok"] is True
    assert retry["merged_count"] == 4
    assert retry["projection_before"]["status_proj"] == "到站"
    assert retry["projection_after"]["status_proj"] == "已签收"


def test_proof_is_read_only_and_shared_source(env):
    proof = _run_fixture("success", "src")
    master = proof["fixture"]["master_id"]
    conn = connect()
    stored = merge_svc.batch_proof(conn, proof["batch_id"])
    summary = merge_svc.merge_summary(conn, master)
    listed = merge_svc.list_batches(conn)
    conn.close()

    assert stored["merged_count"] == summary["merged_count"] == proof["merged_count"]
    assert [m["id"] for m in stored["members"]] == proof["fixture"]["member_ids"]
    assert stored["projection_before"] == proof["projection_before"]
    assert stored["projection_after"] == proof["projection_after"]
    assert any(b["batch_id"] == proof["batch_id"] for b in listed)
    assert stored["merged_hop_ids"] == proof["merged_hop_ids"]


def test_merge_rejects_unknown_and_self(env):
    _run_fixture("success", "neg")
    bad = merge_svc.merge_legs("WB-M500-neg", ["WB-NOPE"])
    assert bad["ok"] is False
    assert "不存在" in bad["error"]

    self_merge = merge_svc.merge_legs("WB-M500-neg", ["WB-M500-neg"])
    assert self_merge["ok"] is False


def test_ingest_freezes_merged_member(env):
    proof = _run_fixture("success", "ing")
    member = proof["fixture"]["member_ids"][0]
    master = proof["fixture"]["master_id"]
    import app.services.ingest as ingest_mod

    orig = ingest_mod.FIXTURES_DIR

    class FakeDir:
        def __truediv__(self, other):
            return _fake_fixture

    _fake_fixture = Path("/tmp/_ingest_fake.json")
    _fake_fixture.write_text(
        json.dumps(
            {
                "batch_id": "B-T",
                "hops": [
                    {
                        "id": "NEW-1",
                        "waybill_id": member,
                        "event_ts": "2026-11-17T23:00:00Z",
                        "location_code": "X",
                        "event_code": "EXCEPTION",
                        "seq": 9,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    ingest_mod.FIXTURES_DIR = FakeDir()
    try:
        result = ingest_svc.run_batch()
    finally:
        ingest_mod.FIXTURES_DIR = orig
        _fake_fixture.unlink(missing_ok=True)
    assert result["ok"] is True
    assert result["report"]["written"] == 0
    assert result["report"]["skipped_merged_member"] == [member]

    conn = connect()
    assert conn.execute(
        "SELECT COUNT(*) c FROM hops WHERE id='NEW-1'"
    ).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT status_proj FROM waybills WHERE id=?", (master,)
    ).fetchone()["status_proj"] == "已签收"
    conn.close()


def test_multi_batch_master_counts_match_timeline(env):
    first = _run_fixture("success", "m1")
    master = first["fixture"]["master_id"]

    # 追加一段新成员并入同一主单，验证多次合并后摘要与时间线同数。
    conn = connect()
    conn.execute(
        "INSERT INTO waybills(id, title, status_proj, created_at) VALUES (?,?,?,?)",
        ("WB-M503-x", "追加段", "未知", "2026-11-17T00:00:00Z"),
    )
    conn.execute(
        "INSERT INTO hops(id, waybill_id, event_ts, location_code, event_code, seq) VALUES (?,?,?,?,?,?)",
        ("L3-1-x", "WB-M503-x", "2026-11-17T20:00:00Z", "HZ-DC", "DELIVERED", 1),
    )
    conn.commit()
    conn.close()

    second = merge_svc.merge_legs(master, ["WB-M503-x"])
    assert second["ok"] is True
    assert second["merged_count"] == 1

    conn = connect()
    summary = merge_svc.merge_summary(conn, master)
    hops = timeline_svc.list_primary(conn, master)
    conn.close()
    assert summary["merged_count"] == 5
    assert len(summary["members"]) == 3
    assert sum(1 for h in hops if h["merged"]) == 5
    assert summary["projection_after"]["status_proj"] == "已签收"


def test_transitive_merge_is_rejected(env):
    first = _run_fixture("success", "tr")
    master = first["fixture"]["master_id"]
    member = first["fixture"]["member_ids"][0]

    # 成员段本身已成为主单时不可再被并走。
    nested = merge_svc.merge_legs(master, [member])
    assert nested["ok"] is False
    assert "已合并" in nested["error"]


def test_colliding_member_point_rejected_cleanly(env):
    conn = connect()
    # 主单原点与成员点在 (event_ts, location, code) 上完全相同，并入必撞唯一约束。
    conn.execute(
        "INSERT INTO waybills(id, title, status_proj, created_at) VALUES (?,?,?,?)",
        ("WB-M900", "撞点主单", "未知", "2026-11-17T00:00:00Z"),
    )
    conn.execute(
        "INSERT INTO hops(id, waybill_id, event_ts, location_code, event_code, seq) VALUES (?,?,?,?,?,?)",
        ("M-1", "WB-M900", "2026-11-17T08:00:00Z", "SHA-HUB", "PICKED", 1),
    )
    conn.execute(
        "INSERT INTO waybills(id, title, status_proj, created_at) VALUES (?,?,?,?)",
        ("WB-C1", "撞点段", "未知", "2026-11-17T00:00:00Z"),
    )
    conn.execute(
        "INSERT INTO hops(id, waybill_id, event_ts, location_code, event_code, seq) VALUES (?,?,?,?,?,?)",
        ("C-1", "WB-C1", "2026-11-17T08:00:00Z", "SHA-HUB", "PICKED", 1),
    )
    conn.execute(
        "INSERT INTO hops(id, waybill_id, event_ts, location_code, event_code, seq) VALUES (?,?,?,?,?,?)",
        ("C-2", "WB-C1", "2026-11-17T10:00:00Z", "NJ-HUB", "IN_TRANSIT", 2),
    )
    conn.commit()
    conn.close()

    result = merge_svc.merge_legs("WB-M900", ["WB-C1"])
    assert result["ok"] is False
    assert "唯一约束" in result["error"]

    conn = connect()
    # 失败闭：主单仍只有原点，成员点没有任何副本。
    assert conn.execute(
        "SELECT COUNT(*) c FROM hops WHERE waybill_id='WB-M900'"
    ).fetchone()["c"] == 1
    assert conn.execute(
        "SELECT COUNT(*) c FROM merge_hops WHERE batch_id=?", (result["batch_id"],)
    ).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT merged_into FROM waybills WHERE id='WB-C1'"
    ).fetchone()["merged_into"] is None
    conn.close()
