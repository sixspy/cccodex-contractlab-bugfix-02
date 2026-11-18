"""缓冲对齐回归测试。

覆盖：
1. 有行无缓冲（缓冲行存在，但批次不是缓冲结果 / 批次根本不存在）
2. 有缓冲无行（批次结果为缓冲，但没有缓冲行）
3. 计数相等但双向集合不对齐的陷阱
4. 入站双写原子性（写缓冲行失败则整笔回滚）
5. 补齐的原子性、失败回滚、孤儿拒绝、路径偏离
6. 同批次并发补齐的排队与争用记账
7. 入站列表 / 详情 / 对账账册 / 策略附页双向有行
"""

import threading

import pytest
from fastapi.testclient import TestClient

import app as appmod


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(appmod, "DB_PATH", tmp_path / "t.db")
    appmod.init_db()
    client = TestClient(appmod.app)
    tok = client.post(
        "/api/login", json={"username": "admin", "password": "123456"}
    ).json()["token"]
    client.headers.update({"Authorization": f"Bearer {tok}"})
    return client


def _db():
    return appmod.connect()


def _batch(result, subject="订单入站", fields="order_id,amount"):
    with _db() as c:
        cur = c.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
            " VALUES (?,?,?,?,?,?)",
            (subject, fields, "v3", result, f"fixture:{result}", appmod.now()),
        )
        return cur.lastrowid


def _buffer(batch_id, reason="fixture:缓冲"):
    with _db() as c:
        cur = c.execute(
            "INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (batch_id, reason)
        )
        return cur.lastrowid


def _enable_compat():
    with _db() as c:
        c.execute(
            "INSERT INTO contracts(version,subject,mode,track,status,required_fields)"
            " VALUES (?,?,?,?,?,?)",
            (
                "v5-compat",
                "订单入站",
                "兼容",
                "正式",
                "启用",
                "order_id,amount,currency,channel",
            ),
        )


def _events(client):
    return client.get("/api/alignment/events").json()["items"]


# ---------------------------------------------------------------- 复现两例


def test_repro_buffer_row_without_buffered_batch(env):
    """有行无缓冲：缓冲行悬挂，或挂到非缓冲批次上。"""
    rejected = _batch("拒收")
    brid_attached = _buffer(rejected, "错挂到拒收批次")
    brid_dangling = _buffer(99999, "悬空批次")

    body = env.get("/api/buffers").json()
    by_batch = {row["batch_id"]: row for row in body["items"]}
    assert by_batch[rejected]["alignment"] == "buffer_only"
    assert by_batch[99999]["alignment"] == "buffer_only"

    rec = env.get("/api/reconcile").json()
    assert rec["aligned"] is False
    assert set(rec["buffer_only_ids"]) == {brid_attached, brid_dangling}
    assert rec["batch_only_ids"] == []


def test_repro_buffered_batch_without_buffer_row(env):
    """有缓冲无行：批次 result=缓冲，但缓冲表没有对应行。"""
    buffered = _batch("缓冲")

    body = env.get("/api/buffers").json()
    row = next(item for item in body["items"] if item["batch_id"] == buffered)
    assert row["alignment"] == "batch_only"
    assert row["buffer_id"] is None
    assert row["id"] is None

    rec = env.get("/api/reconcile").json()
    assert rec["aligned"] is False
    assert rec["batch_only_ids"] == [buffered]
    assert rec["buffer_only_ids"] == []


def test_reconcile_alignment_is_set_based_not_count_based(env):
    """一有一缺时两侧计数相等，old count 口径会误判为对齐。"""
    rejected = _batch("拒收")
    _buffer(rejected)
    _batch("缓冲")

    rec = env.get("/api/reconcile").json()
    assert rec["buffered"] == 1
    assert rec["buffer_rows"] == 1
    assert rec["aligned"] is False
    assert rec["buffer_only_ids"]
    assert rec["batch_only_ids"]


# ---------------------------------------------------------------- 双写事务


def test_buffered_ingest_writes_batch_and_buffer_same_source(env):
    _enable_compat()
    r = env.post(
        "/api/ingress/validate",
        json={"subject": "订单入站", "payload_fields": "order_id,amount"},
    )
    assert r.status_code == 200 and r.json()["result"] == "缓冲"
    bid = r.json()["batch_id"]
    with _db() as c:
        batch = c.execute("SELECT * FROM ingress_batches WHERE id=?", (bid,)).fetchone()
        rows = c.execute("SELECT * FROM buffers WHERE batch_id=?", (bid,)).fetchall()
    assert batch["result"] == "缓冲"
    assert len(rows) == 1 and rows[0]["batch_id"] == bid
    assert env.get("/api/reconcile").json()["aligned"] is True


def test_buffered_ingest_rolls_back_whole_batch_when_buffer_write_fails(env, monkeypatch):
    _enable_compat()

    def boom(*args, **kwargs):
        raise RuntimeError("模拟缓冲行写入失败")

    monkeypatch.setattr(appmod, "_insert_buffer_row", boom)
    r = env.post(
        "/api/ingress/validate",
        json={"subject": "订单入站", "payload_fields": "order_id,amount"},
    )
    assert r.status_code == 500
    assert "整笔回滚" in r.json()["detail"]
    with _db() as c:
        assert c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"] == 0
        assert c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 0


# ---------------------------------------------------------------- 补齐与孤儿


def test_backfill_batch_only_creates_missing_row(env):
    buffered = _batch("缓冲")
    r = env.post("/api/alignment/backfill", json={"batch_id": buffered})
    assert r.status_code == 200
    assert r.json()["action"] == "补写缓冲行"
    with _db() as c:
        rows = c.execute("SELECT * FROM buffers WHERE batch_id=?", (buffered,)).fetchall()
    assert len(rows) == 1
    assert env.get("/api/reconcile").json()["aligned"] is True

    again = env.post("/api/alignment/backfill", json={"batch_id": buffered})
    assert again.status_code == 200
    assert again.json()["action"] == "已对齐"


def test_backfill_buffer_only_removes_orphan_without_flipping_result(env):
    rejected = _batch("拒收")
    brid = _buffer(rejected)
    r = env.post("/api/alignment/backfill", json={"batch_id": rejected})
    assert r.status_code == 200
    assert r.json()["action"] == "删除错挂缓冲行"
    with _db() as c:
        batch = c.execute("SELECT * FROM ingress_batches WHERE id=?", (rejected,)).fetchone()
        assert batch["result"] == "拒收"
        assert c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 0
    rec = env.get("/api/reconcile").json()
    assert rec["passed"] == 0 and rec["rejected"] == 1
    assert any(e["kind"] == "孤儿拒绝" for e in _events(env))
    assert brid not in env.get("/api/reconcile").json()["buffer_only_ids"]


def test_backfill_dangling_orphan_is_rejected_and_removed(env):
    _buffer(99999, "悬空")
    r = env.post("/api/alignment/backfill", json={"batch_id": 99999})
    assert r.status_code == 200
    assert r.json()["action"] == "删除孤儿缓冲行"
    with _db() as c:
        assert c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 0
    assert any(e["kind"] == "孤儿拒绝" for e in _events(env))


def test_backfill_removes_duplicate_buffer_rows(env):
    buffered = _batch("缓冲")
    _buffer(buffered, "首行")
    _buffer(buffered, "重复行")
    assert env.get("/api/reconcile").json()["aligned"] is False
    r = env.post("/api/alignment/backfill", json={"batch_id": buffered})
    assert r.status_code == 200
    assert r.json()["action"] == "删除重复缓冲行"
    with _db() as c:
        rows = c.execute("SELECT * FROM buffers WHERE batch_id=?", (buffered,)).fetchall()
    assert len(rows) == 1
    assert env.get("/api/reconcile").json()["aligned"] is True


def test_backfill_failure_rolls_back_and_records_failure(env, monkeypatch):
    buffered = _batch("缓冲")

    def boom(*args, **kwargs):
        raise RuntimeError("模拟补齐写入失败")

    monkeypatch.setattr(appmod, "_insert_buffer_row", boom)
    with pytest.raises(appmod.AlignmentError) as ei:
        appmod.backfill_alignment(buffered, "值班甲", appmod.ALIGN_PATH)
    assert ei.value.status == 500
    with _db() as c:
        assert c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 0
        claims = c.execute("SELECT COUNT(*) c FROM alignment_claims").fetchone()["c"]
    assert claims == 0
    assert any(e["kind"] == "对齐失败" for e in _events(env))


def test_backfill_deviation_from_ledger_is_rejected(env):
    buffered = _batch("缓冲")
    r = env.post(
        "/api/alignment/backfill", json={"batch_id": buffered, "path": "乙"}
    )
    assert r.status_code == 409
    assert "对齐偏了" in r.json()["detail"]
    with _db() as c:
        assert c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 0
    assert any(e["kind"] == "对齐偏了" for e in _events(env))


# ---------------------------------------------------------------- 并发争用


def test_concurrent_backfill_same_batch_queues_then_records_contention(env, monkeypatch):
    buffered = _batch("缓冲")
    original = appmod._apply_backfill
    entered = threading.Event()
    release = threading.Event()

    def slow(batch_id, operator):
        entered.set()
        assert release.wait(5)
        return original(batch_id, operator)

    monkeypatch.setattr(appmod, "_apply_backfill", slow)
    results = {}

    def run(key, operator):
        try:
            results[key] = appmod.backfill_alignment(buffered, operator, appmod.ALIGN_PATH)
        except appmod.AlignmentError as exc:  # pragma: no cover - asserted below
            results[key] = exc

    first = threading.Thread(target=run, args=("first", "值班甲"))
    first.start()
    assert entered.wait(5)

    second = threading.Thread(target=run, args=("second", "值班乙"))
    second.start()
    # 后到须排队：前一笔未收尾前不得返回，也不得抢跑写库。
    second.join(0.3)
    assert second.is_alive()
    assert "second" not in results

    release.set()
    first.join(5)
    second.join(5)
    assert not second.is_alive()
    assert results["first"]["action"] == "补写缓冲行"
    assert isinstance(results["second"], appmod.AlignmentError)
    assert results["second"].status == 409
    assert "对齐争用" in results["second"].detail
    with _db() as c:
        rows = c.execute("SELECT COUNT(*) c FROM buffers WHERE batch_id=?", (buffered,)).fetchone()["c"]
    assert rows == 1
    assert any(e["kind"] == "对齐争用" for e in _events(env))


def test_sequential_repeat_after_alignment_is_idempotent_not_contention(env):
    """前一笔已完全收尾后才到的不再算同时抢补齐：幂等返回已对齐，不记争用。"""
    buffered = _batch("缓冲")
    first = env.post(
        "/api/alignment/backfill", json={"batch_id": buffered, "operator": "值班甲"}
    )
    assert first.status_code == 200 and first.json()["action"] == "补写缓冲行"

    later = env.post(
        "/api/alignment/backfill", json={"batch_id": buffered, "operator": "值班甲"}
    )
    assert later.status_code == 200 and later.json()["action"] == "已对齐"
    assert not any(e["kind"] == "对齐争用" for e in _events(env))


def test_race_window_between_arrival_and_claim_records_contention(env, monkeypatch):
    """本笔到达时前一笔尚未落对齐记录、抢锁时前一笔已收尾：仍属同时抢补齐。"""
    buffered = _batch("缓冲")
    winner = env.post(
        "/api/alignment/backfill", json={"batch_id": buffered, "operator": "值班甲"}
    )
    assert winner.status_code == 200
    # 模拟后到者在本笔到达时看到的对齐记录水位还在前一笔落笔之前。
    monkeypatch.setattr(appmod, "_max_op_id", lambda: 0)
    late = env.post(
        "/api/alignment/backfill", json={"batch_id": buffered, "operator": "值班乙"}
    )
    assert late.status_code == 409
    assert "对齐争用" in late.json()["detail"]
    assert any(e["kind"] == "对齐争用" for e in _events(env))


# ---------------------------------------------------------------- 四处双向有行


def test_ingress_list_and_detail_expose_both_directions(env):
    rejected = _batch("拒收")
    _buffer(rejected)
    buffered = _batch("缓冲")

    items = env.get("/api/ingress").json()["items"]
    states = {item["id"]: item["alignment"] for item in items}
    assert states[rejected] == "buffer_only"
    assert states[buffered] == "batch_only"

    d1 = env.get(f"/api/ingress/{rejected}").json()
    assert d1["alignment"]["state"] == "buffer_only"
    assert len(d1["buffers"]) == 1

    d2 = env.get(f"/api/ingress/{buffered}").json()
    assert d2["alignment"]["state"] == "batch_only"
    assert d2["buffers"] == []


def test_policy_appendix_shows_both_directions_and_ledger(env):
    rejected = _batch("拒收")
    _buffer(rejected)
    buffered = _batch("缓冲")

    policy = env.get("/api/alignment/policy").json()
    assert policy["path"] == "甲"
    assert "同一事务" in policy["ledger"]["path_title"]
    assert policy["ledger"]["path"] == "甲"
    assert any(row["batch_id"] == buffered for row in policy["batch_only"])
    assert any(row["batch_id"] == rejected for row in policy["buffer_only"])
