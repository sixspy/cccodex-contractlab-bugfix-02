
import hashlib, secrets, sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 缓冲对齐册：路径一经选定，当天不换；实现与路径参数都必须与册一致。
ALIGN_PATH = "甲"
CLAIM_WAIT_SECONDS = 10.0
CLAIM_POLL_SECONDS = 0.02
CLAIM_TTL_SECONDS = 120
ALIGN_PATHS = {
    "甲": {
        "title": "甲：写缓冲行与批次标缓冲捆在同一事务，缺一侧则整笔回滚",
        "body": "批次 INSERT 与 buffers INSERT 共用同一连接同一事务；缓冲行写失败整笔回滚，不留半笔。",
    },
    "乙": {
        "title": "乙：先落对齐草稿，核双向外键齐备后再原子提交，孤儿必须拒绝",
        "body": "本册未选用乙路径；若选用须先改造草稿表，不得与甲路径混用。",
    },
}

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def now() -> str:
    return datetime.now().isoformat(timespec="seconds")

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffer_alignment_ledger(
          id INTEGER PRIMARY KEY CHECK (id = 1),
          path TEXT NOT NULL,
          path_title TEXT NOT NULL,
          chosen_at TEXT NOT NULL,
          locked INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS alignment_claims(
          batch_id INTEGER PRIMARY KEY,
          operator TEXT NOT NULL,
          claimed_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS alignment_ops(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          operator TEXT NOT NULL,
          path TEXT NOT NULL,
          status TEXT NOT NULL,
          action TEXT,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS alignment_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          batch_id INTEGER,
          operator TEXT,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        # 对齐册只落一次，选定后当天不换；若库中已有别的路径则拒绝启动期混用。
        led = conn.execute("SELECT * FROM buffer_alignment_ledger WHERE id=1").fetchone()
        if led is None:
            conn.execute(
                "INSERT INTO buffer_alignment_ledger(id,path,path_title,chosen_at,locked) VALUES (1,?,?,?,1)",
                (ALIGN_PATH, ALIGN_PATHS[ALIGN_PATH]["title"], now()),
            )
        elif led["path"] != ALIGN_PATH:
            raise RuntimeError(f"对齐册选定路径 {led['path']} 与实现路径 {ALIGN_PATH} 混用，拒绝启动")
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))

@app.on_event("startup")
def _s():
    init_db()

# ------------------------------------------------------------------ 对齐工具

class AlignmentError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status = status
        self.detail = detail

def _insert_buffer_row(conn, batch_id: int, reason: str) -> int:
    cur = conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (batch_id, reason))
    return cur.lastrowid

def _log_event(kind: str, batch_id=None, operator=None, detail: str = "") -> None:
    with connect() as c:
        c.execute(
            "INSERT INTO alignment_events(kind,batch_id,operator,detail,created_at) VALUES (?,?,?,?,?)",
            (kind, batch_id, operator, detail, now()),
        )

def _snapshot(conn):
    """双向集合口径：缓冲批次集合与缓冲行集合逐项相减，不用计数相等充对齐。

    返回 (buffered, primary, batch_only, duplicate_ids, orphan_ids)：
    batch_only=有缓冲无行；duplicate_ids=同一缓冲批次的重复行；orphan_ids=有行无缓冲。
    """
    buffered = [r["id"] for r in conn.execute(
        "SELECT id FROM ingress_batches WHERE result='缓冲' ORDER BY id").fetchall()]
    rows = conn.execute("SELECT id,batch_id FROM buffers ORDER BY id").fetchall()
    primary: dict[int, int] = {}
    duplicate_ids: list[int] = []
    orphan_ids: list[int] = []
    for r in rows:
        if r["batch_id"] in buffered:
            if r["batch_id"] in primary:
                duplicate_ids.append(r["id"])
            else:
                primary[r["batch_id"]] = r["id"]
        else:
            orphan_ids.append(r["id"])
    batch_only = [bid for bid in buffered if bid not in primary]
    return buffered, primary, batch_only, duplicate_ids, orphan_ids

def _ledger(conn) -> dict:
    row = conn.execute("SELECT * FROM buffer_alignment_ledger WHERE id=1").fetchone()
    d = dict(row)
    d["locked"] = bool(d["locked"])
    return d

def _batch_alignment(conn, batch_id: int, buffered: bool) -> dict:
    rows = conn.execute("SELECT id FROM buffers WHERE batch_id=? ORDER BY id", (batch_id,)).fetchall()
    if buffered and len(rows) == 1:
        state = "aligned"
    elif buffered and not rows:
        state = "batch_only"
    elif buffered:
        state = "duplicate"
    elif rows:
        state = "buffer_only"
    else:
        state = "none"
    return {
        "state": state,
        "buffer_id": rows[0]["id"] if rows else None,
        "buffer_rows": len(rows),
        "batch_buffered": buffered,
    }

def _buffer_items(conn) -> list[dict]:
    """双写缓冲队列：缓冲批次无行、缓冲行无批次，两类都出列。"""
    batches = {r["id"]: dict(r) for r in conn.execute("SELECT * FROM ingress_batches").fetchall()}
    buffered, primary, batch_only, duplicate_ids, orphan_ids = _snapshot(conn)
    items = []
    for bid in buffered:
        b = batches[bid]
        buf_id = primary.get(bid)
        reason = None
        if buf_id is not None:
            reason = conn.execute("SELECT reason FROM buffers WHERE id=?", (buf_id,)).fetchone()["reason"]
        items.append({
            "id": buf_id, "buffer_id": buf_id, "batch_id": bid,
            "subject": b["subject"], "payload_fields": b["payload_fields"],
            "result": b["result"], "reason": reason,
            "alignment": "aligned" if buf_id is not None else "batch_only",
        })
    for buf_id in duplicate_ids:
        row = conn.execute("SELECT * FROM buffers WHERE id=?", (buf_id,)).fetchone()
        b = batches[row["batch_id"]]
        items.append({
            "id": buf_id, "buffer_id": buf_id, "batch_id": row["batch_id"],
            "subject": b["subject"], "payload_fields": b["payload_fields"],
            "result": b["result"], "reason": row["reason"], "alignment": "duplicate",
        })
    for buf_id in orphan_ids:
        row = conn.execute("SELECT * FROM buffers WHERE id=?", (buf_id,)).fetchone()
        b = batches.get(row["batch_id"])
        items.append({
            "id": buf_id, "buffer_id": buf_id, "batch_id": row["batch_id"],
            "subject": b["subject"] if b else None,
            "payload_fields": b["payload_fields"] if b else None,
            "result": b["result"] if b else None, "reason": row["reason"],
            "alignment": "buffer_only",
        })
    items.sort(key=lambda x: (-(x["batch_id"] or 0), x["id"] or 0))
    return items

def _apply_backfill(batch_id: int, operator: str) -> dict:
    """单事务补齐/清理：要么这一笔对齐完成，要么整笔回滚。"""
    with connect() as c:
        batch = c.execute("SELECT * FROM ingress_batches WHERE id=?", (batch_id,)).fetchone()
        rows = c.execute("SELECT * FROM buffers WHERE batch_id=? ORDER BY id", (batch_id,)).fetchall()
        if batch is not None and batch["result"] == "缓冲":
            if not rows:
                _insert_buffer_row(c, batch_id, "对齐补齐:批次缓冲缺缓冲行")
                action, status = "补写缓冲行", "已对齐"
            elif len(rows) == 1:
                action, status = "已对齐", "已对齐"
            else:
                for r in rows[1:]:
                    c.execute("DELETE FROM buffers WHERE id=?", (r["id"],))
                action, status = "删除重复缓冲行", "已对齐"
            c.execute("DELETE FROM alignment_claims WHERE batch_id=?", (batch_id,))
            c.execute(
                "INSERT INTO alignment_ops(batch_id,operator,path,status,action,detail,created_at) VALUES (?,?,?,?,?,?,?)",
                (batch_id, operator, ALIGN_PATH, status, action, "补齐后双向有行", now()),
            )
            return {"batch_id": batch_id, "action": action, "status": status, "aligned": True}
        if not rows:
            raise AlignmentError(404, f"批次 #{batch_id} 无可对齐的缓冲行可清")
        c.execute("DELETE FROM buffers WHERE batch_id=?", (batch_id,))
        action = "删除孤儿缓冲行" if batch is None else "删除错挂缓冲行"
        detail = "缓冲区无对应批次，拒绝保留孤儿行" if batch is None else "缓冲行挂在非缓冲批次上，拒绝把拒收改成通过"
        c.execute(
            "INSERT INTO alignment_events(kind,batch_id,operator,detail,created_at) VALUES (?,?,?,?,?)",
            ("孤儿拒绝", batch_id, operator, detail, now()),
        )
        c.execute("DELETE FROM alignment_claims WHERE batch_id=?", (batch_id,))
        c.execute(
            "INSERT INTO alignment_ops(batch_id,operator,path,status,action,detail,created_at) VALUES (?,?,?,?,?,?,?)",
            (batch_id, operator, ALIGN_PATH, "孤儿拒绝", action, detail, now()),
        )
        return {"batch_id": batch_id, "action": action, "status": "孤儿拒绝", "aligned": False}

def _release_after_failure(batch_id: int, operator: str, detail: str) -> None:
    with connect() as c:
        c.execute("DELETE FROM alignment_claims WHERE batch_id=? AND operator=?", (batch_id, operator))
        c.execute(
            "INSERT INTO alignment_events(kind,batch_id,operator,detail,created_at) VALUES (?,?,?,?,?)",
            ("对齐失败", batch_id, operator, detail, now()),
        )
        c.execute(
            "INSERT INTO alignment_ops(batch_id,operator,path,status,action,detail,created_at) VALUES (?,?,?,?,?,?,?)",
            (batch_id, operator, ALIGN_PATH, "对齐失败", None, detail, now()),
        )

def _record_contention(batch_id: int, operator: str, detail: str) -> None:
    _log_event("对齐争用", batch_id, operator, detail)
    with connect() as c:
        c.execute(
            "INSERT INTO alignment_ops(batch_id,operator,path,status,action,detail,created_at) VALUES (?,?,?,?,?,?,?)",
            (batch_id, operator, ALIGN_PATH, "对齐争用", None, detail, now()),
        )

def _expire_stale_claim(batch_id: int) -> None:
    """持有补齐的值班若异常退出，静置超过 TTL 的认领作废，避免同批次永久堵死。"""
    cutoff = (datetime.now() - timedelta(seconds=CLAIM_TTL_SECONDS)).isoformat(timespec="seconds")
    with connect() as c:
        c.execute("DELETE FROM alignment_claims WHERE batch_id=? AND claimed_at<?", (batch_id, cutoff))

def _max_op_id() -> int:
    with connect() as c:
        row = c.execute("SELECT COALESCE(MAX(id),0) m FROM alignment_ops").fetchone()
    return row["m"]

def _aligned_by_other_after(batch_id: int, operator: str, op_id: int) -> bool:
    """本笔到达之后，同批次是否被另一名值班对齐；是则算同时抢补齐，后到只记争用。"""
    with connect() as c:
        row = c.execute(
            "SELECT 1 FROM alignment_ops WHERE batch_id=? AND status='已对齐' AND operator<>? AND id>? LIMIT 1",
            (batch_id, operator, op_id),
        ).fetchone()
    return row is not None

def _is_aligned(batch_id: int) -> bool:
    with connect() as c:
        batch = c.execute("SELECT result FROM ingress_batches WHERE id=?", (batch_id,)).fetchone()
        rows = c.execute("SELECT COUNT(*) c FROM buffers WHERE batch_id=?", (batch_id,)).fetchone()["c"]
    if batch is not None and batch["result"] == "缓冲":
        return rows == 1
    return rows == 0

def backfill_alignment(batch_id: int, operator: str, path: str) -> dict:
    if path != ALIGN_PATH:
        _log_event("对齐偏了", batch_id, operator,
                   f"请求路径 {path} 偏离对齐册选定路径 {ALIGN_PATH}")
        with connect() as c:
            c.execute(
                "INSERT INTO alignment_ops(batch_id,operator,path,status,action,detail,created_at) VALUES (?,?,?,?,?,?,?)",
                (batch_id, operator, path, "对齐偏了", None,
                 f"对齐册选定 {ALIGN_PATH}，不得混用", now()),
            )
        raise AlignmentError(409, f"对齐偏了：对齐册选定路径 {ALIGN_PATH}，请求路径 {path} 不得混用")
    deadline = monotonic() + CLAIM_WAIT_SECONDS
    arrived_after_op = _max_op_id()
    waited = False
    while True:
        try:
            with connect() as c:
                c.execute(
                    "INSERT INTO alignment_claims(batch_id,operator,claimed_at) VALUES (?,?,?)",
                    (batch_id, operator, now()),
                )
            break
        except sqlite3.IntegrityError:
            # 同批次补齐须排队：后到先等前一笔收尾，不抢跑；等不到才落争用。
            waited = True
            _expire_stale_claim(batch_id)
            if monotonic() >= deadline:
                _record_contention(batch_id, operator, "同批次补齐排队超时，后到只记对齐争用")
                raise AlignmentError(409, "同批次补齐排队中，后到只记对齐争用")
            sleep(CLAIM_POLL_SECONDS)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or monotonic() >= deadline:
                raise
            waited = True
            sleep(CLAIM_POLL_SECONDS)
    if _is_aligned(batch_id) and (waited or _aligned_by_other_after(batch_id, operator, arrived_after_op)):
        # 同批次同时抢补齐：前一笔已收尾对齐，后到不再重复落笔，只记争用。
        with connect() as c:
            c.execute("DELETE FROM alignment_claims WHERE batch_id=? AND operator=?", (batch_id, operator))
        _record_contention(batch_id, operator, "后到排队时前一笔已完成，只记对齐争用")
        raise AlignmentError(409, "同批次补齐排队中，后到只记对齐争用")
    try:
        return _apply_backfill(batch_id, operator)
    except AlignmentError as exc:
        _release_after_failure(batch_id, operator, exc.detail)
        raise
    except Exception as exc:
        _release_after_failure(batch_id, operator, f"对齐写入失败，整笔回滚：{exc}")
        raise AlignmentError(500, f"对齐失败，整笔回滚：{exc}") from exc

# ------------------------------------------------------------------ 通用接口

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    with connect() as c:
        ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
        if not ctr:
            raise HTTPException(404, "无启用正式契约")
        need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
        missing = sorted(need - fields)
        stamp = now()
        if missing:
            if ctr["mode"] == "兼容":
                # 甲路径：批次标缓冲与缓冲行同源同事务，缺一侧整笔回滚。
                cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                (body.subject, body.payload_fields, ctr["version"], "缓冲", "缺字段:"+ ",".join(missing), stamp))
                batch_id = cur.lastrowid
                try:
                    _insert_buffer_row(c, batch_id, "缺字段进双写缓冲")
                except Exception as exc:
                    raise HTTPException(500, f"缓冲行写入失败，整笔回滚：{exc}") from exc
                return {"result":"缓冲", "version": ctr["version"], "missing": missing, "batch_id": batch_id}
            cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                            (body.subject, body.payload_fields, ctr["version"], "拒收", "缺字段:"+ ",".join(missing), stamp))
            return {"result":"拒收", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
        cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", stamp))
        return {"result":"通过", "version": ctr["version"], "batch_id": cur.lastrowid}

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        buffered = {r["id"] for r in c.execute("SELECT id FROM ingress_batches WHERE result='缓冲'").fetchall()}
        items = []
        for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall():
            d = dict(r)
            d["alignment"] = _batch_alignment(c, r["id"], r["id"] in buffered)["state"]
            items.append(d)
        return {"items": items}

@app.get("/api/ingress/{batch_id}")
def ingress_detail(batch_id: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        batch = c.execute("SELECT * FROM ingress_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise HTTPException(404, "批次不存在")
        rows = [dict(r) for r in c.execute("SELECT * FROM buffers WHERE batch_id=? ORDER BY id", (batch_id,)).fetchall()]
        state = _batch_alignment(c, batch_id, batch["result"] == "缓冲")
        return {"batch": dict(batch), "buffers": rows, "alignment": state}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": _buffer_items(c)}

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
        buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
        _, _, batch_only, duplicate_ids, buffer_only = _snapshot(c)
        led = _ledger(c)
    return {
        "ingress_total": total, "passed": passed, "buffered": buffered,
        "rejected": rejected, "buffer_rows": buf_rows,
        "aligned": not batch_only and not buffer_only and not duplicate_ids,
        "batch_only_ids": batch_only, "buffer_only_ids": buffer_only,
        "duplicate_ids": duplicate_ids,
        "batch_only_count": len(batch_only), "buffer_only_count": len(buffer_only),
        "duplicate_count": len(duplicate_ids),
        "ledger": led,
    }

class BackfillIn(BaseModel):
    batch_id: int
    path: str = ALIGN_PATH
    operator: str = "值班甲"

@app.post("/api/alignment/backfill")
def backfill(body: BackfillIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    try:
        return backfill_alignment(body.batch_id, body.operator, body.path)
    except AlignmentError as exc:
        raise HTTPException(exc.status, exc.detail) from exc

@app.get("/api/alignment/events")
def alignment_events(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM alignment_events ORDER BY id DESC LIMIT 100").fetchall()
        return {"items": [dict(r) for r in rows]}

@app.get("/api/alignment/policy")
def alignment_policy(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        led = _ledger(c)
        _, _, batch_only, duplicate_ids, orphan_ids = _snapshot(c)
        batches = {r["id"]: dict(r) for r in c.execute("SELECT * FROM ingress_batches").fetchall()}
        rows = {r["id"]: dict(r) for r in c.execute("SELECT * FROM buffers").fetchall()}
        events = [dict(r) for r in c.execute("SELECT * FROM alignment_events ORDER BY id DESC LIMIT 20").fetchall()]
        ops = [dict(r) for r in c.execute("SELECT * FROM alignment_ops ORDER BY id DESC LIMIT 20").fetchall()]
    return {
        "path": led["path"],
        "path_title": led["path_title"],
        "paths": ALIGN_PATHS,
        "ledger": led,
        "batch_only": [
            {"batch_id": bid, "subject": batches[bid]["subject"], "result": batches[bid]["result"]}
            for bid in batch_only
        ],
        "buffer_only": [
            {"buffer_id": rid, "batch_id": rows[rid]["batch_id"],
             "subject": (batches.get(rows[rid]["batch_id"]) or {}).get("subject"),
             "reason": rows[rid]["reason"]}
            for rid in orphan_ids
        ],
        "duplicate": [
            {"buffer_id": rid, "batch_id": rows[rid]["batch_id"],
             "subject": batches[rows[rid]["batch_id"]]["subject"],
             "reason": rows[rid]["reason"]}
            for rid in duplicate_ids
        ],
        "events": events,
        "ops": ops,
    }

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
