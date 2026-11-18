# 入站契约

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 入站契约 |
| 目录 | `05-contractlab` |
| 领域建议 | 数据采集、爬虫与 RPA |
| 接口 | http://127.0.0.1:10727 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

字段增删改类型、宽严模式、影子契约、缓冲窗、生产者消费者双视角

## 避开

别做成质检签发换皮；别做成纯表单 CRUD

## 互斥切口候选

- 破坏性变更直接拒收 vs 双写缓冲
- 影子只读试跑 vs 双轨分开展示
- 契约回滚整版原子切换 vs 按生产者摘流

## 缓冲对齐册

缓冲行与批次标缓冲须同源写；对齐口径按集合双向核，不看计数。

- **选定路径：甲**（写进 `buffer_alignment_ledger`，当天不换）
  - 甲：写缓冲行与批次标缓冲捆在同一事务，缺一侧则整笔回滚
  - 乙：先落对齐草稿，核双向外键齐备后再原子提交，孤儿必须拒绝（本仓未选用，不得与甲混用）
- 偏离对齐册的补齐请求一律 409 拒绝，记 `对齐偏了`
- 同批次两名值班同时补齐须排队；后到在前一笔收尾后只记 `对齐争用`，不重复落笔
- 孤儿（缓冲行无批次 / 挂在非缓冲批次）必须拒绝，删除缓冲行但绝不改批次结果为通过

对齐状态在缓冲队列、入站列表、批次详情、对账账册、对齐册附页五处双向可见：

| 接口 | 用途 |
|------|------|
| `GET /api/ingress` | 入站列表，每行带 `alignment` |
| `GET /api/ingress/{id}` | 批次详情，返回 `batch` / `buffers` / `alignment` |
| `GET /api/buffers` | 缓冲队列，同时列出 `batch_only` 与 `buffer_only` |
| `GET /api/reconcile` | 对账账册，返回 `batch_only_ids` / `buffer_only_ids` |
| `GET /api/alignment/policy` | 对齐册与策略附页，含双向明细与对齐记录 |
| `POST /api/alignment/backfill` | 单事务补齐 / 清孤儿，`{"batch_id": n, "path": "甲"}` |
| `GET /api/alignment/events` | 对齐争用 / 对齐失败 / 对齐偏了 / 孤儿拒绝 事件 |

## 启动（一键）

`powershell
cd projects/05-contractlab
docker compose up -d --build
`

浏览器：http://127.0.0.1:10727/  
健康检查：GET http://127.0.0.1:10727/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10602
python -m pytest -q
`
