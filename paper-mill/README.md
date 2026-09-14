# 纸坊抄纸排产与成品发货系统

纯 Python 标准库 + SQLite 实现，无第三方依赖。覆盖工单排产、成品登记、质检入库、
订单锁定、分批发货、退货回库全流程，并内建幂等、角色鉴权、并发防超卖与失败回滚。

## 运行

```bash
# 启动服务（默认 127.0.0.1:8039，库文件 paper-mill/data/mill.db）
python3 app.py

# 端到端验证（自动起停真实子进程，约 5 秒）
python3 demo.py
```

验证脚本会依次断言 **40 项**检查：业务主线 + 排期冲突 + 重复提交 + 越权改级 +
并发超卖 + 失败回滚 + 重启恢复。

## 业务主线（状态机，不可跳步）

```
工单 created
  │  排产（纸槽 + 班组 + 时段，槽/班时段均不得重叠）
  ▼
工单 scheduled ──登记卷（卷号/重量/克重）──► 卷 pending_qc
  │ 质检 A/B 合格                         │ 质检 C
  ▼                                        ▼
卷 in_stock ◄──────────────┐            qc_rejected
  │ 锁定到销售订单          │ 退货回库
  ▼                        │
卷 locked                  │
  │ 分批发货                │
  ▼                        │
卷 shipped ────────────────┘
```

- 只有**已排产**工单才能登记成品；只有**合格且在库**的卷才能锁定
- 卷必须**锁在该订单上**才能发货；订单可分批发货，已发重量不得超过订单总量
- 发货后退货：卷回 `in_stock`、释放历史锁定、冲减订单已发重量（CHECK 约束保底不为负），可再次锁定发货

## 接口

所有写操作：`POST` JSON；角色通过 `X-Role` 头或 body 内 `role` 传递；
幂等键通过 `Idempotency-Key` 头或 body 内 `idem_key` 传递。

| 路径 | 角色 | 说明 |
|---|---|---|
| `/api/work-orders` | planner | 建工单（克重、计划产量 kg） |
| `/api/schedules` | planner | 排产：`machine/team/start/end`，半开区间 `[start,end)` |
| `/api/rolls` | worker | 登记卷：卷号、重量 kg、克重（须与工单一致） |
| `/api/rolls/inspect` | qc | 质检定级 A/B（合格）/ C（不合格） |
| `/api/rolls/regrade` | qc | 在库卷改级（锁定中不可改） |
| `/api/orders` | dispatcher | 建销售订单 |
| `/api/locks` | dispatcher | 卷数组锁定到订单 |
| `/api/shipments` | dispatcher | 已锁卷分批发货 |
| `/api/returns` | warehouse | 对发货单退货回库 |
| `GET /api/state` | — | 全量账本快照 |
| `GET /healthz` | — | 健康检查 |

角色：`planner` 计划员、`worker` 抄纸工、`qc` 质检员、`dispatcher` 发运员、
`warehouse` 仓库员、`admin` 管理员（可执行任意操作）。

### curl 示例

```bash
curl -s -X POST localhost:8039/api/work-orders -H 'X-Role: planner' \
  -H 'Content-Type: application/json' \
  -d '{"code":"WO-1","product_gsm":80,"planned_kg":500}'

curl -s -X POST localhost:8039/api/schedules -H 'X-Role: planner' \
  -H 'Idempotency-Key: sch-wo1-1' -H 'Content-Type: application/json' \
  -d '{"work_order":"WO-1","machine":"一号槽","team":"甲班",
       "start":"2026-09-20T08:00","end":"2026-09-20T16:00"}'
```

错误统一为 `{"error": "<码>", "message": "<说明>"}`，HTTP 状态码：
400 参数非法（含负/零重量）、401 未知角色、403 越权、404 不存在、409 业务冲突
（排期冲突、重复锁定、超卖、幂等键跨操作复用等）、500 内部错误。

## 关键不变量与实现手段

1. **排期不重叠**：`schedules` 上同一纸槽或同一班组的半开区间重叠查询，撞期整体拒绝；
   工单有 UNIQUE 排产约束，不能重复排。
2. **卷全局唯一**：`rolls.roll_no` UNIQUE，重复卷号拒绝。
3. **一卷只能被一个订单活锁**：部分唯一索引
   `CREATE UNIQUE INDEX ux_active_alloc ON allocations(roll_id) WHERE active=1`，
   数据库层兜底，绕过应用检查也不可能双锁。
4. **不超卖/不负数**：订单总量/已发量有 `CHECK`；锁定与发货在事务内重新汇总
   已锁/已发并比对余量；卷状态用**守卫更新**
   `UPDATE ... WHERE status='in_stock'`（或 `'locked'`），影响行数不为 1 即报错。
5. **并发安全**：每个写请求 `BEGIN IMMEDIATE` 立即取 SQLite 写锁，
   并发锁定/发货被数据库串行化，后到者要么看到卷已锁、要么看到余量不足而 409，
   绝不超卖。
6. **幂等**：幂等键 + 操作名存 `idempotency` 表；同键同操作重放首次响应（`replayed:true`），
   同键跨操作返回 409 `idem_key_reused`（标识不得跨操作复用）。
7. **无半笔账**：一次业务动作的全部写入（状态、锁定关系、账本累计）在单事务内完成，
   任何一步失败或提交前故障都整体回滚；幂等记录与业务写入同事务，失败不脏占键。
8. **重启恢复**：SQLite WAL 持久化，重启后数据与幂等键均有效；重复请求依旧重放。

## 验证场景（demo.py）

- 主线：WO 排产 → 3 卷登记 → A/B/C 质检 → 锁 2 卷 → 两批发货 → 首批退货回库再锁
- 排期冲突：同槽重叠、同班重叠（换槽也拒）、首尾相接放行、重复排产拒绝
- 重复提交：同键重放原结果、跨操作复用拒绝、同键换参数仍重放首次结果
- 越权改级：发运员改级/抄纸工质检 403、未知角色 401、越权且参数残缺也先 403
- 跳步与非法值：未排产登记、判废卷锁定、克重不符、负/零重量、未锁发货
- 并发超卖：10 线程抢锁 300kg 订单恰锁 300kg；同卷同键 6 并发恰 1 笔成功其余重放；
  同卷 8 线程并发发货恰 1 笔成功
- 失败回滚：`MILL_FAIL_ON=lock_rolls` 让故障实例在提交前抛错，卷状态、锁定关系、
  幂等键全部回滚，正常实例可用同一键重提成功
- 重启恢复：杀进程后以同库重启，快照逐字节一致，旧幂等键继续重放，业务可续做
