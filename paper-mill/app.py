#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
纸坊抄纸排产与成品发货系统

业务主线（状态机，不可跳步）：
  工单 --排产(纸槽/班组/时段)--> 已排产 --登记卷(卷号/重量/克重)--> 待检
       --质检合格--> 在库 --锁定到销售订单--> 已锁定 --分批发货--> 已发货
       --退货--> 回库（可再次锁定）

不变量：
  * 同一纸槽、同一班组的排期时段不得重叠（半开区间 [start, end)）
  * 只有已排产工单才能登记成品；只有合格的在库卷才能锁定
  * 一个卷同一时刻只能被一个订单锁定（部分唯一索引）
  * 锁定/发货重量不得超过订单剩余量（超发禁止），重量必须为正（负重量禁止）
  * 每张写操作请求携带幂等键；重复请求返回原结果；幂等键不得跨操作复用
  * 所有写操作在单条 BEGIN IMMEDIATE 事务内完成，失败整体回滚
"""

import json
import os
import sqlite3
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

DB_PATH = os.environ.get(
    "MILL_DB",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "mill.db"),
)

# 角色：计划员、抄纸工、质检员、发运员、仓库员、管理员（管理员可执行任意操作）
ROLES = {"planner", "worker", "qc", "dispatcher", "warehouse", "admin"}


class ApiError(Exception):
    def __init__(self, status, code, message=None):
        super().__init__(message or code)
        self.status = status
        self.code = code
        self.message = message or code


# ---------------------------------------------------------------- 基础工具

def now_iso():
    return datetime.now().replace(microsecond=0).isoformat()


def parse_ts(value):
    if not isinstance(value, str):
        raise ApiError(400, "bad_time", "时间必须是 ISO 8601 字符串")
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise ApiError(400, "bad_time", f"无法解析时间：{value}")


def norm_ts(value):
    return parse_ts(value).replace(microsecond=0).isoformat()


def to_grams(value, field="weight_kg"):
    """入参重量单位为千克（允许小数），内部统一存整数克；禁止零和负数。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ApiError(400, "bad_weight", f"{field} 必须是正数（千克）")
    grams = int(round(float(value) * 1000))
    if grams <= 0:
        raise ApiError(400, "negative_weight", f"{field} 必须大于 0")
    return grams


def require_gsm(value, field="gsm"):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ApiError(400, "bad_gsm", f"{field} 必须是正整数")
    return value


# ---------------------------------------------------------------- 数据库

SCHEMA = """
CREATE TABLE IF NOT EXISTS work_orders (
    id          INTEGER PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    product_gsm INTEGER NOT NULL,           -- 计划克重 g/m²
    planned_g   INTEGER NOT NULL,           -- 计划产量（克）
    status      TEXT NOT NULL DEFAULT 'created',  -- created / scheduled
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schedules (
    id            INTEGER PRIMARY KEY,
    work_order_id INTEGER NOT NULL UNIQUE,  -- 一个工单只排一次产
    machine       TEXT NOT NULL,            -- 抄纸槽
    team          TEXT NOT NULL,            -- 班组
    start_ts      TEXT NOT NULL,
    end_ts        TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    FOREIGN KEY (work_order_id) REFERENCES work_orders(id)
);

CREATE TABLE IF NOT EXISTS rolls (
    id            INTEGER PRIMARY KEY,
    roll_no       TEXT NOT NULL UNIQUE,     -- 卷号，全局唯一
    work_order_id INTEGER NOT NULL,
    gsm           INTEGER NOT NULL,
    weight_g      INTEGER NOT NULL CHECK (weight_g > 0),
    grade         TEXT,                     -- A/B 合格，C 不合格；质检前为 NULL
    status        TEXT NOT NULL DEFAULT 'pending_qc',
                  -- pending_qc / in_stock / locked / shipped / qc_rejected
    created_at    TEXT NOT NULL,
    FOREIGN KEY (work_order_id) REFERENCES work_orders(id)
);

CREATE TABLE IF NOT EXISTS orders (
    id                INTEGER PRIMARY KEY,
    code              TEXT NOT NULL UNIQUE,
    customer          TEXT NOT NULL,
    product_gsm       INTEGER NOT NULL,
    total_weight_g    INTEGER NOT NULL CHECK (total_weight_g > 0),
    shipped_weight_g  INTEGER NOT NULL DEFAULT 0
                      CHECK (shipped_weight_g >= 0 AND shipped_weight_g <= total_weight_g),
    created_at        TEXT NOT NULL
);

-- 锁定批次：一次锁定请求一行
CREATE TABLE IF NOT EXISTS lock_batches (
    id         INTEGER PRIMARY KEY,
    code       TEXT NOT NULL UNIQUE,
    order_id   INTEGER NOT NULL,
    weight_g   INTEGER NOT NULL,
    idem_key   TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id)
);

-- 卷-订单锁定关系。active=1 表示卷当前锁在该订单上；
-- 发货后 active 置 0（释放），退货回库后可再次被锁定。
CREATE TABLE IF NOT EXISTS allocations (
    id          INTEGER PRIMARY KEY,
    roll_id     INTEGER NOT NULL,
    order_id    INTEGER NOT NULL,
    batch_id    INTEGER NOT NULL,
    weight_g    INTEGER NOT NULL,
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL,
    released_at TEXT,
    FOREIGN KEY (roll_id) REFERENCES rolls(id),
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (batch_id) REFERENCES lock_batches(id)
);
-- 一卷同时只能有一条生效锁定
CREATE UNIQUE INDEX IF NOT EXISTS ux_active_alloc ON allocations(roll_id) WHERE active = 1;

CREATE TABLE IF NOT EXISTS shipments (
    id              INTEGER PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    order_id        INTEGER NOT NULL,
    shipped_weight_g INTEGER NOT NULL DEFAULT 0,
    returned_weight_g INTEGER NOT NULL DEFAULT 0,
    idem_key        TEXT,
    created_at      TEXT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE IF NOT EXISTS shipment_items (
    id          INTEGER PRIMARY KEY,
    shipment_id INTEGER NOT NULL,
    roll_id     INTEGER NOT NULL,
    weight_g    INTEGER NOT NULL,
    returned_at TEXT,                       -- 非 NULL 表示该卷已退货回库
    UNIQUE (shipment_id, roll_id),
    FOREIGN KEY (shipment_id) REFERENCES shipments(id),
    FOREIGN KEY (roll_id) REFERENCES rolls(id)
);

CREATE TABLE IF NOT EXISTS returns (
    id          INTEGER PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    shipment_id INTEGER NOT NULL,
    weight_g    INTEGER NOT NULL,
    reason      TEXT,
    idem_key    TEXT,
    created_at  TEXT NOT NULL,
    FOREIGN KEY (shipment_id) REFERENCES shipments(id)
);

-- 幂等记录：同一幂等键只允许绑定一个操作，响应原样重放
CREATE TABLE IF NOT EXISTS idempotency (
    idem_key      TEXT PRIMARY KEY,
    op            TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
"""


def connect(db_path=DB_PATH):
    fresh = not os.path.exists(db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    if fresh:
        init_db(conn)
    return conn


def init_db(conn):
    conn.executescript(SCHEMA)


# ---------------------------------------------------------------- 服务层

class MillService:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        # 启动即建库；并发首写由 BEGIN IMMEDIATE 串行化
        conn = connect(db_path)
        init_db(conn)
        conn.close()

    # ---- 事务骨架：鉴权 → 即时事务 → 幂等重放/登记 → 提交 ----
    def _txn(self, op, body, role, need_role, fn):
        if role not in ROLES:
            raise ApiError(401, "unknown_role", f"缺少或未知角色：{role}")
        if need_role and role != "admin" and role != need_role:
            raise ApiError(403, "forbidden", f"操作 {op} 仅允许 {need_role} 执行")

        key = body.get("idem_key")
        conn = connect(self.db_path)
        try:
            # 立即拿写锁：并发锁定/发货在此排队串行，杜绝超卖
            conn.execute("BEGIN IMMEDIATE")

            if key is not None:
                row = conn.execute(
                    "SELECT op, response_json FROM idempotency WHERE idem_key=?", (key,)
                ).fetchone()
                if row is not None:
                    if row["op"] != op:
                        raise ApiError(
                            409, "idem_key_reused",
                            f"幂等键已用于其他操作（{row['op']}），不得跨操作复用",
                        )
                    result = json.loads(row["response_json"])
                    result["replayed"] = True
                    conn.commit()
                    return result

            result = fn(conn)

            # 测试用故障注入：业务写入全部完成后、提交前抛错 → 必须整体回滚
            if os.environ.get("MILL_FAIL_ON") == op:
                raise ApiError(500, "injected_failure", "注入故障：模拟提交前失败")

            if key is not None:
                conn.execute(
                    "INSERT INTO idempotency(idem_key, op, response_json, created_at) "
                    "VALUES(?,?,?,?)",
                    (key, op, json.dumps(result, ensure_ascii=False), now_iso()),
                )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---- 小工具 ----
    @staticmethod
    def _get(conn, table, ident, code):
        row = conn.execute(f"SELECT * FROM {table} WHERE id=? OR code=?", (ident, ident)).fetchone()
        if row is None:
            raise ApiError(404, code, f"找不到：{ident}")
        return row

    @staticmethod
    def _next_code(conn, prefix, table):
        n = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"] + 1
        while True:
            code = f"{prefix}-{n:04d}"
            if not conn.execute(f"SELECT 1 FROM {table} WHERE code=?", (code,)).fetchone():
                return code
            n += 1

    @staticmethod
    def _schedule_dict(conn, row):
        wo = conn.execute("SELECT code FROM work_orders WHERE id=?", (row["work_order_id"],)).fetchone()
        return {
            "id": row["id"], "work_order": wo["code"], "machine": row["machine"],
            "team": row["team"], "start": row["start_ts"], "end": row["end_ts"],
        }

    @staticmethod
    def _roll_dict(conn, row):
        wo = conn.execute("SELECT code FROM work_orders WHERE id=?", (row["work_order_id"],)).fetchone()
        alloc = conn.execute(
            "SELECT o.code FROM allocations a JOIN orders o ON o.id=a.order_id "
            "WHERE a.roll_id=? AND a.active=1", (row["id"],)
        ).fetchone()
        return {
            "id": row["id"], "roll_no": row["roll_no"], "work_order": wo["code"],
            "gsm": row["gsm"], "weight_kg": round(row["weight_g"] / 1000, 3),
            "grade": row["grade"], "status": row["status"],
            "locked_to": alloc["code"] if alloc else None,
            "created_at": row["created_at"],
        }

    @staticmethod
    def _order_status(row, locked_g):
        shipped = row["shipped_weight_g"]
        total = row["total_weight_g"]
        if shipped >= total:
            return "closed"
        if shipped > 0:
            return "partial_shipped"
        if locked_g > 0:
            return "locked"
        return "open"

    def _order_dict(self, conn, row):
        locked_g = conn.execute(
            "SELECT COALESCE(SUM(weight_g),0) AS s FROM allocations WHERE order_id=? AND active=1",
            (row["id"],)
        ).fetchone()["s"]
        return {
            "id": row["id"], "code": row["code"], "customer": row["customer"],
            "product_gsm": row["product_gsm"],
            "total_kg": round(row["total_weight_g"] / 1000, 3),
            "locked_kg": round(locked_g / 1000, 3),
            "shipped_kg": round(row["shipped_weight_g"] / 1000, 3),
            "available_to_lock_kg": round(
                max(0, row["total_weight_g"] - row["shipped_weight_g"] - locked_g) / 1000, 3),
            "status": self._order_status(row, locked_g),
            "created_at": row["created_at"],
        }

    # ================= 写操作 =================

    def create_work_order(self, body, role):
        gsm = require_gsm(body.get("product_gsm"), "product_gsm")
        planned_g = to_grams(body.get("planned_kg"), "planned_kg")

        def fn(conn):
            code = body.get("code") or self._next_code(conn, "WO", "work_orders")
            if conn.execute("SELECT 1 FROM work_orders WHERE code=?", (code,)).fetchone():
                raise ApiError(409, "work_order_exists", f"工单号已存在：{code}")
            cur = conn.execute(
                "INSERT INTO work_orders(code, product_gsm, planned_g, status, created_at) "
                "VALUES(?,?,?,?,?)",
                (code, gsm, planned_g, "created", now_iso()),
            )
            row = conn.execute("SELECT * FROM work_orders WHERE id=?", (cur.lastrowid,)).fetchone()
            return {"work_order": {"code": row["code"], "product_gsm": row["product_gsm"],
                                   "planned_kg": round(row["planned_g"] / 1000, 3),
                                   "status": row["status"]}}
        return self._txn("create_work_order", body, role, "planner", fn)

    def schedule_work_order(self, body, role):
        machine = body.get("machine")
        team = body.get("team")
        if not machine or not team:
            raise ApiError(400, "bad_schedule", "machine 与 team 必填")
        start = norm_ts(body.get("start"))
        end = norm_ts(body.get("end"))
        if parse_ts(end) <= parse_ts(start):
            raise ApiError(400, "bad_time", "结束时间必须晚于开始时间")

        def fn(conn):
            wo = self._get(conn, "work_orders", body.get("work_order"), "work_order_not_found")
            if wo["status"] != "created":
                raise ApiError(409, "already_scheduled", f"工单 {wo['code']} 已排产")
            # 半开区间重叠：同一纸槽 或 同一班组 任一冲突即拒绝
            clash = conn.execute(
                "SELECT machine, team, start_ts, end_ts FROM schedules "
                "WHERE ? < end_ts AND ? > start_ts AND (machine=? OR team=?)",
                (start, end, machine, team),
            ).fetchone()
            if clash:
                reason = "纸槽" if clash["machine"] == machine else "班组"
                raise ApiError(
                    409, "schedule_conflict",
                    f"排期冲突：{reason}在 {clash['start_ts']}~{clash['end_ts']} 已被占用",
                )
            cur = conn.execute(
                "INSERT INTO schedules(work_order_id, machine, team, start_ts, end_ts, created_at) "
                "VALUES(?,?,?,?,?,?)",
                (wo["id"], machine, team, start, end, now_iso()),
            )
            conn.execute("UPDATE work_orders SET status='scheduled' WHERE id=?", (wo["id"],))
            row = conn.execute("SELECT * FROM schedules WHERE id=?", (cur.lastrowid,)).fetchone()
            return {"schedule": self._schedule_dict(conn, row)}
        return self._txn("schedule_work_order", body, role, "planner", fn)

    def register_roll(self, body, role):
        roll_no = body.get("roll_no")
        if not roll_no:
            raise ApiError(400, "bad_roll", "roll_no 必填")
        weight_g = to_grams(body.get("weight_kg"), "weight_kg")

        def fn(conn):
            wo = self._get(conn, "work_orders", body.get("work_order"), "work_order_not_found")
            if wo["status"] != "scheduled":
                raise ApiError(409, "not_scheduled",
                               f"工单 {wo['code']} 尚未排产，不能登记成品")
            if conn.execute("SELECT 1 FROM rolls WHERE roll_no=?", (roll_no,)).fetchone():
                raise ApiError(409, "roll_no_exists", f"卷号重复：{roll_no}")
            gsm = body.get("gsm", wo["product_gsm"])
            gsm = require_gsm(gsm, "gsm")
            if gsm != wo["product_gsm"]:
                raise ApiError(409, "gsm_mismatch",
                               f"克重 {gsm} 与工单 {wo['code']} 计划克重 {wo['product_gsm']} 不符")
            cur = conn.execute(
                "INSERT INTO rolls(roll_no, work_order_id, gsm, weight_g, status, created_at) "
                "VALUES(?,?,?,?,?,?)",
                (roll_no, wo["id"], gsm, weight_g, "pending_qc", now_iso()),
            )
            row = conn.execute("SELECT * FROM rolls WHERE id=?", (cur.lastrowid,)).fetchone()
            return {"roll": self._roll_dict(conn, row)}
        return self._txn("register_roll", body, role, "worker", fn)

    def inspect_roll(self, body, role):
        """质检员判定等级：A/B 合格入在库，C 不合格。"""
        grade = body.get("grade")
        if grade not in ("A", "B", "C"):
            raise ApiError(400, "bad_grade", "grade 必须是 A / B / C")

        def fn(conn):
            row = conn.execute("SELECT * FROM rolls WHERE id=? OR roll_no=?",
                               (body.get("roll"), body.get("roll"))).fetchone()
            if row is None:
                raise ApiError(404, "roll_not_found", f"找不到卷：{body.get('roll')}")
            if row["status"] not in ("pending_qc", "qc_rejected"):
                raise ApiError(409, "roll_not_pending_qc",
                               f"卷 {row['roll_no']} 当前状态 {row['status']}，不可质检")
            qualified = grade in ("A", "B")
            new_status = "in_stock" if qualified else "qc_rejected"
            conn.execute("UPDATE rolls SET grade=?, status=? WHERE id=?",
                         (grade, new_status, row["id"]))
            out = conn.execute("SELECT * FROM rolls WHERE id=?", (row["id"],)).fetchone()
            return {"roll": self._roll_dict(conn, out),
                    "qualified": qualified}
        return self._txn("inspect_roll", body, role, "qc", fn)

    def regrade_roll(self, body, role):
        """改级同样只有质检员能做；在库卷降到 C 即退出可锁定池；锁定中的卷禁止改级。"""
        grade = body.get("grade")
        if grade not in ("A", "B", "C"):
            raise ApiError(400, "bad_grade", "grade 必须是 A / B / C")

        def fn(conn):
            row = conn.execute("SELECT * FROM rolls WHERE id=? OR roll_no=?",
                               (body.get("roll"), body.get("roll"))).fetchone()
            if row is None:
                raise ApiError(404, "roll_not_found", f"找不到卷：{body.get('roll')}")
            if row["status"] not in ("in_stock", "qc_rejected"):
                raise ApiError(409, "roll_not_regrable",
                               f"卷 {row['roll_no']} 当前状态 {row['status']}，不可改级")
            new_status = "in_stock" if grade in ("A", "B") else "qc_rejected"
            conn.execute("UPDATE rolls SET grade=?, status=? WHERE id=?",
                         (grade, new_status, row["id"]))
            out = conn.execute("SELECT * FROM rolls WHERE id=?", (row["id"],)).fetchone()
            return {"roll": self._roll_dict(conn, out)}
        return self._txn("regrade_roll", body, role, "qc", fn)

    def create_order(self, body, role):
        gsm = require_gsm(body.get("product_gsm"), "product_gsm")
        total_g = to_grams(body.get("total_kg"), "total_kg")
        customer = body.get("customer")
        if not customer:
            raise ApiError(400, "bad_order", "customer 必填")

        def fn(conn):
            code = body.get("code") or self._next_code(conn, "SO", "orders")
            if conn.execute("SELECT 1 FROM orders WHERE code=?", (code,)).fetchone():
                raise ApiError(409, "order_exists", f"订单号已存在：{code}")
            cur = conn.execute(
                "INSERT INTO orders(code, customer, product_gsm, total_weight_g, created_at) "
                "VALUES(?,?,?,?,?)",
                (code, customer, gsm, total_g, now_iso()),
            )
            row = conn.execute("SELECT * FROM orders WHERE id=?", (cur.lastrowid,)).fetchone()
            return {"order": self._order_dict(conn, row)}
        return self._txn("create_order", body, role, "dispatcher", fn)

    def lock_rolls(self, body, role):
        roll_ids = body.get("rolls")
        if not isinstance(roll_ids, list) or not roll_ids:
            raise ApiError(400, "bad_rolls", "rolls 必须是非空数组")

        def fn(conn):
            order = self._get(conn, "orders", body.get("order"), "order_not_found")
            # 1) 逐卷校验：存在、合格、在库、克重匹配、未被锁定
            rolls = []
            total_g = 0
            for ident in roll_ids:
                r = conn.execute("SELECT * FROM rolls WHERE id=? OR roll_no=?",
                                 (ident, ident)).fetchone()
                if r is None:
                    raise ApiError(404, "roll_not_found", f"找不到卷：{ident}")
                if r["status"] != "in_stock":
                    raise ApiError(409, "roll_not_available",
                                   f"卷 {r['roll_no']} 状态为 {r['status']}，不能锁定")
                if r["grade"] not in ("A", "B"):
                    raise ApiError(409, "roll_not_qualified", f"卷 {r['roll_no']} 质检不合格")
                if r["gsm"] != order["product_gsm"]:
                    raise ApiError(409, "gsm_mismatch",
                                   f"卷 {r['roll_no']} 克重 {r['gsm']} 与订单克重 "
                                   f"{order['product_gsm']} 不符")
                if conn.execute(
                    "SELECT 1 FROM allocations WHERE roll_id=? AND active=1", (r["id"],)
                ).fetchone():
                    raise ApiError(409, "roll_already_locked", f"卷 {r['roll_no']} 已被锁定")
                rolls.append(r)
                total_g += r["weight_g"]

            # 2) 订单余量校验（超发禁止）
            locked_g = conn.execute(
                "SELECT COALESCE(SUM(weight_g),0) AS s FROM allocations "
                "WHERE order_id=? AND active=1", (order["id"],)
            ).fetchone()["s"]
            remaining = order["total_weight_g"] - order["shipped_weight_g"] - locked_g
            if total_g > remaining:
                raise ApiError(
                    409, "order_oversold",
                    f"本次锁定 {round(total_g/1000,3)}kg 超过订单可锁余量 "
                    f"{round(remaining/1000,3)}kg",
                )

            # 3) 落库：锁批次 + 锁定关系 + 卷状态（同一事务）
            batch_code = self._next_code(conn, "LK", "lock_batches")
            cur = conn.execute(
                "INSERT INTO lock_batches(code, order_id, weight_g, idem_key, created_at) "
                "VALUES(?,?,?,?,?)",
                (batch_code, order["id"], total_g, body.get("idem_key"), now_iso()),
            )
            batch_id = cur.lastrowid
            for r in rolls:
                conn.execute(
                    "INSERT INTO allocations(roll_id, order_id, batch_id, weight_g, created_at) "
                    "VALUES(?,?,?,?,?)",
                    (r["id"], order["id"], batch_id, r["weight_g"], now_iso()),
                )
                # 守卫更新：只有仍在库的卷才会被置为锁定
                upd = conn.execute(
                    "UPDATE rolls SET status='locked' WHERE id=? AND status='in_stock'",
                    (r["id"],),
                )
                if upd.rowcount != 1:
                    raise ApiError(409, "roll_already_locked",
                                   f"卷 {r['roll_no']} 已被其他请求锁定")
            order_row = conn.execute("SELECT * FROM orders WHERE id=?", (order["id"],)).fetchone()
            return {"lock_batch": batch_code,
                    "rolls": [r["roll_no"] for r in rolls],
                    "locked_kg": round(total_g / 1000, 3),
                    "order": self._order_dict(conn, order_row)}
        return self._txn("lock_rolls", body, role, "dispatcher", fn)

    def ship_rolls(self, body, role):
        roll_ids = body.get("rolls")
        if not isinstance(roll_ids, list) or not roll_ids:
            raise ApiError(400, "bad_rolls", "rolls 必须是非空数组")

        def fn(conn):
            order = self._get(conn, "orders", body.get("order"), "order_not_found")
            # 1) 全部卷必须当前锁在该订单上（锁定后才能发货）
            rolls = []
            total_g = 0
            for ident in roll_ids:
                r = conn.execute("SELECT * FROM rolls WHERE id=? OR roll_no=?",
                                 (ident, ident)).fetchone()
                if r is None:
                    raise ApiError(404, "roll_not_found", f"找不到卷：{ident}")
                alloc = conn.execute(
                    "SELECT * FROM allocations WHERE roll_id=? AND order_id=? AND active=1",
                    (r["id"], order["id"]),
                ).fetchone()
                if alloc is None:
                    raise ApiError(409, "roll_not_locked",
                                   f"卷 {r['roll_no']} 未锁定到订单 {order['code']}，不能发货")
                rolls.append((r, alloc))
                total_g += r["weight_g"]

            # 2) 发货不得超过订单总量
            if order["shipped_weight_g"] + total_g > order["total_weight_g"]:
                raise ApiError(
                    409, "order_oversold",
                    f"本次发货 {round(total_g/1000,3)}kg 将超过订单总量 "
                    f"{round(order['total_weight_g']/1000,3)}kg",
                )

            # 3) 发货单 + 明细 + 释放锁定 + 卷已发货 + 订单累计（同一事务）
            ship_code = self._next_code(conn, "SH", "shipments")
            cur = conn.execute(
                "INSERT INTO shipments(code, order_id, shipped_weight_g, idem_key, created_at) "
                "VALUES(?,?,?,?,?)",
                (ship_code, order["id"], total_g, body.get("idem_key"), now_iso()),
            )
            ship_id = cur.lastrowid
            for r, alloc in rolls:
                conn.execute(
                    "INSERT INTO shipment_items(shipment_id, roll_id, weight_g) VALUES(?,?,?)",
                    (ship_id, r["id"], r["weight_g"]),
                )
                conn.execute(
                    "UPDATE allocations SET active=0, released_at=? WHERE id=?",
                    (now_iso(), alloc["id"]),
                )
                upd = conn.execute(
                    "UPDATE rolls SET status='shipped' WHERE id=? AND status='locked'",
                    (r["id"],),
                )
                if upd.rowcount != 1:
                    raise ApiError(409, "roll_not_locked",
                                   f"卷 {r['roll_no']} 已被其他请求处理")
            conn.execute(
                "UPDATE orders SET shipped_weight_g=shipped_weight_g+? WHERE id=?",
                (total_g, order["id"]),
            )
            order_row = conn.execute("SELECT * FROM orders WHERE id=?", (order["id"],)).fetchone()
            return {"shipment": ship_code, "order": order["code"],
                    "rolls": [r["roll_no"] for r, _ in rolls],
                    "shipped_kg": round(total_g / 1000, 3),
                    "order_summary": self._order_dict(conn, order_row)}
        return self._txn("ship_rolls", body, role, "dispatcher", fn)

    def return_rolls(self, body, role):
        """发货后退货：卷回在库，冲减订单已发重量，可再次锁定/发货。"""
        roll_ids = body.get("rolls")
        if not isinstance(roll_ids, list) or not roll_ids:
            raise ApiError(400, "bad_rolls", "rolls 必须是非空数组")
        reason = body.get("reason", "")

        def fn(conn):
            ship = self._get(conn, "shipments", body.get("shipment"), "shipment_not_found")
            returned_g = 0
            back_rolls = []
            for ident in roll_ids:
                item = conn.execute(
                    "SELECT si.*, r.roll_no, r.id AS rid FROM shipment_items si "
                    "JOIN rolls r ON r.id=si.roll_id "
                    "WHERE shipment_id=? AND (r.id=? OR r.roll_no=?)",
                    (ship["id"], ident, ident),
                ).fetchone()
                if item is None:
                    raise ApiError(404, "not_in_shipment", f"发货单 {ship['code']} 中没有卷：{ident}")
                if item["returned_at"] is not None:
                    raise ApiError(409, "roll_already_returned",
                                   f"卷 {item['roll_no']} 已退货，不能重复退")
                conn.execute(
                    "UPDATE shipment_items SET returned_at=? WHERE id=?", (now_iso(), item["id"])
                )
                upd = conn.execute(
                    "UPDATE rolls SET status='in_stock' WHERE id=? AND status='shipped'",
                    (item["rid"],),
                )
                if upd.rowcount != 1:
                    raise ApiError(409, "roll_not_shipped",
                                   f"卷 {item['roll_no']} 状态异常，无法退货回库")
                returned_g += item["weight_g"]
                back_rolls.append(item["roll_no"])

            ret_code = self._next_code(conn, "RT", "returns")
            conn.execute(
                "INSERT INTO returns(code, shipment_id, weight_g, reason, idem_key, created_at) "
                "VALUES(?,?,?,?,?,?)",
                (ret_code, ship["id"], returned_g, reason, body.get("idem_key"), now_iso()),
            )
            conn.execute(
                "UPDATE shipments SET returned_weight_g=returned_weight_g+? WHERE id=?",
                (returned_g, ship["id"]),
            )
            # CHECK 约束保证不会冲成负数
            conn.execute(
                "UPDATE orders SET shipped_weight_g=shipped_weight_g-? WHERE id=?",
                (returned_g, ship["order_id"]),
            )
            order_row = conn.execute("SELECT * FROM orders WHERE id=?", (ship["order_id"],)).fetchone()
            return {"return_note": ret_code, "shipment": ship["code"],
                    "rolls": back_rolls, "returned_kg": round(returned_g / 1000, 3),
                    "order_summary": self._order_dict(conn, order_row)}
        return self._txn("return_rolls", body, role, "warehouse", fn)

    # ================= 读操作 =================

    def snapshot(self):
        conn = connect(self.db_path)
        try:
            return {
                "work_orders": [
                    {**dict(r), "planned_kg": round(r["planned_g"] / 1000, 3)}
                    for r in conn.execute("SELECT * FROM work_orders ORDER BY id")
                ],
                "schedules": [
                    self._schedule_dict(conn, r)
                    for r in conn.execute("SELECT * FROM schedules ORDER BY id")
                ],
                "rolls": [
                    self._roll_dict(conn, r)
                    for r in conn.execute("SELECT * FROM rolls ORDER BY id")
                ],
                "orders": [
                    self._order_dict(conn, r)
                    for r in conn.execute("SELECT * FROM orders ORDER BY id")
                ],
                "shipments": [
                    self._shipment_dict(conn, r)
                    for r in conn.execute("SELECT * FROM shipments ORDER BY id")
                ],
                "returns": [dict(r) for r in conn.execute("SELECT * FROM returns ORDER BY id")],
            }
        finally:
            conn.close()

    @staticmethod
    def _shipment_dict(conn, row):
        items = []
        for it in conn.execute(
            "SELECT r.roll_no, si.weight_g, si.returned_at FROM shipment_items si "
            "JOIN rolls r ON r.id=si.roll_id WHERE si.shipment_id=? ORDER BY si.id",
            (row["id"],),
        ):
            items.append({"roll_no": it["roll_no"], "weight_kg": round(it["weight_g"] / 1000, 3),
                          "returned": it["returned_at"] is not None})
        return {"code": row["code"], "items": items,
                "shipped_kg": round(row["shipped_weight_g"] / 1000, 3),
                "returned_kg": round(row["returned_weight_g"] / 1000, 3),
                "created_at": row["created_at"]}


# ---------------------------------------------------------------- HTTP 层

ROUTES = {
    "/api/work-orders": ("POST", "create_work_order", "planner"),
    "/api/schedules": ("POST", "schedule_work_order", "planner"),
    "/api/rolls": ("POST", "register_roll", "worker"),
    "/api/rolls/inspect": ("POST", "inspect_roll", "qc"),
    "/api/rolls/regrade": ("POST", "regrade_roll", "qc"),
    "/api/orders": ("POST", "create_order", "dispatcher"),
    "/api/locks": ("POST", "lock_rolls", "dispatcher"),
    "/api/shipments": ("POST", "ship_rolls", "dispatcher"),
    "/api/returns": ("POST", "return_rolls", "warehouse"),
}


class Handler(BaseHTTPRequestHandler):
    service = None

    def log_message(self, fmt, *args):  # 静默默认访问日志
        pass

    def _send(self, status, payload):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/healthz":
            return self._send(200, {"ok": True})
        if path == "/api/state":
            return self._send(200, self.service.snapshot())
        self._send(404, {"error": "not_found"})

    def do_POST(self):
        path = urlparse(self.path).path
        route = ROUTES.get(path)
        if not route:
            return self._send(404, {"error": "not_found"})
        _method, method_name, need_role = route
        try:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw.decode("utf-8")) if raw else {}
            except json.JSONDecodeError:
                raise ApiError(400, "bad_json", "请求体不是合法 JSON")
            if not isinstance(body, dict):
                raise ApiError(400, "bad_body", "请求体必须是 JSON 对象")
            # 角色与幂等键同时支持 header 透传
            body.setdefault("role", self.headers.get("X-Role", ""))
            if "idem_key" not in body and self.headers.get("Idempotency-Key"):
                body["idem_key"] = self.headers["Idempotency-Key"]
            # 鉴权优先于一切业务参数校验（服务层 _txn 内另有同样的纵深校验）
            role = body.get("role") or ""
            if role not in ROLES:
                raise ApiError(401, "unknown_role", f"缺少或未知角色：{role}")
            if role != "admin" and role != need_role:
                raise ApiError(403, "forbidden",
                               f"该操作仅允许 {need_role} 执行，当前角色 {role}")
            result = getattr(self.service, method_name)(body, role)
            self._send(200, result)
        except ApiError as e:
            self._send(e.status, {"error": e.code, "message": e.message})
        except sqlite3.IntegrityError as e:
            self._send(409, {"error": "integrity_constraint", "message": str(e)})
        except Exception as e:  # noqa: BLE001
            self._send(500, {"error": "internal_error", "message": str(e)})


def serve(db_path=DB_PATH, port=None):
    service = MillService(db_path)
    Handler.service = service
    port = port or int(os.environ.get("MILL_PORT", "8039"))
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    return httpd


if __name__ == "__main__":
    httpd = serve()
    print(f"纸坊抄纸排产与成品发货系统 listening on http://127.0.0.1:{httpd.server_address[1]}")
    print(f"数据库：{DB_PATH}")
    httpd.serve_forever()
