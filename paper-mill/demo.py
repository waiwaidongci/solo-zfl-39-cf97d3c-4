#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
纸坊系统 本地端到端验证

覆盖：
  主线：排产 → 成品登记 → 质检入库 → 订单锁定 → 分批发货 → 退货回库
  验证：排期冲突、重复提交（幂等重放）、越权改级、并发超卖、
        失败回滚（注入故障）、重启恢复（杀进程重开 + 幂等键跨进程有效）

用法：python3 demo.py
"""

import json
import os
import atexit
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(HERE, "data-demo")
DB_PATH = os.path.join(DB_DIR, "mill.db")

# 任何退出路径（含断言失败）都回收子服务，避免孤儿进程
_CHILDREN = []
atexit.register(lambda: [wait_exit(p) for p in list(_CHILDREN) if p.poll() is None])

PASS = "通过"
FAIL = "失败"
results = []
_lock = threading.Lock()


def check(name, cond, detail=""):
    with _lock:
        results.append((name, bool(cond), detail))
    mark = "✅" if cond else "❌"
    print(f"  {mark} {name}" + (f" —— {detail}" if detail and not cond else ""))


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Client:
    def __init__(self, port):
        self.port = port

    def call(self, path, payload, role, key=None, expect=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json", "X-Role": role}
        if key is not None:
            headers["Idempotency-Key"] = key
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                out = json.loads(resp.read().decode("utf-8"))
                code = resp.status
        except urllib.error.HTTPError as e:
            out = json.loads(e.read().decode("utf-8"))
            code = e.code
        if expect is not None and code != expect:
            raise AssertionError(f"{path}: 期望 HTTP {expect}，实际 {code}，响应 {out}")
        return code, out

    def state(self):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/api/state", timeout=10) as r:
            return json.loads(r.read().decode("utf-8"))


def start_server(port):
    env = dict(os.environ, MILL_DB=DB_PATH, MILL_PORT=str(port))
    proc = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "app.py")],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1) as r:
                if r.status == 200:
                    _CHILDREN.append(proc)
                    return proc
        except Exception:
            time.sleep(0.15)
    proc.kill()
    raise RuntimeError("服务启动超时")


def wait_exit(proc, timeout=10):
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# =====================================================================

def section(title):
    print(f"\n{'='*64}\n{title}\n{'='*64}")


def main():
    if os.path.exists(DB_DIR):
        shutil.rmtree(DB_DIR)

    port = free_port()
    print("启动服务进程（真实子进程）……")
    proc = start_server(port)
    c = Client(port)

    # ------------------------------------------------ 一、主线全流程
    section("一、主线：排产 → 登记 → 质检 → 锁定 → 分批发货 → 退货回库")

    code, wo1 = c.call("/api/work-orders",
                       {"code": "WO-1", "product_gsm": 80, "planned_kg": 1000},
                       role="planner", expect=200)
    code, wo2 = c.call("/api/work-orders",
                       {"code": "WO-2", "product_gsm": 100, "planned_kg": 500},
                       role="planner", expect=200)
    check("建立工单", wo1["work_order"]["code"] == "WO-1")

    # 1) 排产
    code, sch1 = c.call("/api/schedules",
                        {"work_order": "WO-1", "machine": "一号槽", "team": "甲班",
                         "start": "2026-09-15T08:00", "end": "2026-09-15T16:00"},
                        role="planner", expect=200)
    check("WO-1 排产（一号槽/甲班）", sch1["schedule"]["machine"] == "一号槽")
    code, sch2 = c.call("/api/schedules",
                        {"work_order": "WO-2", "machine": "二号槽", "team": "乙班",
                         "start": "2026-09-15T08:00", "end": "2026-09-15T12:00"},
                        role="planner", expect=200)
    check("WO-2 排产（二号槽/乙班，槽与班均不撞）", sch2["schedule"]["machine"] == "二号槽")

    # 2) 登记成品：排产后才能登记
    code, r1 = c.call("/api/rolls",
                      {"work_order": "WO-1", "roll_no": "R-001", "gsm": 80, "weight_kg": 200},
                      role="worker", expect=200)
    code, r2 = c.call("/api/rolls",
                      {"work_order": "WO-1", "roll_no": "R-002", "gsm": 80, "weight_kg": 180},
                      role="worker", expect=200)
    code, r3 = c.call("/api/rolls",
                      {"work_order": "WO-1", "roll_no": "R-003", "gsm": 80, "weight_kg": 150},
                      role="worker", expect=200)
    check("登记三卷成品 R-001/002/003",
          r1["roll"]["status"] == "pending_qc" and r3["roll"]["weight_kg"] == 150)

    # 3) 质检入库：A/B 合格，C 不合格
    code, q1 = c.call("/api/rolls/inspect", {"roll": "R-001", "grade": "A"},
                      role="qc", expect=200)
    c.call("/api/rolls/inspect", {"roll": "R-002", "grade": "B"}, role="qc", expect=200)
    code, q3 = c.call("/api/rolls/inspect", {"roll": "R-003", "grade": "C"},
                      role="qc", expect=200)
    check("R-001(A)、R-002(B) 合格在库；R-003(C) 判废",
          q1["roll"]["status"] == "in_stock" and q3["qualified"] is False
          and q3["roll"]["status"] == "qc_rejected")

    # 4) 订单锁定：合格卷 + 在库 + 克重匹配 + 不超订单量
    code, so = c.call("/api/orders",
                      {"code": "SO-1", "customer": "荣宝斋", "product_gsm": 80, "total_kg": 500},
                      role="dispatcher", expect=200)
    code, lk = c.call("/api/locks", {"order": "SO-1", "rolls": ["R-001", "R-002"]},
                      role="dispatcher", key="K-LOCK-1", expect=200)
    check("锁定 R-001+R-002 到 SO-1（380kg ≤ 500kg）",
          lk["locked_kg"] == 380 and set(lk["rolls"]) == {"R-001", "R-002"})

    # 5) 分批发货：先发 R-001，再发 R-002
    code, sh1 = c.call("/api/shipments", {"order": "SO-1", "rolls": ["R-001"]},
                       role="dispatcher", key="K-SHIP-1", expect=200)
    check("第一批发货 R-001（200kg）",
          sh1["shipment"].startswith("SH-") and sh1["shipped_kg"] == 200)
    code, sh2 = c.call("/api/shipments", {"order": "SO-1", "rolls": ["R-002"]},
                       role="dispatcher", key="K-SHIP-2", expect=200)
    check("第二批发货 R-002（180kg），累计 380kg",
          sh2["order_summary"]["shipped_kg"] == 380
          and sh2["order_summary"]["status"] == "partial_shipped")

    # 6) 退货回库：R-001 退回，回在库、冲减已发
    code, rt = c.call("/api/returns",
                      {"shipment": sh1["shipment"], "rolls": ["R-001"], "reason": "边缘破损"},
                      role="warehouse", key="K-RET-1", expect=200)
    st = c.state()
    r001 = next(r for r in st["rolls"] if r["roll_no"] == "R-001")
    so1 = next(o for o in st["orders"] if o["code"] == "SO-1")
    check("R-001 退货回在库，SO-1 已发冲减为 180kg",
          r001["status"] == "in_stock" and r001["locked_to"] is None
          and so1["shipped_kg"] == 180)

    # ------------------------------------------------ 二、排期冲突
    section("二、排期冲突验证")

    code, wo3 = c.call("/api/work-orders",
                       {"code": "WO-3", "product_gsm": 80, "planned_kg": 300},
                       role="planner", expect=200)
    # 同纸槽重叠
    code, e1 = c.call("/api/schedules",
                      {"work_order": "WO-3", "machine": "一号槽", "team": "丙班",
                       "start": "2026-09-15T10:00", "end": "2026-09-15T14:00"},
                      role="planner", expect=409)
    check("同纸槽时段重叠被拒（409 schedule_conflict）",
          e1["error"] == "schedule_conflict")

    # 同班组重叠（换槽也不行）
    code, e2 = c.call("/api/schedules",
                      {"work_order": "WO-3", "machine": "三号槽", "team": "甲班",
                       "start": "2026-09-15T09:00", "end": "2026-09-15T11:00"},
                      role="planner", expect=409)
    check("同班组时段重叠被拒（即使换纸槽）", e2["error"] == "schedule_conflict")

    # 首尾相接（半开区间）允许：新排期 16:00 开始
    code, ok = c.call("/api/schedules",
                      {"work_order": "WO-3", "machine": "一号槽", "team": "丙班",
                       "start": "2026-09-15T16:00", "end": "2026-09-15T20:00"},
                      role="planner", expect=200)
    check("首尾相接（16:00 接 16:00）不视为冲突", ok["schedule"]["team"] == "丙班")

    # 工单重复排产
    code, e3 = c.call("/api/schedules",
                      {"work_order": "WO-1", "machine": "九号槽", "team": "丁班",
                       "start": "2026-09-16T08:00", "end": "2026-09-16T12:00"},
                      role="planner", expect=409)
    check("工单重复排产被拒", e3["error"] == "already_scheduled")

    # ------------------------------------------------ 三、重复提交（幂等）
    section("三、重复提交：重放原结果，键不得跨操作复用")

    # 同键重放锁定 K-LOCK-1 → 原结果 + replayed
    code, replay = c.call("/api/locks", {"order": "SO-1", "rolls": ["R-001", "R-002"]},
                          role="dispatcher", key="K-LOCK-1", expect=200)
    check("同键重放返回原锁定结果（replayed=true）",
          replay.get("replayed") is True and replay["lock_batch"] == lk["lock_batch"])
    st = c.state()
    check("重放不产生重复锁定、不改变账（两卷均已发货，生效锁定为 0）",
          next(o for o in st["orders"] if o["code"] == "SO-1")["locked_kg"] == 0)

    # 同键用于另一操作 → 409
    code, cross = c.call("/api/shipments", {"order": "SO-1", "rolls": ["R-001"]},
                         role="dispatcher", key="K-LOCK-1", expect=409)
    check("幂等键跨操作复用被拒", cross["error"] == "idem_key_reused")

    # 不同请求体同键仍返回第一次结果（以键为准，而非参数）
    code, replay2 = c.call("/api/shipments", {"order": "SO-1", "rolls": ["R-999"]},
                           role="dispatcher", key="K-SHIP-1", expect=200)
    check("同键不同参数仍重放首次发货结果",
          replay2.get("replayed") is True and replay2["rolls"] == ["R-001"])

    # ------------------------------------------------ 四、越权改级
    section("四、越权改级 / 跳步 / 非法值")

    # 非质检员改级
    code, p1 = c.call("/api/rolls/regrade", {"roll": "R-001", "grade": "A"},
                      role="dispatcher", expect=403)
    code, p2 = c.call("/api/rolls/inspect", {"roll": "R-001", "grade": "A"},
                      role="worker", expect=403)
    check("发运员改级、抄纸工质检均被拒（403 forbidden）",
          p1["error"] == "forbidden" and p2["error"] == "forbidden")
    code, p1b = c.call("/api/schedules", {"work_order": "WO-1"},
                       role="worker", expect=403)
    check("鉴权先于参数校验：越权即使请求残缺也返回 403（而非 400）",
          p1b["error"] == "forbidden")
    code, p3 = c.call("/api/rolls/inspect", {"roll": "R-001", "grade": "A"},
                      role="spy", expect=401)
    check("未知角色拒绝（401）", p3["error"] == "unknown_role")

    # 未排产登记成品
    code, wo4 = c.call("/api/work-orders",
                       {"code": "WO-4", "product_gsm": 80, "planned_kg": 100},
                       role="planner", expect=200)
    code, g1 = c.call("/api/rolls",
                      {"work_order": "WO-4", "roll_no": "R-100", "gsm": 80, "weight_kg": 50},
                      role="worker", expect=409)
    check("未排产工单不能登记成品", g1["error"] == "not_scheduled")

    # 不合格卷 / 克重不符 / 重复卷号 / 负重量
    code, g2 = c.call("/api/locks", {"order": "SO-1", "rolls": ["R-003"]},
                      role="dispatcher", expect=409)
    check("质检不合格卷(C)不能锁定", g2["error"] == "roll_not_available")
    code, g3 = c.call("/api/locks", {"order": "SO-1", "rolls": ["R-001"]},
                      role="dispatcher", key="K-LOCK-X", expect=200)
    check("R-001 退货回库后可再次锁定到 SO-1",
          g3["rolls"] == ["R-001"] and g3["order"]["locked_kg"] == 200)
    code, g4 = c.call("/api/locks", {"order": "SO-1", "rolls": ["R-001"]},
                      role="dispatcher", key="K-LOCK-Y", expect=409)
    check("重复锁定同一卷被拒", g4["error"] in ("roll_already_locked", "roll_not_available"))

    # 另备一卷在库 80g 卷，用于克重不符与未锁定发货的测试
    code, g8 = c.call("/api/rolls",
                      {"work_order": "WO-3", "roll_no": "R-300", "gsm": 80, "weight_kg": 60},
                      role="worker", key="K-REG-300", expect=200)
    c.call("/api/rolls/inspect", {"roll": "R-300", "grade": "A"}, role="qc", expect=200)

    code, so2 = c.call("/api/orders",
                       {"code": "SO-2", "customer": "朵云轩", "product_gsm": 100, "total_kg": 500},
                       role="dispatcher", expect=200)
    code, g5 = c.call("/api/locks", {"order": "SO-2", "rolls": ["R-300"]},
                      role="dispatcher", key="K-LOCK-Z", expect=409)
    check("克重与订单不符不能锁定", g5["error"] == "gsm_mismatch")
    code, g6 = c.call("/api/rolls",
                      {"work_order": "WO-1", "roll_no": "R-200", "gsm": 80, "weight_kg": -5},
                      role="worker", expect=400)
    code, g7 = c.call("/api/rolls",
                      {"work_order": "WO-1", "roll_no": "R-201", "gsm": 80, "weight_kg": 0},
                      role="worker", expect=400)
    check("负重量 / 零重量被拒（negative_weight）",
          g6["error"] == "negative_weight" and g7["error"] == "negative_weight")

    # 未锁定直接发货（R-300 仍在库）
    code, g9 = c.call("/api/shipments", {"order": "SO-1", "rolls": ["R-300"]},
                      role="dispatcher", key="K-SHIP-BAD", expect=409)
    check("未锁定的卷不能发货", g9["error"] == "roll_not_locked")

    # ------------------------------------------------ 五、并发超卖
    section("五、并发锁定 / 并发发货：不得超卖")

    # 准备：SO-3 克重100、总量 300kg；6 卷各 100kg 合格在库
    c.call("/api/orders", {"code": "SO-3", "customer": "西泠印社",
                           "product_gsm": 100, "total_kg": 300},
           role="dispatcher", expect=200)
    roll_ids = []
    for i in range(1, 7):
        no = f"C-{i:03d}"
        c.call("/api/rolls", {"work_order": "WO-2", "roll_no": no,
                              "gsm": 100, "weight_kg": 100},
               role="worker", expect=200)
        c.call("/api/rolls/inspect", {"roll": no, "grade": "A"}, role="qc", expect=200)
        roll_ids.append(no)

    # 10 个线程并发抢锁：每线程一卷，卷集合有重复；订单只有 300kg
    groups = [
        "C-001", "C-002", "C-003", "C-004", "C-005",
        "C-001", "C-002", "C-003", "C-004", "C-006",
    ]

    def lock_one(idx):
        return c.call("/api/locks", {"order": "SO-3", "rolls": [groups[idx]]},
                      role="dispatcher", key=f"K-RACE-L{idx}")

    with ThreadPoolExecutor(max_workers=10) as ex:
        lock_results = list(ex.map(lock_one, range(10)))
    ok_locks = [r for code, r in lock_results if code == 200 and not r.get("replayed")]
    bad_locks = [(code, r) for code, r in lock_results if code != 200]
    locked_total = sum(r["locked_kg"] for r in ok_locks)
    check("10 线程并发锁定：成功者锁定总量恰好 300kg，无超卖",
          locked_total == 300, f"实际成功锁定 {locked_total}kg")
    check("其余请求全部明确失败（409：卷已被抢 / 订单超卖），无半笔账",
          all(code == 409 for code, _ in bad_locks) and len(ok_locks) + len(bad_locks) == 10,
          f"成功 {len(ok_locks)} 笔，失败 {len(bad_locks)} 笔")
    so3 = next(o for o in c.state()["orders"] if o["code"] == "SO-3")
    check("SO-3 账本：locked 300kg、可锁余量 0",
          so3["locked_kg"] == 300 and so3["available_to_lock_kg"] == 0)

    # 同一卷并发、同一幂等键：只能成一笔（用未参与争抢的 C-007）
    c.call("/api/orders", {"code": "SO-4", "customer": "故宫文化",
                           "product_gsm": 100, "total_kg": 1000},
           role="dispatcher", expect=200)
    c.call("/api/rolls", {"work_order": "WO-2", "roll_no": "C-007",
                          "gsm": 100, "weight_kg": 100},
           role="worker", key="K-REG-C7", expect=200)
    c.call("/api/rolls/inspect", {"roll": "C-007", "grade": "A"},
           role="qc", key="K-QC-C7", expect=200)

    def dup_lock(i):
        return c.call("/api/locks", {"order": "SO-4", "rolls": ["C-007"]},
                      role="dispatcher", key="K-DUP-SAME")

    with ThreadPoolExecutor(max_workers=6) as ex:
        dup_res = list(ex.map(dup_lock, range(6)))
    firsts = [r for code, r in dup_res if code == 200 and not r.get("replayed")]
    replays = [r for code, r in dup_res if code == 200 and r.get("replayed")]
    check("同卷同键并发 6 次：恰好 1 笔成功，其余全部重放原结果",
          len(firsts) == 1 and len(replays) == 5)

    # 并发发货超卖：先把 R-001 状态备好（当前锁在 SO-1），多线程抢发同一卷
    def dup_ship(i):
        return c.call("/api/shipments", {"order": "SO-1", "rolls": ["R-001"]},
                      role="dispatcher", key=f"K-RACE-SHIP{i}")

    with ThreadPoolExecutor(max_workers=8) as ex:
        ship_res = list(ex.map(dup_ship, range(8)))
    ship_ok = [(code, r) for code, r in ship_res if code == 200 and not r.get("replayed")]
    ship_bad = [(code, r) for code, r in ship_res if code != 200]
    check("同一卷并发发货 8 次：恰好 1 次成功，其余 409（不超卖、不重复发）",
          len(ship_ok) == 1 and len(ship_bad) == 7
          and all(code == 409 for code, _ in ship_bad),
          f"成功 {len(ship_ok)}，失败 {len(ship_bad)}")

    # 发货总量护栏：用小订单 SO-5（总量 50kg）锁 60kg 的 R-300 → 409
    c.call("/api/orders", {"code": "SO-5", "customer": "私坊",
                           "product_gsm": 80, "total_kg": 50},
           role="dispatcher", expect=200)
    code, ov = c.call("/api/locks", {"order": "SO-5", "rolls": ["R-300"]},
                      role="dispatcher", key="K-OV-LOCK", expect=409)
    check("单卷 60kg 锁进 50kg 订单：超发被拒", ov["error"] == "order_oversold")

    # ------------------------------------------------ 六、失败回滚
    section("六、失败回滚：注入故障不留半笔账")

    # 进程外：带 MILL_FAIL_ON=lock_rolls 起一个新服务实例（同库）
    port2 = free_port()
    env = dict(os.environ, MILL_DB=DB_PATH, MILL_PORT=str(port2), MILL_FAIL_ON="lock_rolls")
    bad_proc = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "app.py")],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _CHILDREN.append(bad_proc)
    try:
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port2}/healthz", timeout=1):
                    break
            except Exception:
                time.sleep(0.15)
        # 准备一卷在库（经主服务），再让故障实例去锁
        c.call("/api/rolls", {"work_order": "WO-3", "roll_no": "R-301",
                              "gsm": 80, "weight_kg": 40},
               role="worker", key="K-REG-301", expect=200)
        c.call("/api/rolls/inspect", {"roll": "R-301", "grade": "A"},
               role="qc", key="K-QC-301", expect=200)
        code, injected = Client(port2).call(
            "/api/locks", {"order": "SO-1", "rolls": ["R-301"]},
            role="dispatcher", key="K-INJECT", expect=500)
        check("注入故障：锁定请求返回 500 injected_failure",
              injected["error"] == "injected_failure")
    finally:
        bad_proc.terminate()
        wait_exit(bad_proc)

    st = c.state()
    r301 = next(r for r in st["rolls"] if r["roll_no"] == "R-301")
    check("故障后卷仍为在库、未被锁定（状态机回滚）",
          r301["status"] == "in_stock" and r301["locked_to"] is None)
    # 幂等键也随事务回滚 → 可被正常服务重新使用且成功
    code, reuse = c.call("/api/locks", {"order": "SO-1", "rolls": ["R-301"]},
                         role="dispatcher", key="K-INJECT", expect=200)
    check("失败请求的幂等键未被占用，重提成功（无幂等脏记录）",
          reuse["rolls"] == ["R-301"] and not reuse.get("replayed"))
    st = c.state()
    # R-001 已在并发发货测试中被成功发出（锁定释放），故生效锁定只剩 R-301 40kg
    check("无锁批次/锁定关系残留（SO-1 生效锁定仅 R-301 40kg）",
          next(o for o in st["orders"] if o["code"] == "SO-1")["locked_kg"] == 40)

    # ------------------------------------------------ 七、重启恢复
    section("七、重启恢复：杀进程 → 重开 → 数据与幂等键仍在")

    before = c.state()
    proc.terminate()
    wait_exit(proc)

    port3 = free_port()
    proc = start_server(port3)  # 同一 DB_PATH，新端口新进程
    c = Client(port3)
    after = c.state()

    check("重启后全部业务数据不丢（工单/排期/卷/订单/发货/退货一致）",
          json.dumps(before, sort_keys=True) == json.dumps(after, sort_keys=True))

    # 幂等键跨进程仍然有效
    code, rplay = c.call("/api/shipments", {"order": "SO-1", "rolls": ["R-001"]},
                         role="dispatcher", key="K-SHIP-1", expect=200)
    check("重启后旧幂等键仍重放原结果（不重复发货）",
          rplay.get("replayed") is True and rplay["rolls"] == ["R-001"])

    # 重启后业务可继续：把 R-301 发出，验证全链路在新进程上畅通
    code, cont = c.call("/api/shipments", {"order": "SO-1", "rolls": ["R-301"]},
                        role="dispatcher", key="K-SHIP-CONT", expect=200)
    check("重启后可继续发货（R-301 40kg）",
          cont["shipped_kg"] == 40)

    proc.terminate()
    wait_exit(proc)

    # ------------------------------------------------ 汇总
    section("汇总")
    total_n = len(results)
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"  {passed}/{total_n} 项断言通过")
    if passed != total_n:
        print("\n失败项：")
        for name, ok, detail in results:
            if not ok:
                print(f"   - {name} {detail}")
        sys.exit(1)
    print("\n🎉 全部验证通过：排产、登记、质检、锁定、分批发货、退货回库主线畅通，")
    print("   排期冲突 / 重复提交 / 越权改级 / 并发超卖 / 失败回滚 / 重启恢复 均符合要求。")


if __name__ == "__main__":
    main()
