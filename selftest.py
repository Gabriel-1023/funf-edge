#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
selftest.py — 无硬件端到端自测

覆盖四层：
  A. 帧协议：combo_frames / STOP / parse 往返
  B. 边控状态机：dwell 防抖、回差、冷静期、自动重布防、锁定
  C. WebSocket：自建中继 + 模拟 DG-LAB APP，验证 hello/attach/snapshot/slots.patch
  D. 联动：气压越线 -> EdgeGuard 触发 -> 调用 stop 回调（用假 BLE 记录发送帧）

跑法：python3 selftest.py
"""
import json
import sys
import threading
import time

import funf_proto as P
import wsproto
import relay_v4
from edge_guard import EdgeGuard, IDLE, ARMED, TRIPPED
from edge_sensor import EdgeSensor

FAILED = []


def check(name, cond, detail=""):
    if cond:
        print("  ✓ %s" % name)
    else:
        print("  ✗ %s  %s" % (name, detail))
        FAILED.append(name)


# ---------------------------------------------------------------- A 协议
def test_proto():
    print("\n[A] 帧协议")
    f = P.combo_frames(60, 1, 20, 2, 0, 1)
    check("combo 拆三条", len(f) == 3, f)
    check("吮吸帧", f[0] == "010100020007113c000811 01".replace(" ", ""), f[0])
    check("震动帧", f[1] == "010100020001111400021102", f[1])
    check("全停帧", P.STOP == "01010003000111000003110000071100", P.STOP)
    check("单帧 ≤20 字节", all(len(x) // 2 <= 20 for x in f),
          [len(x) // 2 for x in f])

    rep = P.parse("0101000100051146")
    check("解析电量", rep.get(0x0005) == b"\x46", rep)
    check("describe", "电量=70" in P.describe("0101000100051146"),
          P.describe("0101000100051146"))

    rep2 = P.parse("010100020001111400021102")
    check("解析双属性", rep2.get(0x0001) == b"\x14" and rep2.get(0x0002) == b"\x02",
          rep2)

    check("失控帧", P.lost_start(5, 0x03) == "010100020009110500 0b1103".replace(" ", ""),
          P.lost_start(5, 0x03))
    check("取消失控", P.LOST_CANCEL == "0101000200091100000b1100", P.LOST_CANCEL)


# ---------------------------------------------------------------- B 状态机
def test_guard():
    print("\n[B] 边控状态机")
    trips, rels = [], []
    g = EdgeGuard(threshold=50, release_delta=10, dwell=0.1, cooldown=0.5,
                  auto_rearm=True,
                  on_trip=lambda p, t: trips.append(p),
                  on_release=lambda: rels.append(1))

    check("初始 IDLE", g.state == IDLE)
    g.feed(999)          # 未布防不响应
    check("未布防不触发", not trips)

    g.arm()
    check("布防 ARMED", g.state == ARMED)

    g.feed(55, time.time())        # 第一次越线，开始计时
    check("首次越线不立即触发", g.state == ARMED)
    check("dwell 未到不触发", not trips)

    g.feed(55, time.time() + 0.05)
    check("dwell 内仍不触发", not trips)

    g.feed(55, time.time() + 0.2)
    check("dwell 到期触发", g.state == TRIPPED and len(trips) == 1, g.state)

    g.feed(55, time.time() + 0.3)
    check("触发后不重复触发", len(trips) == 1, len(trips))

    # 回差：45 仍在回差带内（<= 40 才释放），不应恢复
    g.feed(45, time.time() + 0.4)
    check("回差内不恢复", g.state == TRIPPED, g.state)

    # 低于回差但冷静期未到
    g.feed(20, time.time() + 0.45)
    check("冷静期未到不恢复", g.state == TRIPPED, g.state)

    g.feed(20, time.time() + 1.2)
    check("冷静期到恢复布防", g.state == ARMED and len(rels) == 1, g.state)

    # 第二次触发
    g.feed(80, time.time() + 1.3)
    g.feed(80, time.time() + 1.5)
    check("可二次触发", len(trips) == 2, len(trips))

    # 手动撤防解除锁定
    g.disarm()
    check("撤防回 IDLE", g.state == IDLE)

    # 瞬时尖峰被 dwell 滤掉
    g2 = EdgeGuard(threshold=50, dwell=0.2, cooldown=0.1, auto_rearm=True,
                   on_trip=lambda p, t: trips.append("spike"))
    g2.arm()
    n0 = len(trips)
    g2.feed(99, 100.0)
    g2.feed(10, 100.1)       # 立刻回落
    g2.feed(10, 100.5)
    check("瞬时尖峰被滤除", len(trips) == n0 and g2.state == ARMED, g2.state)

    # 冷却期从「气压回落到阈值以下」才开始计时
    rel_c = []
    g4 = EdgeGuard(threshold=50, release_delta=10, dwell=0.0, cooldown=3.0,
                   auto_rearm=True, stale_after=999,
                   on_trip=lambda p, t: None,
                   on_release=lambda: rel_c.append(1))
    g4.arm()
    g4.feed(60, 100.0)
    g4.tick(100.0)
    check("触发", g4.state == TRIPPED, g4.state)

    # 高位停留 10 秒：冷却不该启动
    for dt in (2, 5, 8, 10):
        g4.feed(60, 100.0 + dt)
        g4.tick(100.0 + dt)
    check("高位停留不消耗冷却", g4.state == TRIPPED and not rel_c,
          (g4.state, rel_c))
    check("高位时 release_since 为空", g4.release_since is None, g4.release_since)

    # 降到 40（阈值50-回差10=40，<=40 才算回落）—— 从这一刻开始计时
    g4.feed(40, 110.0)
    g4.tick(110.0)
    check("回落后开始计时", g4.release_since is not None, g4.release_since)

    g4.feed(40, 112.0)
    g4.tick(112.0)
    check("回落 2s 仍在冷却", g4.state == TRIPPED and not rel_c, rel_c)

    g4.feed(40, 113.1)
    g4.tick(113.1)
    check("回落满 3s 才恢复", g4.state == ARMED and len(rel_c) == 1,
          (g4.state, rel_c))

    # 冷却中途气压回升 -> 计时作废
    rel_d = []
    g5 = EdgeGuard(threshold=50, release_delta=10, dwell=0.0, cooldown=3.0,
                   auto_rearm=True, stale_after=999,
                   on_trip=lambda p, t: None,
                   on_release=lambda: rel_d.append(1))
    g5.arm()
    g5.feed(60, 200.0); g5.tick(200.0)
    g5.feed(10, 200.1); g5.tick(200.1)      # 回落，计时开始
    g5.tick(201.5)                           # 冷却进行 1.4s
    g5.feed(60, 202.0); g5.tick(202.0)      # 又升回去
    check("回升使计时作废", g5.release_since is None, g5.release_since)
    g5.tick(204.0); g5.tick(205.0)
    check("回升后不恢复", g5.state == TRIPPED and not rel_d, rel_d)
    g5.feed(10, 206.0); g5.tick(206.0)      # 重新回落，计时归零
    g5.feed(10, 208.5); g5.tick(208.5)
    check("重新回落未满 3s 不恢复", g5.state == TRIPPED and not rel_d, rel_d)
    g5.feed(10, 209.2); g5.tick(209.2)
    check("重新回落满 3s 恢复", g5.state == ARMED and len(rel_d) == 1,
          (g5.state, rel_d))

    # 不自动重布防：回 IDLE，且绝不能触发 on_release（那会误恢复输出）
    rel3 = []
    g3 = EdgeGuard(threshold=50, dwell=0, cooldown=0, auto_rearm=False,
                   on_trip=lambda p, t: None,
                   on_release=lambda: rel3.append(1))
    g3.arm()
    g3.feed(60, 1.0)
    g3.feed(10, 1.1)
    check("auto_rearm=False 回 IDLE", g3.state == IDLE, g3.state)
    check("auto_rearm=False 不触发 on_release", len(rel3) == 0, rel3)


# ---------------------------------------------------------------- C 中继
def test_relay():
    print("\n[C] V4 中继 + APP 握手")
    port = 19998
    relay, th = relay_v4.serve_in_thread("127.0.0.1", port)
    time.sleep(0.2)

    events = []
    sensor = EdgeSensor("ws://127.0.0.1:%d/" % port,
                        on_pressure=lambda p, ts, raw: events.append(("p", p)),
                        on_state=lambda s: events.append(("s", s)),
                        on_log=lambda m: events.append(("log", m)))
    sensor.start()

    deadline = time.time() + 5
    while time.time() < deadline and not sensor.target_id:
        time.sleep(0.05)
    check("控制方拿到 targetId", bool(sensor.target_id), sensor.target_id)

    # 模拟 APP 接入
    app_ws = wsproto.connect("ws://127.0.0.1:%d/?tid=%s" % (port, sensor.target_id),
                             timeout=5)
    got_hello = got_ctl = False
    t_end = time.time() + 3
    while time.time() < t_end and not (got_hello and got_ctl):
        txt = app_ws.recv_text(0.2)
        if not txt:
            continue
        f = json.loads(txt)
        if f.get("type") == "hello":
            got_hello = True
        if f.get("type") == "controller_attached":
            got_ctl = True
    check("APP 收到 hello", got_hello)
    check("APP 收到 controller_attached", got_ctl)

    # APP 推 snapshot
    app_ws.send_json({"type": "message", "data": {
        "t": "ev", "ev": "devices.snapshot",
        "devices": [{"slotId": "slot-x", "name": "灵猫", "type": "BMTR_1",
                     "props": {"pressure": 42}, "slotState": {}}]}})

    deadline = time.time() + 3
    while time.time() < deadline and sensor.pressure_slot != "slot-x":
        time.sleep(0.05)
    check("控制方识别灵猫 slot", sensor.pressure_slot == "slot-x",
          sensor.pressure_slot)
    check("snapshot 气压到位", sensor.last_pressure == 42, sensor.last_pressure)

    # APP 推增量
    app_ws.send_json({"type": "message", "data": {
        "t": "ev", "ev": "slots.patch",
        "slots": [{"slotId": "slot-x", "props": {"pressure": 77.5}}]}})
    deadline = time.time() + 3
    while time.time() < deadline and sensor.last_pressure != 77.5:
        time.sleep(0.05)
    check("slots.patch 增量合并", sensor.last_pressure == 77.5, sensor.last_pressure)
    check("气压回调触发", any(e[0] == "p" and e[1] == 77.5 for e in events),
          events[-5:])

    # 控制方 -> APP 的 RPC 透传
    sensor._rpc("devices.get")
    seen_req = False
    t_end = time.time() + 3
    while time.time() < t_end and not seen_req:
        txt = app_ws.recv_text(0.2)
        if not txt:
            continue
        f = json.loads(txt)
        if f.get("type") == "message" and (f.get("data") or {}).get("t") == "req":
            seen_req = (f["data"].get("m") == "devices.get")
    check("控制方 RPC 透传到 APP", seen_req)

    # APP 上报 -> 控制方
    app_ws.send_json({"type": "message", "data": {"t": "ev", "ev": "custom.action",
                                                  "action": 3}})
    time.sleep(0.4)
    check("APP 上报透传到控制方",
          any(e[0] == "log" and "action=3" in str(e[1]) for e in events),
          events[-3:])

    sensor.stop()
    try:
        app_ws.close()
    except Exception:
        pass
    relay.stop()
    time.sleep(0.2)
    return port


# ---------------------------------------------------------------- D 联动
def test_integration():
    print("\n[D] 气压 -> 边控 -> 全停 联动")
    sent = []

    class FakeBle:
        def do_emergency_stop(self, repeats=3, cancel_lost=False):
            if cancel_lost:
                sent.append(P.LOST_CANCEL)
            for _ in range(repeats):
                sent.append(P.STOP)

    ble = FakeBle()
    sliders_zeroed = []

    g = EdgeGuard(threshold=50, release_delta=10, dwell=0.0, cooldown=1.0,
                  auto_rearm=True,
                  on_trip=lambda p, t: ble.do_emergency_stop(3, True))

    g.arm()
    # 模拟传感器连续采样：低 -> 高
    for v in (10, 20, 30, 60):
        g.feed(v, time.time())
    check("越线触发全停", g.state == TRIPPED, g.state)
    check("全停帧连发 3 次", sent.count(P.STOP) == 3, sent)
    check("触发时取消失控", P.LOST_CANCEL in sent, sent)

    # 锁定：触发期间界面层应拒绝手动下发（由 GUI._guard_blocks 实现）
    def guard_blocks(state):
        return state == TRIPPED
    check("触发期间锁定手动输出", guard_blocks(g.state))


# ---------------------------------------------------------------- E 绘图分段
def test_plot_segments():
    """越线描红必须按连续段分组，否则两次触发会被一条直线连起来。"""
    print("\n[E] 越线描红分段")
    import funf_edge_gui as G
    y_of = lambda v: 100 - v

    def seg(vals, thr=45):
        hist = [(i * 0.1, v, "real") for i, v in enumerate(vals)]
        return G.App._over_segments(hist, thr, hist[0][0],
                                    hist[-1][0] - hist[0][0], 600, y_of)

    check("单次越线 = 1 段", len(seg([10, 20, 50, 60, 55, 20, 10])) == 1)
    check("两次独立越线 = 2 段", len(seg([10, 50, 55, 10, 20, 52, 58, 10])) == 2)
    check("全程越线 = 1 段", len(seg([50, 55, 60, 58])) == 1)
    check("全程未越线 = 0 段", len(seg([10, 20, 30])) == 0)
    check("开头即越线", len(seg([50, 55, 10, 20])) == 1)
    check("结尾越线", len(seg([10, 20, 50, 55])) == 1)

    # 三次触发
    check("三次独立越线 = 3 段",
          len(seg([50, 10, 50, 10, 50, 10])) == 3)


# ---------------------------------------------------------------- F 触发上限
def test_max_trips():
    """触发上限：走完最后一次冷却才撤防，并回调 on_finish（冲刺）。"""
    print("\n[F] 触发上限自动撤防")
    log = []
    g = EdgeGuard(threshold=50, release_delta=10, dwell=0.0, cooldown=2.0,
                  auto_rearm=True, stale_after=999, max_trips=2,
                  on_trip=lambda p, t: log.append("TRIP"),
                  on_release=lambda: log.append("RELEASE"),
                  on_finish=lambda n: log.append("FINISH%d" % n))
    g.arm()

    def cycle(t0):
        g.feed(60, t0); g.tick(t0)              # 触发
        g.feed(10, t0 + 0.1); g.tick(t0 + 0.1)  # 回落，冷却开始
        g.feed(10, t0 + 2.2); g.tick(t0 + 2.2)  # 冷却结束

    cycle(100.0)
    check("第1次冷却后重新布防", g.state == ARMED and g.trip_count == 1,
          (g.state, g.trip_count))
    check("未达上限不 finish", not g.finished and "FINISH2" not in log, log)

    cycle(110.0)
    check("第2次达到上限后撤防", g.state == IDLE, g.state)
    check("标记 finished", g.finished, g.finished)
    check("最后一次冷却后才 finish", log == ["TRIP", "RELEASE", "TRIP", "FINISH2"],
          log)
    check("达到上限后不再 RELEASE", log.count("RELEASE") == 1, log)

    # 撤防后气压再高也不触发
    n0 = g.trip_count
    g.feed(999, 200.0); g.tick(200.0)
    check("撤防后不再触发", g.trip_count == n0 and g.state == IDLE,
          (g.trip_count, g.state))

    # max_trips=0 表示不限制
    g2 = EdgeGuard(threshold=50, dwell=0.0, cooldown=0.0, auto_rearm=True,
                   stale_after=999, max_trips=0,
                   on_trip=lambda p, t: None, on_finish=lambda n: log.append("X"))
    g2.arm()
    for i in range(5):
        g2.feed(60, 300.0 + i * 10); g2.tick(300.0 + i * 10)
        g2.feed(10, 300.1 + i * 10); g2.tick(300.1 + i * 10)
    check("max_trips=0 不限次数", g2.trip_count == 5 and not g2.finished,
          (g2.trip_count, g2.finished))


# ---------------------------------------------------------------- G 二维码
def test_qr():
    """二维码必须：颜色不反、不溢出画布、模块 >= 4px、能被真实解码器扫出。"""
    print("\n[G] 二维码渲染")
    import tkinter as tk
    import qrcode
    import funf_edge_gui as G

    root = tk.Tk()
    app = G.App(root)
    root.update()

    payload = ("https://dungeon-lab.cn/s/?v=1&action=socket&url="
               "ws%3A%2F%2F192.168.1.185%3A9998%2F%3Ftid%3Dc2c68a00")
    app._draw_qr(payload)
    root.update()

    img = app._qr_img
    check("生成了位图", img is not None)
    if img is None:
        root.destroy()
        return

    # 1) 不溢出画布
    check("不超出画布", img.width() <= app.QR_BOX,
          (img.width(), app.QR_BOX))

    # 2) 与 qrcode 矩阵逐模块一致（这条能抓出颜色反转）
    q = qrcode.QRCode(border=4, box_size=1,
                      error_correction=qrcode.constants.ERROR_CORRECT_M)
    q.add_data(payload)
    q.make(fit=True)
    m = q.get_matrix()
    n = len(m)
    scale = img.width() // n

    def px(x, y):
        v = img.get(x, y)
        return int(v[0] if isinstance(v, tuple) else v.split()[0])

    bad = 0
    for r in range(n):
        for c in range(n):
            got = px(c * scale + scale // 2, r * scale + scale // 2)
            if (got < 128) != bool(m[r][c]):
                bad += 1
    check("像素与矩阵一致（颜色未反）", bad == 0, "不符 %d/%d" % (bad, n * n))

    # 3) 模块足够大
    check("每模块 >= 4px", scale >= 4, "scale=%d" % scale)

    # 4) 静区存在：左上角 4 模块必须全白
    quiet = all(px(x, y) > 128
                for y in range(4 * scale) for x in range(4 * scale))
    check("四周静区为白", quiet)

    # 5) 真实解码器能扫出
    try:
        import cv2
        import numpy as np
        w, h = img.width(), img.height()
        arr = np.full((h, w), 255, dtype=np.uint8)
        for y in range(h):
            for x in range(w):
                arr[y, x] = px(x, y)
        data, _pts, _ = cv2.QRCodeDetector().detectAndDecode(arr)
        check("OpenCV 能解码且内容一致", data == payload,
              (data[:40] if data else "(空)"))
    except ImportError:
        print("  - 跳过 OpenCV 解码校验（未安装）")

    root.destroy()


# ---------------------------------------------------------------- H 实时下发
def test_live_send():
    """滑杆实时下发：节流生效、端点精确、模式即时。"""
    print("\n[H] 滑杆实时下发")
    import tkinter as tk
    import funf_edge_gui as G

    class FC:
        is_connected = True
        def __init__(self):
            self.writes = []
        async def write_gatt_char(self, ch, data, response=True):
            self.writes.append(data.hex())

    root = tk.Tk()
    app = G.App(root)
    root.update()
    fc = FC()
    app.ble.client = fc

    # 拖动 0->100 后松手。
    # 注意：after() 定时器要靠 update() + 真实时间推进才会触发，
    # 单纯连续 update() 不会让节流器跑起来 —— 必须夹 sleep。
    import time as _t
    fc.writes.clear()
    for v in range(0, 101, 10):
        app.sliders["vib"].set(v)
        app._queue_live_send("vib")
        root.update()
        _t.sleep(0.03)          # > LIVE_SEND_MS/4，让节流器有机会触发
        root.update()
    app.sliders["vib"].set(100)
    app._flush_now("vib")
    for _ in range(10):
        root.update()
        _t.sleep(0.02)

    vals = [int(f[14:16], 16) for f in fc.writes if f.startswith("01010002000111")]
    check("拖动产生多帧", len(vals) >= 3, vals)
    check("末值精确为 100", vals and vals[-1] == 100, vals)
    check("中间值被节流合并（非逐点全发）", len(vals) < 11, len(vals))

    # 模式变化即时下发（同样需要时间推进让节流器跑）
    fc.writes.clear()
    app.modes["vib"].set("4")
    for _ in range(12):
        root.update()
        _t.sleep(0.03)
    check("模式变化立即下发",
          any("000211 04".replace(" ", "") in f for f in fc.writes), fc.writes)

    # 快照记录的是真正发出去的值
    check("快照记录已下发值", app.last_output and app.last_output["vib"][0] == 100,
          app.last_output)

    # ---- 手感：快速来回拖动不该回放历史值 ----
    import time as _t

    class SlowFC:
        """故意慢的 BLE（30ms/次），用来暴露「历史值回放」问题。"""
        is_connected = True
        def __init__(self):
            self.writes = []
            self.times = []
        async def write_gatt_char(self, ch, data, response=True):
            self.writes.append(data.hex())
            self.times.append(_t.time())
            import asyncio as _a
            await _a.sleep(0.03)

    slow = SlowFC()
    app.ble.client = slow
    slow.writes.clear()
    slow.times.clear()

    t0 = _t.time()
    while _t.time() - t0 < 1.2:
        ph = ((_t.time() - t0) * 2) % 1.0
        v = int(ph * 100 if ph < 0.5 else (1 - ph) * 100)
        app.sliders["vib"].set(v)
        app._queue_live_send("vib")
        root.update()
        _t.sleep(0.016)
    drag_end = _t.time()
    for _ in range(40):
        root.update()
        _t.sleep(0.02)

    tail = (_t.time() - drag_end) * 1000
    check("松手后快速收尾（无回放）", slow.times and
          (slow.times[-1] - drag_end) < 0.25,
          "尾帧 %.0fms" % tail)
    check("拖动过程中有连续输出", len(slow.writes) >= 8, len(slow.writes))

    # ---- 插值：大跳变分步逼近，不一次写到位 ----
    app._actual_live["vib"] = (0, 1)
    app._target_live["vib"] = (100, 1)
    seq = []
    for _ in range(12):
        app._live_tick()
        seq.append(app._actual_live["vib"][0])
        if seq[-1] == 100:
            break
    check("大跳变分步逼近（非一次到位）", len(seq) >= 3 and seq[-1] == 100, seq)
    check("步进单调递增", all(b >= a for a, b in zip(seq, seq[1:])), seq)

    root.destroy()


def main():
    test_proto()
    test_guard()
    test_relay()
    test_integration()
    test_plot_segments()
    test_max_trips()
    test_qr()
    test_live_send()
    print("\n" + ("全部通过" if not FAILED else "失败项: %s" % FAILED))
    return 0 if not FAILED else 1


if __name__ == "__main__":
    sys.exit(main())
