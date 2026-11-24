#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mock_dglab_app.py — 模拟 DG-LAB 4 APP 被控方（含一个灵猫 BMTR_1 设备）

用途：没有真机和灵猫时，验证「中继 -> 控制方 -> 边控」整条链路。

它会：
  1. 连 ws://host:port/?tid=<控制方 clientId>
  2. 收到 controller_attached 后推 devices.snapshot，带一台 BMTR_1
  3. 每 tick 推 slots.patch 的 pressure 增量（正弦 + 噪声，可被 --burst 顶到高位）
  4. 响应 devices.get / device.op / device.op.clear，并回 resp

用法：
  python3 tools/mock_dglab_app.py --tid <targetId> --port 9998
  python3 tools/mock_dglab_app.py --tid abc123 --pressure 800 --once
"""
import argparse
import json
import math
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import wsproto  # noqa: E402


class MockApp:
    def __init__(self, url, pressure=None, tick=0.08, base=120.0, amp=90.0):
        self.url = url
        self.fixed = pressure
        self.tick = tick
        self.base = base
        self.amp = amp
        self.ws = None
        self.slot = "slot-bmtr-1"
        self.req = 0
        self.sent = 0
        self.t0 = time.time()

    def pressure_at(self, now):
        if self.fixed is not None:
            return float(self.fixed)
        t = now - self.t0
        # 慢波 + 快抖，制造能越过/回落阈值的曲线
        v = self.base + self.amp * math.sin(t * 0.9) + 40 * math.sin(t * 5.3)
        return max(0.0, min(1000.0, v))

    def send(self, obj):
        if self.ws and not self.ws.closed:
            self.ws.send_json(obj)

    def rpc_resp(self, req_id, result=None, error=None):
        r = {"t": "resp", "reqId": req_id}
        if error:
            r["error"] = error
        else:
            r["result"] = result
        self.send({"type": "message", "data": r})

    def snapshot(self):
        self.send({"type": "message", "data": {
            "t": "ev", "ev": "devices.snapshot",
            "devices": [{
                "id": 0, "slotId": self.slot, "name": "灵猫",
                "type": "BMTR_1",
                "props": {"power": 88, "version": 1, "label": 0,
                          "pressure": int(self.pressure_at(time.time())),
                          "connectState": "connected",
                          "updateTime": "", "updateValue": ""},
                "slotState": {"markLight": "cyan", "hasDevice": True,
                              "edge": {"edgeState": 1}},
            }],
        }})

    def run(self, once=False):
        ws = wsproto.connect(self.url, timeout=8)
        self.ws = ws
        print("[mock] 已连接 %s" % self.url, flush=True)
        next_tick = time.time()
        attached = False
        while not ws.closed:
            txt = ws.recv_text(0.02)
            if txt:
                try:
                    f = json.loads(txt)
                except ValueError:
                    f = {}
                t = f.get("type")
                if t == "hello":
                    print("[mock] hello clientId=%s" % f.get("clientId"), flush=True)
                elif t == "controller_attached":
                    attached = True
                    print("[mock] controller_attached", flush=True)
                    self.snapshot()
                elif t == "controller_disconnected":
                    print("[mock] 控制方断开，退出", flush=True)
                    break
                elif t == "message":
                    d = f.get("data") or {}
                    if d.get("t") == "req":
                        m = d.get("m")
                        print("[mock] RPC %s %s" % (m, d.get("data")), flush=True)
                        if m == "devices.get":
                            self.rpc_resp(d.get("reqId"), {"devices": [{
                                "id": 0, "slotId": self.slot, "name": "灵猫",
                                "type": "BMTR_1"}]})
                            self.snapshot()
                        elif m == "device.op":
                            self.rpc_resp(d.get("reqId"), {"ok": True})
                        elif m == "device.op.clear":
                            self.rpc_resp(d.get("reqId"), {})
                        else:
                            self.rpc_resp(d.get("reqId"), {"ok": True})
            if once and attached and self.sent >= 3:
                break
            now = time.time()
            if attached and now >= next_tick:
                next_tick = now + self.tick
                p = self.pressure_at(now)
                self.send({"type": "message", "data": {
                    "t": "ev", "ev": "slots.patch",
                    "slots": [{"slotId": self.slot,
                               "props": {"pressure": round(p, 2)},
                               "slotState": {"markLight": "cyan"}}]}})
                self.sent += 1
        print("[mock] 结束，推送 %d 帧" % self.sent, flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tid", required=True, help="控制方 clientId")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9998)
    ap.add_argument("--pressure", type=float, default=None,
                    help="固定气压值；不给则走正弦模拟")
    ap.add_argument("--tick", type=float, default=0.08)
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args()
    url = "ws://%s:%d/?tid=%s" % (a.host, a.port, a.tid)
    try:
        MockApp(url, pressure=a.pressure, tick=a.tick).run(once=a.once)
    except KeyboardInterrupt:
        pass
