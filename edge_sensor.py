#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
edge_sensor.py — 「灵猫」(BMTR_1) 气压传感器读取客户端

灵猫不直接对外开 socket，它是挂在 DG-LAB 4 APP 上的一个设备插槽（slot）。
要拿到它的实时气压，路径是：

    DG-LAB 4 APP  --ws-->  DG-LAB 中继 (V4)  <--ws--  本程序（控制方）

流程（对齐 dglab-websocket-server / dglab-kit 的 V4 协议）：
  1. 控制方连 ws://host:9998/  -> 收 {"type":"hello","clientId":"<我>"}
  2. 把这个 clientId 当 targetId 生成配对二维码，APP 扫码接入
  3. 收 {"type":"client_attached","clientId":"<APP>"}
  4. 发 RPC {"t":"req","reqId":..,"m":"devices.get"}
  5. APP 回 {"t":"ev","ev":"devices.snapshot","devices":[...]}，其中
     type=="BMTR_1" 的设备 props.pressure 就是气压值
  6. 之后 APP 按 tick 推 {"t":"ev","ev":"slots.patch","slots":[{"slotId":..,"props":{"pressure":N}}]}
     —— 增量帧只带变化字段，必须做深合并

BMTR_1 相关字段（dglab-kit README）：
  props.pressure        气压/压力值
  props.power           电量
  props.connectState    'connected' / 'disconnected'
  slotState.edge.edgeState  0 停止 / 1 刺激 / 2 强制冷静计时 / 3 冷静判断低于蓝线 / 4 允许高潮
"""
import json
import threading
import time

import wsproto

DEVICE_TYPE_BMTR = "BMTR_1"


class EdgeSensor:
    """V4 控制方 + 灵猫气压订阅。所有回调都在后台线程里触发。"""

    def __init__(self, url="ws://127.0.0.1:9998/",
                 on_pressure=None, on_state=None, on_log=None, on_devices=None,
                 public_url=None):
        self.url = url
        # 二维码 / 配对地址里用的对外地址。本机可以走回环连中继，
        # 但手机必须拿到局域网（或公网）地址，所以两者分开。
        self.public_url = public_url or url
        self.on_pressure = on_pressure or (lambda p, ts, raw: None)
        self.on_state = on_state or (lambda s: None)
        self.on_log = on_log or (lambda m: None)
        self.on_devices = on_devices or (lambda d: None)

        self.ws = None
        self.target_id = None          # 给 APP 扫的配对 ID（= 我的 clientId）
        self.app_client_id = None      # APP 被控方 ID
        self.devices = {}              # slotId -> device dict
        self.pressure_slot = None      # 灵猫的 slotId
        self.last_pressure = None
        self.last_pressure_ts = 0.0
        self.connected = False
        self._alive = False
        self._thread = None
        self._send_lock = threading.Lock()
        self._req_seq = 0
        self.pressure_count = 0
        self._last_raw_pressure = None

    # ---------------------------------------------------------------- 生命周期
    def start(self):
        if self._alive:
            return
        self._alive = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._alive = False
        ws = self.ws
        if ws:
            try:
                ws.close(1000, "bye")
            except Exception:
                pass
        self.ws = None
        self.connected = False

    # ---------------------------------------------------------------- 内部
    def _set_state(self, s):
        try:
            self.on_state(s)
        except Exception:
            pass

    def _log(self, msg):
        try:
            self.on_log(msg)
        except Exception:
            pass

    def _send(self, obj):
        ws = self.ws
        if not ws or ws.closed:
            return False
        with self._send_lock:
            try:
                ws.send_json(obj)
                return True
            except Exception as e:
                self._log("发送失败: %s" % e)
                return False

    def _rpc(self, method, data=None):
        self._req_seq += 1
        req = {"t": "req", "reqId": "r%d" % self._req_seq, "m": method}
        if data is not None:
            req["data"] = data
        self._send({"type": "message", "clientId": self.app_client_id, "data": req})

    def _run(self):
        backoff = 1.0
        while self._alive:
            self._set_state("connecting")
            try:
                ws = wsproto.connect(self.url, timeout=8)
            except Exception as e:
                self._log("连接中继失败 %s: %s（%.0fs 后重试）" % (self.url, e, backoff))
                self._set_state("error")
                time.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
                continue
            self.ws = ws
            self.connected = True
            backoff = 1.0
            self._log("已连接中继 %s" % self.url)
            self._set_state("waiting")
            try:
                while self._alive and not ws.closed:
                    txt = ws.recv_text(0.5)
                    if txt is None:
                        continue
                    self._handle(txt)
            except wsproto.WSClosed as e:
                self._log("中继断开 code=%s %s" % (e.code, e.reason or ""))
            except Exception as e:
                self._log("中继异常: %s" % e)
            self.connected = False
            self.ws = None
            self.app_client_id = None
            if self._alive:
                self._set_state("disconnected")
                time.sleep(1.5)

    def _handle(self, txt):
        try:
            frame = json.loads(txt)
        except ValueError:
            return
        if not isinstance(frame, dict):
            return
        ftype = frame.get("type")

        if ftype == "hello":
            self.target_id = frame.get("clientId")
            self._log("我的配对 ID (targetId) = %s" % self.target_id)
            self._set_state("waiting")
            return

        if ftype == "client_attached":
            self.app_client_id = frame.get("clientId")
            self._log("APP 已接入 clientId=%s" % self.app_client_id)
            self._set_state("attached")
            self._rpc("devices.get")
            return

        if ftype == "client_disconnected":
            self._log("APP 断开")
            self.app_client_id = None
            self.devices.clear()
            self.pressure_slot = None
            self._set_state("waiting")
            return

        if ftype == "idle_timeout":
            self._log("中继空闲超时")
            return

        if ftype == "error":
            self._log("中继错误 %s" % (frame.get("code") or frame.get("message")))
            return

        if ftype != "message":
            return

        data = frame.get("data")
        if not isinstance(data, dict):
            return
        t = data.get("t")
        if t == "ev":
            self._handle_event(data)
        elif t == "resp":
            self._handle_response(data)

    def _handle_response(self, data):
        if data.get("error"):
            self._log("RPC %s 失败: %s" % (data.get("m", "?"), data.get("error")))
            return
        result = data.get("result")
        if isinstance(result, dict) and isinstance(result.get("devices"), list):
            self.devices.clear()
            for dev in result["devices"]:
                if isinstance(dev, dict) and dev.get("slotId"):
                    self.devices[dev["slotId"]] = dict(dev)
            self._refresh_devices()

    def _handle_event(self, data):
        ev = data.get("ev")
        if ev == "devices.snapshot":
            self.devices.clear()
            for dev in data.get("devices") or []:
                if isinstance(dev, dict) and dev.get("slotId"):
                    self.devices[dev["slotId"]] = dict(dev)
            self._refresh_devices()
            return

        if ev == "devices.patch":
            for dev in data.get("added") or []:
                if isinstance(dev, dict) and dev.get("slotId"):
                    self.devices[dev["slotId"]] = dict(dev)
            for sid in data.get("removed") or []:
                self.devices.pop(sid, None)
            self._refresh_devices()
            return

        if ev == "slots.patch":
            for slot in data.get("slots") or []:
                if not isinstance(slot, dict):
                    continue
                sid = slot.get("slotId")
                cur = self.devices.get(sid)
                if cur is None:
                    cur = {"slotId": sid, "props": {}, "slotState": {}}
                    self.devices[sid] = cur
                if isinstance(slot.get("props"), dict):
                    cur.setdefault("props", {}).update(slot["props"])
                if isinstance(slot.get("slotState"), dict):
                    cur.setdefault("slotState", {}).update(slot["slotState"])
                self._emit_pressure(sid)
            return

        if ev == "custom.action":
            self._log("APP 自定义动作 action=%s" % data.get("action"))

    def _refresh_devices(self):
        """找出灵猫 slot；若换了设备则重新绑定。"""
        bmtr = [d for d in self.devices.values()
                if d.get("type") == DEVICE_TYPE_BMTR]
        if bmtr:
            sid = bmtr[0]["slotId"]
            if sid != self.pressure_slot:
                self.pressure_slot = sid
                self._log("发现灵猫 slotId=%s" % sid)
                self._set_state("sensor")
            self._emit_pressure(sid)
        else:
            self.pressure_slot = None
        try:
            self.on_devices(list(self.devices.values()))
        except Exception:
            pass

    def _emit_pressure(self, slot_id):
        if slot_id != self.pressure_slot or self.pressure_slot is None:
            return
        dev = self.devices.get(self.pressure_slot) or {}
        props = dev.get("props") or {}
        val = props.get("pressure")
        if not isinstance(val, (int, float)):
            return
        self.last_pressure = val
        self.last_pressure_ts = time.time()
        self.pressure_count += 1
        try:
            self.on_pressure(float(val), self.last_pressure_ts, props)
        except Exception:
            pass

    # ---------------------------------------------------------------- 外部查询
    def snapshot(self):
        return {
            "target_id": self.target_id,
            "app_client_id": self.app_client_id,
            "connected": self.connected,
            "pressure": self.last_pressure,
            "pressure_ts": self.last_pressure_ts,
            "pressure_slot": self.pressure_slot,
            "count": self.pressure_count,
        }

    def pair_url(self, base=None):
        """APP 扫码用的 ws 地址（含 tid）。

        默认用 public_url（对外地址），而不是本机连中继用的 url。
        """
        if not self.target_id:
            return None
        b = base or self.public_url or self.url
        sep = "&" if "?" in b else "?"
        return "%s%stid=%s" % (b, sep, self.target_id)

    def qr_payload(self, base=None):
        """DG-LAB 4 APP 可识别的二维码内容。"""
        url = self.pair_url(base)
        if not url:
            return None
        return ("https://dungeon-lab.cn/s/?v=1&action=socket&url="
                + _quote(url))


def _quote(s):
    from urllib.parse import quote
    return quote(s, safe="")


# ------------------------------------------------------------------ 本地自测
if __name__ == "__main__":
    import sys
    url = sys.argv[1] if len(sys.argv) > 1 else "ws://127.0.0.1:9998/"

    def on_p(p, ts, raw):
        print("[pressure] %.2f  (%s)" % (p, time.strftime("%H:%M:%S",
                                                         time.localtime(ts))))

    s = EdgeSensor(url, on_pressure=on_p,
                   on_state=lambda st: print("[state]", st),
                   on_log=lambda m: print("[log]", m))
    s.start()
    try:
        while True:
            time.sleep(1)
            if s.target_id and not s.app_client_id:
                print("等待 APP 接入 ->", s.pair_url())
    except KeyboardInterrupt:
        s.stop()
