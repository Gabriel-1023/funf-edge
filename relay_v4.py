#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
relay_v4.py — DG-LAB 4 APP WebSocket 中继服务（V4 协议）的纯 Python 实现

对应 https://github.com/dungeonlab-open/dglab-websocket-server 的 v4-server.ts，
行为对齐：
  - 控制方连 ws://host:port/            -> hello {clientId}
  - 被控方连 ws://host:port/?tid=<ctl>  -> hello + controller_attached
  - 控制方 <-> 被控方 通过 {"type":"message","clientId":..,"data":..} 透传
  - 业务心跳 30s、服务端级 ping/pong、空闲超时 4002、控制方断开踢被控方 4000

用 Python 重写的原因：目标机器只有 Python，装 bun 只为跑一个中继太重。
也可以直接用官方 wss://trex.dungeon-lab.cn/v4，本服务只是本地替代品。
"""
import json
import os
import socket
import threading
import time
import uuid

import wsproto

CLOSE_CONTROLLER_DISCONNECTED = 4000
CLOSE_CONTROLLER_NOT_FOUND = 4001
CLOSE_IDLE_TIMEOUT = 4002

HEARTBEAT_MS = int(os.environ.get("HEARTBEAT_INTERVAL", "30000"))
IDLE_TIMEOUT_MS = int(os.environ.get("IDLE_TIMEOUT", "300000"))
PREFIX = os.environ.get("PREFIX", "/")


def _log(level, msg):
    print("[relay:%s] %s" % (level, msg), flush=True)


class RelayV4:
    def __init__(self, host="127.0.0.1", port=9998):
        self.host = host
        self.port = port
        self._lock = threading.RLock()
        # clientId -> ws
        self.controllers = {}
        # controllerWs -> {clientId: ws}
        self.clients_of = {}
        # clientWs -> controllerWs
        self.controller_of = {}
        self._idle = {}          # controllerWs -> threading.Timer
        self._srv = None
        self._alive = False

    # ------------------------------------------------------------ 生命周期
    def bind(self):
        """只做 bind/listen，把失败暴露给调用方。

        单独拆出来是为了让 GUI 能同步知道端口有没有起来 —— 否则线程里
        崩掉、界面却显示「中继运行中」，用户会一直等一个不存在的服务。
        """
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind((self.host, self.port))
            srv.listen(16)
        except OSError:
            srv.close()
            raise
        srv.settimeout(0.5)
        self._srv = srv
        return srv

    def start(self, bound=False):
        """阻塞启动（在独立线程里调用）。

        bound=True 表示 bind() 已经由调用方做过，这里只跑 accept 循环。
        """
        srv = self._srv if bound else self.bind()
        self._alive = True
        _log("info", "服务启动 %s:%d 路径=%s" % (self.host, self.port, PREFIX))
        t = threading.Thread(target=self._heartbeat_loop, daemon=True)
        t.start()
        try:
            while self._alive:
                try:
                    conn, addr = srv.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                threading.Thread(target=self._serve, args=(conn, addr),
                                 daemon=True).start()
        finally:
            self._alive = False
            try:
                srv.close()
            except OSError:
                pass
            _log("info", "服务停止")

    def stop(self):
        self._alive = False
        try:
            if self._srv:
                self._srv.close()
        except OSError:
            pass

    def _heartbeat_loop(self):
        payload = json.dumps({"type": "heartbeat"})
        while self._alive:
            time.sleep(HEARTBEAT_MS / 1000.0)
            if not self._alive:
                break
            with self._lock:
                targets = list(self.controllers.values())
                for m in self.clients_of.values():
                    targets.extend(m.values())
            for ws in targets:
                if ws.closed:
                    continue
                try:
                    ws.send(payload)
                except Exception:
                    pass

    # ------------------------------------------------------------ 单连接
    def _serve(self, conn, addr):
        try:
            ws = wsproto.accept(conn, timeout=10)
        except Exception as e:
            _log("warn", "握手失败 %s: %s" % (addr, e))
            try:
                conn.close()
            except OSError:
                pass
            return

        client_id = uuid.uuid4().hex[:8]
        path = ws.path
        tid = None
        if "?" in path:
            q = path.split("?", 1)[1]
            for kv in q.split("&"):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    if k in ("tid", "targetId"):
                        tid = v
        base_path = path.split("?", 1)[0]
        if base_path not in (PREFIX, PREFIX.rstrip("/") or "/"):
            try:
                ws.send_json({"type": "error", "code": "bad_path"})
            except Exception:
                pass
            ws.close(1008, "bad_path")
            return

        try:
            ws.send_json({"type": "hello", "clientId": client_id})
        except Exception:
            ws.close()
            return

        if tid:
            self._attach_client(ws, client_id, tid)
        else:
            self._attach_controller(ws, client_id)

        try:
            while not ws.closed:
                txt = ws.recv_text(0.5)
                if txt is None:
                    continue
                self._on_message(ws, client_id, txt)
        except wsproto.WSClosed as e:
            self._on_close(ws, client_id, e.code, e.reason)
        except Exception as e:
            _log("warn", "连接异常 %s: %s" % (client_id, e))
            self._on_close(ws, client_id, 1006, str(e))

    def _attach_controller(self, ws, client_id):
        with self._lock:
            self.controllers[client_id] = ws
            self.clients_of[ws] = {}
            self._start_idle(ws)
        _log("info", "控制方连接 控制方=%s" % client_id)

    def _attach_client(self, ws, client_id, tid):
        with self._lock:
            ctl = self.controllers.get(tid)
            if not ctl or ctl.closed:
                try:
                    ws.send_json({"type": "error", "code": "controller_not_found"})
                except Exception:
                    pass
                ws.close(CLOSE_CONTROLLER_NOT_FOUND, "controller_not_found")
                _log("warn", "被控方拒绝 被控方=%s 目标=%s 原因=控制方不存在"
                     % (client_id, tid))
                return
            self.clients_of[ctl][client_id] = ws
            self.controller_of[ws] = ctl
            self._cancel_idle(ctl)
        try:
            ws.send_json({"type": "controller_attached", "clientId": tid})
            ctl.send_json({"type": "client_attached", "clientId": client_id})
        except Exception:
            pass
        _log("info", "被控方接入 被控方=%s 控制方=%s" % (client_id, tid))

    def _on_message(self, ws, client_id, txt):
        try:
            msg = json.loads(txt)
        except ValueError:
            _log("warn", "WS JSON 无效 连接=%s" % client_id)
            return
        if not isinstance(msg, dict):
            return
        mtype = msg.get("type")
        if mtype == "ping":
            try:
                ws.send_json({"type": "pong", "ts": int(time.time() * 1000)})
            except Exception:
                pass
            return
        if mtype != "message":
            return

        with self._lock:
            is_controller = client_id in self.controllers
            if is_controller:
                target = msg.get("clientId")
                if not isinstance(target, str):
                    try:
                        ws.send_json({"type": "error", "code": "bad_request",
                                      "message": "message.clientId is required"})
                    except Exception:
                        pass
                    return
                tws = self.clients_of.get(ws, {}).get(target)
                if tws and not tws.closed:
                    try:
                        tws.send_json({"type": "message", "data": msg.get("data")})
                    except Exception:
                        pass
                else:
                    try:
                        ws.send_json({"type": "error", "code": "client_not_found",
                                      "clientId": target})
                    except Exception:
                        pass
                return
            ctl = self.controller_of.get(ws)

        if ctl and not ctl.closed:
            try:
                ctl.send_json({"type": "message", "clientId": client_id,
                               "data": msg.get("data")})
            except Exception:
                pass

    def _on_close(self, ws, client_id, code, reason):
        with self._lock:
            if client_id in self.controllers:
                self.controllers.pop(client_id, None)
                clients = self.clients_of.pop(ws, {})
                self._cancel_idle(ws)
                targets = list(clients.items())
            else:
                ctl = self.controller_of.pop(ws, None)
                targets = []
                if ctl:
                    self.clients_of.get(ctl, {}).pop(client_id, None)
                    if ctl not in self.clients_of or not self.clients_of[ctl]:
                        self._start_idle(ctl)
                    targets = [("__notify__", ctl)]

        for cid, other in targets:
            if other.closed:
                continue
            try:
                if cid == "__notify__":
                    other.send_json({"type": "client_disconnected",
                                     "clientId": client_id})
                else:
                    other.send_json({"type": "controller_disconnected",
                                     "clientId": client_id})
                    other.close(CLOSE_CONTROLLER_DISCONNECTED,
                                "controller_disconnected")
            except Exception:
                pass
        _log("info", "断开 %s code=%s reason=%s" % (client_id, code, reason or "-"))

    # ------------------------------------------------------------ 空闲计时
    def _start_idle(self, ctl_ws):
        self._cancel_idle(ctl_ws)
        def fire():
            with self._lock:
                self._idle.pop(ctl_ws, None)
            if ctl_ws.closed:
                return
            try:
                ctl_ws.send_json({"type": "idle_timeout"})
                ctl_ws.close(CLOSE_IDLE_TIMEOUT, "idle_timeout")
            except Exception:
                pass
            _log("warn", "控制方空闲超时")
        t = threading.Timer(IDLE_TIMEOUT_MS / 1000.0, fire)
        t.daemon = True
        t.start()
        self._idle[ctl_ws] = t

    def _cancel_idle(self, ctl_ws):
        t = self._idle.pop(ctl_ws, None)
        if t:
            t.cancel()


def serve_in_thread(host="0.0.0.0", port=9998):
    """在后台线程起中继。

    host 默认 0.0.0.0 —— 手机上的 DG-LAB APP 要跨设备连进来，只听 127.0.0.1
    的话手机根本够不到。本机自己连回环就行，不必走局域网地址。

    端口占用等错误会在这里同步抛出，不会变成线程里的静默崩溃。
    """
    relay = RelayV4(host, port)
    relay.bind()                       # 失败直接抛，调用方好提示
    th = threading.Thread(target=relay.start, kwargs={"bound": True}, daemon=True)
    th.start()
    time.sleep(0.3)
    return relay, th


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9998
    relay = RelayV4("0.0.0.0", port)
    try:
        relay.start()
    except KeyboardInterrupt:
        relay.stop()
