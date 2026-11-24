#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
wsproto.py — 极简 RFC6455 WebSocket 客户端 / 服务端（只用标准库）

为什么不用 websockets / aiohttp：
  - 本项目要跑在别人的 Mac 上，Tk + bleak 已经是硬依赖，不想再压一个 asyncio WS 栈；
  - DG-LAB 中继只用 text 帧 + 少量 ping/pong，协议面很窄；
  - 自带实现可以同时给「Python 版中继服务端」和「控制端客户端」复用同一份代码。

已实现：
  - HTTP Upgrade 握手（客户端 / 服务端两侧），wss 走 ssl
  - 文本 / 二进制帧，126 / 127 长度分支，客户端强制掩码、服务端解掩码
  - 分片（continuation）重组
  - ping 自动回 pong、close 双向握手与关闭码解析
  - 增量缓冲解析：读超时不会破坏帧边界（这点对 Tk 里 0.2s 轮询很关键）
"""
import base64
import hashlib
import os
import socket
import ssl
import struct
import threading
import time
from urllib.parse import urlparse

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OP_CONT = 0x0
OP_TEXT = 0x1
OP_BIN = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


class WSError(Exception):
    pass


class WSClosed(WSError):
    """对端关闭 / 连接异常。"""

    def __init__(self, msg, code=1006, reason=""):
        super().__init__(msg)
        self.code = code
        self.reason = reason


def _encode(payload, opcode, mask):
    b1 = 0x80 | opcode
    n = len(payload)
    flag = 0x80 if mask else 0x00
    if n < 126:
        head = struct.pack("!BB", b1, flag | n)
    elif n < 65536:
        head = struct.pack("!BBH", b1, flag | 126, n)
    else:
        head = struct.pack("!BBQ", b1, flag | 127, n)
    if mask:
        key = os.urandom(4)
        body = bytes(c ^ key[i % 4] for i, c in enumerate(payload))
        return head + key + body
    return head + payload


class WSSocket:
    """一条已握手的 WebSocket 连接。线程安全的 send，单线程 recv。"""

    def __init__(self, sock, client_side):
        self.sock = sock
        self.client_side = client_side
        self.path = "/"
        self._buf = b""
        self._send_lock = threading.Lock()
        self.closed = False
        self.close_code = None
        self.close_reason = ""
        self.remote = None

    # ---------- 发送 ----------
    def send_raw(self, payload, opcode=OP_TEXT):
        if self.closed:
            raise WSClosed("send on closed socket")
        data = _encode(payload, opcode, mask=self.client_side)
        with self._send_lock:
            try:
                self.sock.sendall(data)
            except OSError as e:
                self.closed = True
                raise WSClosed("send failed: %s" % e)

    def send(self, text):
        self.send_raw(text.encode("utf-8"), OP_TEXT)

    def send_json(self, obj):
        import json
        self.send(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))

    # ---------- 接收 ----------
    def _fill(self, timeout):
        try:
            self.sock.settimeout(timeout)
        except OSError:
            pass
        try:
            chunk = self.sock.recv(65536)
        except socket.timeout:
            return False
        except OSError as e:
            self.closed = True
            raise WSClosed("recv failed: %s" % e)
        if not chunk:
            self.closed = True
            raise WSClosed("peer eof")
        self._buf += chunk
        return True

    def _parse_frame(self):
        b = self._buf
        if len(b) < 2:
            return None
        b0, b1 = b[0], b[1]
        fin = bool(b0 & 0x80)
        op = b0 & 0x0F
        masked = bool(b1 & 0x80)
        ln = b1 & 0x7F
        idx = 2
        if ln == 126:
            if len(b) < 4:
                return None
            ln = struct.unpack("!H", b[2:4])[0]
            idx = 4
        elif ln == 127:
            if len(b) < 10:
                return None
            ln = struct.unpack("!Q", b[2:10])[0]
            idx = 10
        key = None
        if masked:
            if len(b) < idx + 4:
                return None
            key = b[idx:idx + 4]
            idx += 4
        if len(b) < idx + ln:
            return None
        payload = b[idx:idx + ln]
        if key:
            payload = bytes(c ^ key[i % 4] for i, c in enumerate(payload))
        self._buf = b[idx + ln:]
        return fin, op, payload

    def recv(self, timeout=0.5):
        """返回 bytes（文本也返回 bytes，调用方自行 decode）；超时返回 None。"""
        deadline = time.time() + timeout
        frag = None
        while True:
            frame = self._parse_frame()
            if frame is None:
                remain = deadline - time.time()
                if remain <= 0:
                    return None
                self._fill(min(remain, 0.25))
                continue
            fin, op, payload = frame
            if op == OP_PING:
                try:
                    self.send_raw(payload, OP_PONG)
                except WSClosed:
                    pass
                continue
            if op == OP_PONG:
                continue
            if op == OP_CLOSE:
                code = 1005
                reason = ""
                if len(payload) >= 2:
                    code = struct.unpack("!H", payload[:2])[0]
                    reason = payload[2:].decode("utf-8", "replace")
                self.close_code = code
                self.close_reason = reason
                try:
                    self.send_raw(struct.pack("!H", 1000), OP_CLOSE)
                except WSClosed:
                    pass
                self.closed = True
                raise WSClosed("closed code=%s reason=%s" % (code, reason),
                               code=code, reason=reason)
            if op in (OP_TEXT, OP_BIN):
                if not fin:
                    frag = payload
                    continue
                return payload
            if op == OP_CONT:
                frag = (frag or b"") + payload
                if fin:
                    out, frag = frag, None
                    return out
                continue

    def recv_text(self, timeout=0.5):
        raw = self.recv(timeout)
        if raw is None:
            return None
        return raw.decode("utf-8", "replace")

    # ---------- 关闭 ----------
    def close(self, code=1000, reason=""):
        if self.closed:
            return
        try:
            payload = struct.pack("!H", code) + reason.encode("utf-8")[:120]
            self.send_raw(payload, OP_CLOSE)
        except WSClosed:
            pass
        self.closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


# ---------------------------------------------------------------- 客户端

def connect(url, timeout=10.0, extra_headers=None):
    """连接 ws:// 或 wss://，返回 WSSocket。"""
    u = urlparse(url)
    if u.scheme not in ("ws", "wss"):
        raise WSError("不支持的 scheme: %s" % u.scheme)
    secure = u.scheme == "wss"
    host = u.hostname
    if not host:
        raise WSError("URL 缺少 host: %s" % url)
    port = u.port or (443 if secure else 80)
    path = u.path or "/"
    if u.query:
        path += "?" + u.query

    sock = socket.create_connection((host, port), timeout=timeout)
    if secure:
        ctx = ssl.create_default_context()
        sock = ctx.wrap_socket(sock, server_hostname=host)

    key = base64.b64encode(os.urandom(16)).decode()
    host_hdr = host if port in (80, 443) else "%s:%d" % (host, port)
    req = (
        "GET %s HTTP/1.1\r\n"
        "Host: %s\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "User-Agent: funf-edge/1.0\r\n" % (path, host_hdr, key)
    )
    for k, v in (extra_headers or {}).items():
        req += "%s: %s\r\n" % (k, v)
    req += "\r\n"
    sock.sendall(req.encode())

    buf = b""
    sock.settimeout(timeout)
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise WSClosed("握手阶段对端关闭")
        buf += chunk
        if len(buf) > 65536:
            raise WSError("握手响应过大")

    head, rest = buf.split(b"\r\n\r\n", 1)
    text = head.decode("latin-1")
    status = text.split("\r\n")[0]
    if " 101" not in status:
        raise WSError("握手失败: %s" % status)

    expect = base64.b64encode(
        hashlib.sha1((key + GUID).encode()).digest()).decode()
    if expect.lower() not in text.lower():
        raise WSError("Sec-WebSocket-Accept 校验失败")

    ws = WSSocket(sock, client_side=True)
    ws._buf = rest
    ws.path = path
    ws.remote = "%s:%d" % (host, port)
    return ws


# ---------------------------------------------------------------- 服务端

def accept(conn, timeout=10.0, extra_response=None):
    """接受一个已建立的 TCP 连接，完成握手。失败抛 WSClosed / WSError。"""
    conn.settimeout(timeout)
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = conn.recv(4096)
        if not chunk:
            raise WSClosed("握手阶段对端关闭")
        buf += chunk
        if len(buf) > 65536:
            raise WSError("请求头过大")

    head, rest = buf.split(b"\r\n\r\n", 1)
    lines = head.decode("latin-1").split("\r\n")
    parts = lines[0].split()
    if len(parts) < 2 or parts[0].upper() != "GET":
        raise WSError("非 WebSocket 请求: %s" % lines[0])
    path = parts[1]

    hdrs = {}
    for ln in lines[1:]:
        if ":" in ln:
            k, v = ln.split(":", 1)
            hdrs[k.strip().lower()] = v.strip()

    if "websocket" not in hdrs.get("upgrade", "").lower():
        raise WSError("缺少 Upgrade: websocket")
    key = hdrs.get("sec-websocket-key")
    if not key:
        raise WSError("缺少 Sec-WebSocket-Key")

    accept_val = base64.b64encode(
        hashlib.sha1((key + GUID).encode()).digest()).decode()
    resp = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: %s\r\n" % accept_val
    )
    for k, v in (extra_response or {}).items():
        resp += "%s: %s\r\n" % (k, v)
    resp += "\r\n"
    conn.sendall(resp.encode())

    ws = WSSocket(conn, client_side=False)
    ws._buf = rest
    ws.path = path
    try:
        ws.remote = "%s:%d" % conn.getpeername()[:2]
    except OSError:
        ws.remote = "?"
    return ws
