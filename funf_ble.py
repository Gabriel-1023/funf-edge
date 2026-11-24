#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
funf_ble.py — 脱离 FUNF App 直接控制 SOSEXY 蓝牙设备（BLE）

逆向来源：FUNF.app (com.funf.iot 3.0.6, bundle 名 Gendome)
  - JS 业务层: Gendome.app/bundle/index.jsbundle (明文 Metro bundle)
  - 原生 BLE 层: Gendome 二进制里的 JL_BLEKit (杰理) + MultiplatformBleAdapter
实测 GATT（macOS 26.1 / CoreBluetooth）：
  服务 0000ee01-...  写 0000ee03（with response） / 通知 0000ee02
  服务 0000ae00-...  写 0000ae01（杰理 RCSP 通道） / 通知 0000ae02
本工具走 0000ee01 服务：把 App 里的 "010100"+TLV 帧原样写入 EE03，从 EE02 收回复。

用法：
  python3 funf_ble.py scan
  python3 funf_ble.py info
  python3 funf_ble.py set --vib 20 --vib-mode 2 --suck 10 --suck-mode 1 --cur 0 --cur-mode 1
  python3 funf_ble.py stop
  python3 funf_ble.py breathe on|off
  python3 funf_ble.py raw 0101000100051101
  python3 funf_ble.py watch
"""
import argparse
import asyncio
import sys
import time

from bleak import BleakClient, BleakScanner

DEV_NAME = "SOSEXY"
CH_WRITE = "0000ee03-0000-1000-8000-00805f9b34fb"
CH_NOTIFY = "0000ee02-0000-1000-8000-00805f9b34fb"

# ---- 属性表（从 bundle/index.jsbundle 的 _bleDataResolve / 各页面指令还原）----
ATTR = {
    0x0001: ("震动强度", "vib"),
    0x0002: ("震动模式", "vib_mode"),
    0x0003: ("电流强度", "cur"),
    0x0004: ("电流模式", "cur_mode"),
    0x0005: ("电量%", "battery"),
    0x0007: ("吮吸强度", "suck"),
    0x0008: ("吮吸模式", "suck_mode"),
    0x0009: ("失控设置时长", "lost_time"),
    0x000A: ("失控剩余时长", "lost_left"),
    0x000B: ("?", "x0b"),
    0x000C: ("闹钟结构(30B)", "alarm"),
    0x000D: ("呼吸灯", "breathe"),
    0x000E: ("?", "x0e"),
}
KEY2ATTR = {v[1]: k for k, v in ATTR.items()}

# 帧内 value 的类型码 -> 字节数
TYPE_LEN = {0x11: 1, 0x12: 4, 0xAF: 30, 0x0A: 4, 0x0C: 30}


def build_body(pairs):
    """pairs: [(attr_id:int, value:int)] -> '02 0001 11 05 0002 11 01'"""
    body = "%02x" % len(pairs)
    for attr, val in pairs:
        body += "%04x11%02x" % (attr, val & 0xFF)
    return body


def frame(body_hex):
    """App 的 iOS 发送前缀：'010100' + body"""
    return "010100" + body_hex


def parse_report(hexstr):
    """解析一条上报帧: 01 <seq> 00 <count> [attr4 type2 value...]"""
    b = bytes.fromhex(hexstr)
    if len(b) < 5:
        return None
    i = 0
    # 帧头 3 字节：01/06 + seq + 00（部分机型只有 2 字节头，做容错）
    if b[2] == 0x00:
        i = 3
    else:
        i = 2
    count = b[i]
    i += 1
    out = {}
    for _ in range(count):
        if i + 3 > len(b):
            break
        attr = (b[i] << 8) | b[i + 1]
        typ = b[i + 2]
        i += 3
        n = TYPE_LEN.get(typ, 1)
        val = b[i:i + n]
        i += n
        out[attr] = val
    return out


def show(report):
    if not report:
        print("  (无法解析)")
        return
    for attr, val in sorted(report.items()):
        name, key = ATTR.get(attr, ("未知", "?"))
        if len(val) == 1:
            v = "%d (0x%02X)" % (val[0], val[0])
        elif len(val) == 4:
            v = "%d (LE32 0x%s)" % (int.from_bytes(val, "little"), val.hex())
        else:
            v = val.hex()
        print("  %04X %-14s = %s" % (attr, name, v))


class Dev:
    def __init__(self, client):
        self.c = client
        self.buf = []

    async def open(self):
        await self.c.start_notify(CH_NOTIFY, self._on_notify)

    def _on_notify(self, _ch, data):
        self.buf.append((time.time(), data.hex()))

    async def send(self, hexstr, wait=1.5):
        self.buf.clear()
        await self.c.write_gatt_char(CH_WRITE, bytes.fromhex(hexstr), response=True)
        end = time.time() + wait
        while time.time() < end:
            await asyncio.sleep(0.1)
        reports = []
        for ts, h in self.buf:
            print("  <- %s" % h)
            reports.append(h)
        return reports

    async def request(self, pairs, wait=1.5):
        hexstr = frame(build_body(pairs))
        print("  -> %s" % hexstr)
        reports = await self.send(hexstr, wait)
        merged = {}
        for h in reports:
            r = parse_report(h)
            if r:
                merged.update(r)
        return merged


async def find(name):
    devs = await BleakScanner.discover(timeout=10, return_adv=True)
    best = None
    for _k, (d, adv) in devs.items():
        n = adv.local_name or d.name or ""
        if name.upper() in n.upper():
            if best is None or (adv.rssi or -999) > (best[1].rssi or -999):
                best = (d, adv)
    return best


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["scan", "info", "set", "stop", "breathe", "raw", "watch", "alarm"])
    ap.add_argument("arg", nargs="?")
    ap.add_argument("--name", default=DEV_NAME)
    ap.add_argument("--addr", default=None)
    ap.add_argument("--vib", type=int, default=None, help="震动强度 0-100")
    ap.add_argument("--vib-mode", type=int, default=None, help="震动模式 1-4")
    ap.add_argument("--suck", type=int, default=None, help="吮吸强度 0-100")
    ap.add_argument("--suck-mode", type=int, default=None, help="吮吸模式 1-4")
    ap.add_argument("--cur", type=int, default=None, help="微电流强度 0-100")
    ap.add_argument("--cur-mode", type=int, default=None, help="电流模式 1-4")
    ap.add_argument("--wait", type=float, default=1.5)
    a = ap.parse_args()

    if a.cmd == "scan":
        devs = await BleakScanner.discover(timeout=10, return_adv=True)
        for _k, (d, adv) in devs.items():
            n = adv.local_name or d.name or ""
            if a.name.upper() in n.upper():
                print("%s  %s  RSSI=%s  mfr=%s" % (d.address, n, adv.rssi,
                      {k: v.hex() for k, v in adv.manufacturer_data.items()}))
        return

    target = a.addr
    if not target:
        f = await find(a.name)
        if not f:
            print("未扫描到 %s，请确认设备已开机、未连手机 App" % a.name)
            sys.exit(1)
        target = f[0]
    async with BleakClient(target, timeout=25) as c:
        print("已连接 %s" % target)
        d = Dev(c)
        await d.open()

        if a.cmd == "info":
            r = await d.request([(0x0005, 0x01), (0x0001, 0x01), (0x0003, 0x01),
                                 (0x0007, 0x01), (0x000D, 0x01), (0x0009, 0x01)], a.wait)
            show(r)

        elif a.cmd == "set":
            pairs = []
            if a.vib is not None:
                pairs.append((0x0001, a.vib))
            if a.vib_mode is not None:
                pairs.append((0x0002, a.vib_mode))
            if a.suck is not None:
                pairs.append((0x0007, a.suck))
            if a.suck_mode is not None:
                pairs.append((0x0008, a.suck_mode))
            if a.cur is not None:
                pairs.append((0x0003, a.cur))
            if a.cur_mode is not None:
                pairs.append((0x0004, a.cur_mode))
            if not pairs:
                print("没给任何强度/模式参数")
                return
            r = await d.request(pairs, a.wait)
            show(r)

        elif a.cmd == "stop":
            # App 里的 stopBleCode = 03000111000003110000071100
            print("  -> 010100" + "03000111000003110000071100")
            await d.send("010100" + "03000111000003110000071100", a.wait)

        elif a.cmd == "breathe":
            on = (a.arg or "").lower() in ("on", "1", "true")
            # App: 01000d1100 / 01000d1101 (via sendBleCode → 前缀 010100)
            hexstr = "010100" + ("01000d1101" if on else "01000d1100")
            print("  -> %s" % hexstr)
            await d.send(hexstr, a.wait)

        elif a.cmd == "raw":
            if not a.arg:
                print("需要 hex 字符串")
                return
            await d.send(a.arg, a.wait)

        elif a.cmd == "alarm":
            # 关闭闹钟：01000CAF000900000000000000000000000000
            hexstr = "010100" + "01000CAF000900000000000000000000000000"
            print("  -> %s" % hexstr)
            await d.send(hexstr, a.wait)

        elif a.cmd == "watch":
            print("监听中，Ctrl-C 退出 …")
            while True:
                await asyncio.sleep(0.5)
                if d.buf:
                    for _ts, h in d.buf:
                        print("  <- %s" % h)
                        show(parse_report(h))
                    d.buf.clear()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
