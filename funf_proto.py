#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
funf_proto.py — FUNF / SOSEXY 蓝牙玩具的帧协议（从 funf_gui.py 抽出，供 GUI 与 CLI 共用）

GATT：服务 0000ee01，写 0000ee03（with response），通知 0000ee02
下发帧：010100 + <条数 N> + [attr(4) 11 val(2)] × N
上报帧：01 <seq> 00 <N> + [attr(2) 类型(1) value(n)]
"""
DEV_NAME = "SOSEXY"
CH_WRITE = "0000ee03-0000-1000-8000-00805f9b34fb"
CH_NOTIFY = "0000ee02-0000-1000-8000-00805f9b34fb"

# attr -> 中文名
ATTR_NAME = {
    0x0001: "震动强度", 0x0002: "震动模式", 0x0003: "电流强度",
    0x0004: "电流模式", 0x0005: "电量", 0x0007: "吮吸强度",
    0x0008: "吮吸模式", 0x0009: "失控时长", 0x000A: "失控剩余",
    0x000B: "状态字", 0x000C: "闹钟", 0x000D: "呼吸灯",
}
# 类型码 -> value 字节数
TYPE_LEN = {0x11: 1, 0x12: 4, 0xAF: 30, 0x0A: 4, 0x0C: 30}

ATTR_VIB, ATTR_VIB_MODE = 0x0001, 0x0002
ATTR_CUR, ATTR_CUR_MODE = 0x0003, 0x0004
ATTR_BATT = 0x0005
ATTR_SUCK, ATTR_SUCK_MODE = 0x0007, 0x0008
ATTR_LOST_TIME, ATTR_LOST_LEFT = 0x0009, 0x000A
ATTR_LOST_MASK = 0x000B
ATTR_BREATHE = 0x000D

# 失控模式功能位（实测：bit0 震动 / bit1 吮吸 / bit2 电流）
LOST_VIB, LOST_SUCK, LOST_CUR = 0x01, 0x02, 0x04


def frame(body):
    return "010100" + body


def pairs_body(pairs):
    """[(attr, val)] -> '02 0007 11 14 0008 11 01'"""
    body = "%02x" % len(pairs)
    for attr, val in pairs:
        body += "%04x11%02x" % (attr, val & 0xFF)
    return body


def f_pair(attr, val):
    return frame(pairs_body([(attr, val)]))


def f_suck(suck, smode):
    return frame(pairs_body([(ATTR_SUCK, suck), (ATTR_SUCK_MODE, smode)]))


def f_vib(vib, vmode):
    return frame(pairs_body([(ATTR_VIB, vib), (ATTR_VIB_MODE, vmode)]))


def f_cur(cur, cmode):
    return frame(pairs_body([(ATTR_CUR, cur), (ATTR_CUR_MODE, cmode)]))


def f_combo(suck, smode, vib, vmode, cur, cmode):
    """六项合并帧（28 字节，固件不稳，仅作参考）。"""
    return frame(pairs_body([
        (ATTR_SUCK, suck), (ATTR_SUCK_MODE, smode),
        (ATTR_VIB, vib), (ATTR_VIB_MODE, vmode),
        (ATTR_CUR, cur), (ATTR_CUR_MODE, cmode),
    ]))


def f_strength(vib, cur, suck):
    return frame(pairs_body([(ATTR_VIB, vib), (ATTR_CUR, cur), (ATTR_SUCK, suck)]))


def combo_frames(suck, smode, vib, vmode, cur, cmode):
    """拆成三条 12 字节短帧，逐条下发最稳（实测固件对长帧处理不稳）。"""
    return [f_suck(suck, smode), f_vib(vib, vmode), f_cur(cur, cmode)]


def lost_start(minutes, mask):
    return frame(pairs_body([(ATTR_LOST_TIME, minutes & 0xFF),
                             (ATTR_LOST_MASK, mask & 0xFF)]))


STOP = frame("03000111000003110000071100")
LOST_CANCEL = frame(pairs_body([(ATTR_LOST_TIME, 0), (ATTR_LOST_MASK, 0)]))
BREATHE_ON = f_pair(ATTR_BREATHE, 1)
BREATHE_OFF = f_pair(ATTR_BREATHE, 0)
QUERY_BATT = f_pair(ATTR_BATT, 1)
ALARM_OFF = "010100" + "01000CAF000900000000000000000000000000"


def parse(hexstr):
    """解析一条上报帧，返回 {attr: bytes}。"""
    try:
        b = bytes.fromhex(hexstr)
    except ValueError:
        return {}
    if len(b) < 5:
        return {}
    i = 3 if b[2] == 0x00 else 2
    if i >= len(b):
        return {}
    n = b[i]
    i += 1
    out = {}
    for _ in range(n):
        if i + 3 > len(b):
            break
        attr = (b[i] << 8) | b[i + 1]
        typ = b[i + 2]
        i += 3
        ln = TYPE_LEN.get(typ, 1)
        out[attr] = b[i:i + ln]
        i += ln
    return out


def describe(hexstr):
    """把上报帧压成一行可读文本。"""
    rep = parse(hexstr)
    if not rep:
        return ""
    bits = []
    for a, v in sorted(rep.items()):
        name = ATTR_NAME.get(a, "%04X" % a)
        if len(v) == 1:
            bits.append("%s=%d" % (name, v[0]))
        elif len(v) == 4:
            bits.append("%s=%d" % (name, int.from_bytes(v, "little")))
        else:
            bits.append("%s=%s" % (name, v.hex()))
    return ", ".join(bits)
