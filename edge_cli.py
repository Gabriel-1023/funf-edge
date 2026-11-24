#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
edge_cli.py — 无界面的边控执行器（跑在终端里，适合 SSH / 后台常驻）

用法：
  # 1. 起中继（也可以连官方 wss://trex.dungeon-lab.cn/v4，那就跳过这步）
  python3 edge_cli.py relay --port 9998

  # 2. 连中继 + 连玩具，布防
  python3 edge_cli.py run --threshold 45 --vib 40 --suck 50

它会：
  - 连上 SOSEXY，下发初始强度
  - 连上 DG-LAB 中继，打印配对地址，等 APP 接入
  - 持续打印气压，越线就全停
  - 气压回落后按 --auto-rearm 决定是否重新布防

子命令：
  relay   只跑中继
  run     完整边控
  probe   只读气压（不碰玩具，用来标定阈值）
"""
import argparse
import asyncio
import time

import funf_proto as P
from edge_guard import EdgeGuard
from edge_sensor import EdgeSensor


# ------------------------------------------------------------------ 中继
def cmd_relay(a):
    import relay_v4
    import netutil
    ip = netutil.lan_ip()
    print("中继监听 %s:%d" % (a.host, a.port))
    if a.host in ("0.0.0.0", "::"):
        print("手机请连：ws://%s:%d/" % (ip, a.port))
    else:
        print("监听地址：ws://%s:%d/" % (a.host, a.port))
    relay = relay_v4.RelayV4(a.host, a.port)
    try:
        relay.start()
    except KeyboardInterrupt:
        relay.stop()


# ------------------------------------------------------------------ 气压探针
def cmd_probe(a):
    last = {"v": None}

    def on_p(p, ts, raw):
        bar_n = int(max(0, min(60, p / max(1.0, a.range) * 60)))
        print("\r%8.1f  %s%s" % (p, "█" * bar_n, " " * (60 - bar_n)), end="",
              flush=True)
        last["v"] = p

    s = EdgeSensor(a.url, on_pressure=on_p,
                   on_log=lambda m: print("\n[log] %s" % m),
                   on_state=lambda st: print("\n[state] %s" % st))
    s.start()
    print("连接 %s …" % a.url)
    try:
        shown = False
        while True:
            time.sleep(0.5)
            if s.target_id and not s.app_client_id and not shown:
                print("\n用 DG-LAB 4 APP 接入：\n  %s\n" % s.pair_url())
                shown = True
            elif s.app_client_id:
                shown = False
    except KeyboardInterrupt:
        s.stop()
        print("\n结束")


# ------------------------------------------------------------------ 完整边控
def cmd_run(a):
    sent_log = []

    def make_toy():
        """用 bleak 在独立事件循环里跑玩具，跟 GUI 里的 BleWorker 同构。"""
        from bleak import BleakClient, BleakScanner

        state = {"client": None, "loop": None, "queue": asyncio.Queue()}

        async def worker():
            state["loop"] = asyncio.get_running_loop()
            while True:
                fn, args = await state["queue"].get()
                try:
                    await fn(*args)
                except Exception as e:
                    print("[ble] 错误: %s" % e)

        async def connect():
            devs = await BleakScanner.discover(timeout=10, return_adv=True)
            hits = [(d, adv) for _k, (d, adv) in devs.items()
                    if P.DEV_NAME.upper() in
                    (adv.local_name or d.name or "").upper()]
            if not hits:
                print("[ble] 未找到 %s —— 边控仍会运行，但没有输出可停"
                      % P.DEV_NAME)
                return
            d, adv = max(hits, key=lambda x: x[1].rssi or -999)
            c = BleakClient(d.address, timeout=25)
            await c.connect()
            state["client"] = c
            print("[ble] 已连接 %s RSSI=%s" % (d.address, adv.rssi))
            # 连上之后再下发初始强度，否则扫描期间那次下发必然被丢弃
            if a.vib or a.suck or a.cur:
                frames = P.combo_frames(a.suck, a.suck_mode, a.vib, a.vib_mode,
                                        a.cur, a.cur_mode)
                print("[ble] 下发初始强度 吮吸=%d 震动=%d 电流=%d"
                      % (a.suck, a.vib, a.cur))
                await send(frames)

        async def send(frames):
            c = state["client"]
            if not c or not c.is_connected:
                print("[ble] 未连接，丢弃 %s" % frames)
                return
            for f in ([frames] if isinstance(frames, str) else frames):
                await c.write_gatt_char(P.CH_WRITE, bytes.fromhex(f),
                                        response=True)
                await asyncio.sleep(0.12)

        async def stop_all():
            c = state["client"]
            if not c or not c.is_connected:
                return
            if a.cancel_lost:
                await c.write_gatt_char(P.CH_WRITE,
                                        bytes.fromhex(P.LOST_CANCEL),
                                        response=True)
                await asyncio.sleep(0.15)
            for _ in range(3):
                await c.write_gatt_char(P.CH_WRITE,
                                        bytes.fromhex(P.STOP), response=True)
                await asyncio.sleep(0.15)
            sent_log.append(time.time())
            print("[ble] 已全停")

        def post(fn, *args):
            lp = state["loop"]
            if lp:
                lp.call_soon_threadsafe(state["queue"].put_nowait, (fn, args))

        def boot():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.create_task(worker())
            loop.create_task(connect())
            loop.run_forever()

        import threading as _th
        _th.Thread(target=boot, daemon=True).start()
        return post, send, stop_all

    post, send, stop_all = make_toy()

    # 触发前的设定，回落时用来恢复
    resume = {"frames": None}

    def snapshot_frames():
        if not (a.vib or a.suck or a.cur):
            return None
        return P.combo_frames(a.suck, a.suck_mode, a.vib, a.vib_mode,
                              a.cur, a.cur_mode)

    resume["frames"] = snapshot_frames()

    def on_trip(p, ts):
        print("\n⚠ 气压 %.1f 越过阈值 %.1f —— 全部输出停止" % (p, a.threshold))
        post(stop_all)

    def on_release():
        # 只有 auto_rearm=True 才会走到这里（撤防不会回调 on_release）
        frames = resume["frames"]
        if not frames:
            print("气压回落，恢复监视（触发前输出为 0，跳过恢复）")
            return
        print("气压回落，恢复原设定：吮吸=%d/%d 震动=%d/%d 电流=%d/%d"
              % (a.suck, a.suck_mode, a.vib, a.vib_mode, a.cur, a.cur_mode))
        post(send, frames)

    guard = EdgeGuard(threshold=a.threshold, release_delta=a.release_delta,
                      dwell=a.dwell, cooldown=a.cooldown,
                      auto_rearm=a.auto_rearm, stale_after=a.stale_after,
                      on_trip=on_trip,
                      on_release=on_release,
                      on_state=lambda s, i: print("[边控] %s" % i))

    def on_p(p, ts, raw):
        if a.verbose:
            print("  p=%.1f state=%s" % (p, guard.state))
        guard.feed(p, ts)

    sensor = EdgeSensor(a.url, on_pressure=on_p,
                        on_log=lambda m: print("[sensor] %s" % m),
                        on_state=lambda st: print("[sensor] %s" % st))
    sensor.start()

    # 等中继握手完成再布防（玩具连接是异步的，连上后自己下发初始强度）
    time.sleep(2.0)
    guard.arm()
    print("已布防，阈值 %.1f（回差 %.1f，持续 %.2fs，冷却 %.1fs 从回落后起算）"
          % (a.threshold, a.release_delta, a.dwell, a.cooldown))
    try:
        while True:
            time.sleep(0.12)
            guard.tick()
            if sensor.target_id and not sensor.app_client_id:
                if not getattr(cmd_run, "_shown", False):
                    print("等待 APP 接入：%s" % sensor.pair_url())
                    cmd_run._shown = True
            elif sensor.app_client_id:
                cmd_run._shown = False
    except KeyboardInterrupt:
        print("\n退出，执行全停")
        post(stop_all)
        time.sleep(0.6)
        sensor.stop()


def main():
    ap = argparse.ArgumentParser(description="FUNF 吮吸玩具 + 灵猫气压边控")
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("relay", help="只跑 DG-LAB V4 中继")
    r.add_argument("--host", default="0.0.0.0",
                   help="监听地址，默认 0.0.0.0 让手机能连进来")
    r.add_argument("--port", type=int, default=9998)
    r.set_defaults(fn=cmd_relay)

    p = sub.add_parser("probe", help="只读气压，用于标定阈值")
    p.add_argument("--url", default="ws://127.0.0.1:9998/")
    p.add_argument("--range", type=float, default=50,
                   help="气压量程上限（正常波动 0-40，默认 50）")
    p.set_defaults(fn=cmd_probe)

    run = sub.add_parser("run", help="完整边控")
    run.add_argument("--url", default="ws://127.0.0.1:9998/")
    run.add_argument("--threshold", type=float, default=45.0)
    run.add_argument("--release-delta", type=float, default=5.0)
    run.add_argument("--dwell", type=float, default=0.15)
    run.add_argument("--cooldown", type=float, default=3.0,
                     help="气压回落到阈值以下后，再等这么久才恢复（秒）")
    run.add_argument("--stale-after", type=float, default=3.0)
    run.add_argument("--auto-rearm", action="store_true", default=True)
    run.add_argument("--no-auto-rearm", dest="auto_rearm", action="store_false")
    run.add_argument("--cancel-lost", action="store_true", default=True)
    run.add_argument("--vib", type=int, default=40)
    run.add_argument("--vib-mode", type=int, default=1)
    run.add_argument("--suck", type=int, default=0)
    run.add_argument("--suck-mode", type=int, default=1)
    run.add_argument("--cur", type=int, default=0)
    run.add_argument("--cur-mode", type=int, default=1)
    run.add_argument("--verbose", action="store_true")
    run.set_defaults(fn=cmd_run)

    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
