#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
funf_edge_gui.py — FUNF / SOSEXY 吮吸玩具 + 灵猫(BMTR_1)气压边控台

一个窗口里同时管两件事：

  1. 蓝牙玩具（SOSEXY / FUNF 协议，走 bleak 直连）
     吮吸 / 震动 / 微电流 三路强度 + 模式，失控模式，呼吸灯，全停

  2. 灵猫气压传感器（DG-LAB 4 APP 的 BMTR_1 设备，走 V4 WebSocket）
     DG-LAB 4 APP 接入本机中继后，灵猫的 pressure 值会持续推上来；
     超过阈值就立刻把玩具的全部输出停掉 —— 这就是「边控」。

边控链路：

    灵猫 --BLE--> DG-LAB 4 APP --ws--> 中继(本机或官方) --ws--> 本程序
                                                        |
                                                        v
                                            气压 >= 阈值 -> 全停帧 -> 玩具

界面里的操作顺序就是需求里的顺序：
    ① 设阈值/布防   ② 手动开玩具   ③ 程序盯气压，到线自动全停

运行：python3 funf_edge_gui.py
依赖：pip3 install bleak
      （中继与 WebSocket 用自带 wsproto.py，无需 websockets/aiohttp）
      qrcode 可选，装了就能显示配对二维码
"""
import asyncio
import csv
import os
import queue
import threading
import time
import tkinter as tk
from tkinter import ttk

from bleak import BleakClient, BleakScanner

import funf_proto as P
import netutil
import wsproto
from edge_guard import EdgeGuard, ARMED, TRIPPED, IDLE
from edge_sensor import EdgeSensor

try:
    import objc
    from AppKit import NSApplication
    HAVE_APPKIT = True
except Exception:
    HAVE_APPKIT = False

try:
    import qrcode
    HAVE_QR = True
except Exception:
    HAVE_QR = False

# ---- 灵猫气压量程标定 ----
# 实测正常呼吸/体动在 0~40 区间波动，越线一般是 45 以上。
# 图上限取 50：阈值线落在上方约 1/4，下面 3/4 全是波动区。
PRESS_NORMAL_MAX = 40.0     # 正常上限
PRESS_PLOT_MAX = 50.0       # 图表默认上限
PRESS_THRESHOLD_DEFAULT = 45.0

# 电量轮询间隔（秒）。连接后立刻查一次，之后按这个间隔刷新。
BATT_POLL_SEC = 30

# ---- 滑杆实时下发 ----
# tick 间隔：40ms ≈ 25 次/秒。比 120ms 明显顺滑，又不会压垮 BLE 写队列。
LIVE_TICK_MS = 40
# 每 tick 强度最多变化多少格。目标跳 40 时分 4~5 次追上，
# 观感是连续过渡而不是一次跳到 40。
LIVE_STEP_MAX = 10
# 普通下发（非实时）的帧间隔。固件需要一点喘息，太快会丢帧。
FRAME_GAP_SEC = 0.12

# 中继默认监听 0.0.0.0，界面展示/二维码编码用本机局域网 IP ——
# 手机上的 DG-LAB APP 必须能路由到这个地址，127.0.0.1 在手机上指向手机自己。
LAN_IP = netutil.lan_ip()
DEFAULT_RELAY = netutil.ws_url(LAN_IP, 9998)
BG = "#16161a"
BG2 = "#1e1e24"
FG = "#e8e8ef"
ACCENT = "#c9a0ff"
COL_SUCK = "#7fd1ff"
COL_VIB = "#ff9ad5"
COL_CUR = "#ffe08a"
COL_PRESS = "#7ef0c0"
COL_WARN = "#ff8a8a"


def fmt_p(v):
    """气压统一一位小数。所有展示和配置都走这里，避免各处格式不一致。"""
    try:
        return "%.1f" % float(v)
    except (TypeError, ValueError):
        return "—"


def _install_mac_fixes(root):
    """macOS 上「点一下没反应、要点好几次」的修复（同 funf_gui.py）。"""
    if not HAVE_APPKIT:
        return
    try:
        app = NSApplication.sharedApplication()
        app.setActivationPolicy_(0)
        app.activateIgnoringOtherApps_(True)
    except Exception as e:
        print("activate skipped:", e)
    try:
        root.update_idletasks()
        root.lift()
        root.focus_force()
    except Exception:
        pass

    def _patch_views():
        try:
            for win in NSApplication.sharedApplication().windows():
                for view in (win.contentView(), win.contentView().superview()):
                    if view is None:
                        continue
                    cls = view.class_() if hasattr(view, "class_") else type(view)
                    if getattr(cls, "_dsh_accepts", False):
                        continue
                    try:
                        def _accepts(self, event):
                            return True
                        objc.class_addMethod(
                            cls, b"acceptsFirstMouse:",
                            objc.selector(_accepts, signature=b"B@:@"), b"B@:@")
                        cls._dsh_accepts = True
                    except Exception:
                        pass
        except Exception as e:
            print("view patch skipped:", e)

    root.after(200, _patch_views)
    root.after(1200, _patch_views)


# ====================================================================== BLE
class BleWorker:
    """后台线程跑 asyncio，GUI 通过队列交互（沿用原程序结构）。"""

    def __init__(self, ui_q):
        self.ui = ui_q
        self.cmd_q = queue.Queue()
        self._live_lock = threading.Lock()
        self._pending_frames = None
        self._live_posted = False
        self.loop = None
        self.client = None
        self.buf = []
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def post(self, fn, *a):
        self.cmd_q.put((fn, a))

    def post_live(self, frames):
        """投递实时帧：队列里若已有一条未执行的实时帧，直接替换成最新的。

        拖拽只关心最新值。若每次都 put，主循环一旦慢下来就会积压一串
        历史值，设备把它们逐个播完 —— 手感就是慢半拍地「咔咔」爬。
        替换而非追加，保证任何时刻最多只有一条待发的实时帧。
        """
        with self._live_lock:
            self._pending_frames = frames
            if self._live_posted:
                return
            self._live_posted = True
        self.cmd_q.put((self._drain_live, ()))

    async def _drain_live(self):
        """取出最新那批实时帧发出去（一次只发最新）。"""
        with self._live_lock:
            frames = self._pending_frames
            self._pending_frames = None
            self._live_posted = False
        if frames:
            await self.do_send_live(frames)

    def log(self, msg):
        self.ui.put(("log", msg))

    def _run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.create_task(self._pump())
        self.loop.run_forever()

    async def _pump(self):
        while True:
            try:
                fn, a = self.cmd_q.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.05)
                continue
            try:
                await fn(*a)
            except Exception as e:
                self.log("错误: %s: %s" % (type(e).__name__, e))
                self.ui.put(("state", "err"))

    def _on_notify(self, _ch, data):
        self.buf.append(data.hex())
        self.ui.put(("rx", data.hex()))

    async def do_scan(self):
        self.ui.put(("state", "scan"))
        self.log("扫描中 …")
        devs = await BleakScanner.discover(timeout=10, return_adv=True)
        hits = []
        for _k, (d, adv) in devs.items():
            n = adv.local_name or d.name or ""
            if P.DEV_NAME.upper() in n.upper():
                hits.append((d, adv))
        if not hits:
            self.log("没找到 %s，确认设备开机、且手机 App 已断开" % P.DEV_NAME)
            self.ui.put(("state", "idle"))
            return
        d, adv = max(hits, key=lambda x: x[1].rssi or -999)
        self.ui.put(("found", (d.address, adv.rssi)))
        self.log("找到 %s  RSSI=%s" % (d.address, adv.rssi))
        self.ui.put(("state", "idle"))

    async def do_connect(self, addr):
        self.ui.put(("state", "conn"))
        if self.client and self.client.is_connected:
            await self.client.disconnect()
        self.client = BleakClient(
            addr, timeout=25,
            disconnected_callback=lambda c: self.ui.put(("state", "disc")))
        await self.client.connect()
        await self.client.start_notify(P.CH_NOTIFY, self._on_notify)
        self.log("已连接 %s" % addr)
        self.ui.put(("state", "on"))

        # 连上先查一次，再起 30 秒轮询；断开时由 do_disconnect 取消
        self._start_battery_poll()

    def _start_battery_poll(self):
        """连接成功后自动查电量：立刻一次 + 每 30 秒一次。"""
        self._stop_battery_poll()
        self._batt_task = asyncio.ensure_future(self._battery_loop())

    def _stop_battery_poll(self):
        t = getattr(self, "_batt_task", None)
        if t and not t.done():
            t.cancel()
        self._batt_task = None

    async def _battery_loop(self):
        while True:
            if not (self.client and self.client.is_connected):
                return
            try:
                await self.client.write_gatt_char(
                    P.CH_WRITE, bytes.fromhex(P.QUERY_BATT), response=True)
            except Exception as e:
                self.log("查电量失败: %s" % e)
                return
            await asyncio.sleep(BATT_POLL_SEC)

    async def do_disconnect(self):
        self._stop_battery_poll()
        if self.client and self.client.is_connected:
            await self.client.disconnect()
        self.log("已断开")
        self.ui.put(("state", "idle"))

    async def do_send(self, frames, wait=0.3):
        """普通下发：逐帧写，帧间留一点间隔（固件需要）。"""
        if not (self.client and self.client.is_connected):
            self.log("未连接，指令丢弃")
            return
        if isinstance(frames, str):
            frames = [frames]
        for f in frames:
            await self.client.write_gatt_char(
                P.CH_WRITE, bytes.fromhex(f), response=True)
            self.log("-> %s" % f)
            await asyncio.sleep(FRAME_GAP_SEC)
        await asyncio.sleep(wait)

    async def do_send_live(self, frames):
        """实时下发：不排队、不 sleep，直接写。

        拖拽时的值只关心「最新」，历史中间值没有回放价值 —— 排队反而会让
        设备慢半拍地重播刚才的轨迹（用户感觉到的「咔咔咔」）。
        所以这里连续写、只在必要时让出一次事件循环。
        """
        if not (self.client and self.client.is_connected):
            return
        if isinstance(frames, str):
            frames = [frames]
        for f in frames:
            try:
                await self.client.write_gatt_char(
                    P.CH_WRITE, bytes.fromhex(f), response=True)
            except Exception as e:
                self.log("实时下发失败: %s" % e)
                return
        await asyncio.sleep(0)      # 让出控制权，但不引入延迟

    async def do_emergency_stop(self, repeats=3, cancel_lost=False):
        """边控触发时调用：取消失控（可选）+ 连发全停帧。"""
        if not (self.client and self.client.is_connected):
            self.log("边控触发，但蓝牙未连接 —— 无输出可停")
            return
        if cancel_lost:
            await self.client.write_gatt_char(
                P.CH_WRITE, bytes.fromhex(P.LOST_CANCEL), response=True)
            self.log("-> %s (取消失控)" % P.LOST_CANCEL)
            await asyncio.sleep(0.15)
        for i in range(repeats):
            await self.client.write_gatt_char(
                P.CH_WRITE, bytes.fromhex(P.STOP), response=True)
            self.log("-> %s  (全停 %d/%d)" % (P.STOP, i + 1, repeats))
            await asyncio.sleep(0.15)

    async def do_cancel_lost(self):
        await self.do_send(P.LOST_CANCEL, 0.8)
        await self.do_send(P.QUERY_BATT, 0.8)


# ====================================================================== 主界面
class App:
    def __init__(self, root):
        self.root = root
        self.ui_q = queue.Queue()
        self.ble = BleWorker(self.ui_q)

        self.sensor = None
        self.guard = EdgeGuard(
            threshold=PRESS_THRESHOLD_DEFAULT, release_delta=5, dwell=0.15,
            cooldown=3.0,
            auto_rearm=True, stale_after=3.0,
            on_trip=self._on_trip,
            on_release=self._on_release,
            on_state=lambda s, info: self.ui_q.put(("gstate", (s, info))),
            on_finish=self._on_finish,
        )
        self.relay = None
        self.relay_thread = None

        self._qr_img = None           # 二维码位图引用（防 GC）
        self._target_live = {}        # 目标值 {key: (val, mode)}
        self._actual_live = {}        # 当前实际输出，用于插值逼近
        self._live_job = None         # tick 定时器
        self.last_output = None       # 触发前的三路设定快照
        self._last_guard_state = IDLE
        self.press_hist = []          # [(ts, value, source)]
        self.csv_fp = None
        self.csv_w = None
        self.csv_lock = threading.Lock()
        self._lock_pressure_ui = False

        self._build()
        self._pump_ui()
        # 二维码是给 APP 扫的，必须先有。所以启动就把中继拉起来并连上，
        # 让 hello 帧尽快回来拿到 targetId —— 不需要用户先点「连接」。
        self.root.after(300, self._autostart)

    def _autostart(self):
        """启动时自动起中继并连上，直接进入「等 APP 扫码」状态。"""
        self.start_relay()
        self.start_sensor()

    # ------------------------------------------------------------ 布局
    def _build(self):
        r = self.root
        r.title("FUNF 边控台 · 吮吸玩具 + 灵猫气压")
        r.geometry("1340x860")
        r.configure(bg=BG)
        r.minsize(1300, 820)
        _install_mac_fixes(r)

        st = ttk.Style()
        st.theme_use("clam")
        st.configure("TFrame", background=BG)
        st.configure("Card.TFrame", background=BG2)
        st.configure("TLabel", background=BG, foreground=FG)
        st.configure("Card.TLabel", background=BG2, foreground=FG)
        st.configure("H1.TLabel", font=("Helvetica", 15, "bold"), foreground=ACCENT)
        st.configure("H2.TLabel", font=("Helvetica", 12, "bold"),
                     background=BG2, foreground=ACCENT)
        st.configure("Big.TLabel", font=("Helvetica", 13, "bold"))
        st.configure("CardBig.TLabel", font=("Helvetica", 13, "bold"),
                     background=BG2, foreground=FG)
        st.configure("TCheckbutton", background=BG2, foreground=FG)
        st.configure("TButton", padding=(8, 4))
        st.configure("Card.TButton", padding=(8, 4))

        head = ttk.Frame(r, padding=(12, 10, 12, 4))
        head.pack(fill="x")
        ttk.Label(head, text="FUNF 边控台", style="H1.TLabel").pack(side="left")
        self.ble_status = tk.StringVar(value="蓝牙：未连接")
        ttk.Label(head, textvariable=self.ble_status,
                  style="Big.TLabel").pack(side="left", padx=16)
        self.guard_status = tk.StringVar(value="边控：未布防")
        self.guard_lbl = tk.Label(head, textvariable=self.guard_status,
                                  bg=BG, fg=COL_PRESS,
                                  font=("Helvetica", 13, "bold"))
        self.guard_lbl.pack(side="left", padx=8)

        # 电量：连接后自动查一次，之后每 30 秒刷新，不需要手动点
        self.batt_var = tk.StringVar(value="电量 —")
        self.batt_lbl = tk.Label(head, textvariable=self.batt_var, bg=BG,
                                 fg=COL_PRESS,
                                 font=("Helvetica", 13, "bold"))
        self.batt_lbl.pack(side="left", padx=(18, 0))

        cols = ttk.Frame(r, padding=(12, 0, 12, 8))
        cols.pack(fill="both", expand=True)
        left = ttk.Frame(cols)
        left.pack(side="left", fill="both", expand=True)
        right = ttk.Frame(cols)
        right.pack(side="left", fill="both", expand=True, padx=(10, 0))

        self._build_toy(left)
        self._build_sensor(right)
        self._build_log(left)

    # ------------------------------------------------------------ 玩具面板
    def _build_toy(self, parent):
        card = ttk.Frame(parent, style="Card.TFrame", padding=10)
        card.pack(fill="x")
        ttk.Label(card, text="① 蓝牙玩具 (SOSEXY)", style="H2.TLabel").pack(anchor="w")

        bar = ttk.Frame(card, style="Card.TFrame")
        bar.pack(fill="x", pady=(8, 4))
        self.addr_var = tk.StringVar(value="")
        ttk.Entry(bar, textvariable=self.addr_var, width=30).pack(side="left")
        ttk.Button(bar, text="扫描", command=lambda: self.ble.post(self.ble.do_scan)).pack(side="left", padx=3)
        ttk.Button(bar, text="连接", command=lambda: self.ble.post(
            self.ble.do_connect, self.addr_var.get())).pack(side="left", padx=3)
        ttk.Button(bar, text="断开", command=lambda: self.ble.post(
            self.ble.do_disconnect)).pack(side="left", padx=3)

        self.sliders, self.modes = {}, {}
        for key, label, color in (("suck", "吮吸", COL_SUCK),
                                  ("vib", "震动", COL_VIB),
                                  ("cur", "微电流", COL_CUR)):
            f = ttk.Frame(card, style="Card.TFrame")
            f.pack(fill="x", pady=6)
            ttk.Label(f, text=label, width=7, style="CardBig.TLabel").pack(side="left")
            val = tk.IntVar(value=0)
            ttk.Label(f, textvariable=val, width=4,
                      style="Card.TLabel").pack(side="left")
            # 拖动即下发：Scale 的 command 在拖动过程中持续触发，
            # 由 _queue_live_send 做节流，避免把 BLE 队列刷爆
            sc = tk.Scale(f, from_=0, to=100, orient="horizontal", variable=val,
                          showvalue=False, bg=BG2, fg=FG, troughcolor="#2b2b33",
                          highlightthickness=0, sliderrelief="flat",
                          activebackground=color, length=205,
                          command=lambda _v, k=key: self._queue_live_send(k))
            # 松开鼠标时立即补发一次最终值 —— 节流可能把端点吞掉，
            # 导致「拖到 100 但实际停在 98」
            sc.bind("<ButtonRelease-1>",
                    lambda _e, k=key: self._flush_now(k))
            sc.pack(side="left", padx=6)
            m = tk.StringVar(value="1")
            ttk.Combobox(f, width=3, state="readonly", values=["1", "2", "3", "4"],
                         textvariable=m).pack(side="left", padx=3)
            # 模式变化也立即下发
            m.trace_add("write",
                        lambda *_a, k=key: self._queue_live_send(k))
            ttk.Label(f, text="模式", style="Card.TLabel").pack(side="left")
            self.sliders[key] = val
            self.modes[key] = m

        btns = ttk.Frame(card, style="Card.TFrame")
        btns.pack(fill="x", pady=(8, 2))
        ttk.Button(btns, text="全部停止",
                   command=self.stop_all).pack(side="left", padx=3)
        ttk.Button(btns, text="取消失控",
                   command=lambda: self.ble.post(self.ble.do_cancel_lost)).pack(side="left", padx=3)

        self.breathe_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(btns, text="呼吸灯", variable=self.breathe_var,
                        command=self.toggle_breathe).pack(side="left", padx=8)

        lost = ttk.Frame(card, style="Card.TFrame")
        lost.pack(fill="x", pady=(6, 0))
        ttk.Label(lost, text="失控模式", style="CardBig.TLabel").pack(side="left")
        self.lost_min = tk.IntVar(value=5)
        ttk.Spinbox(lost, from_=1, to=60, width=5,
                    textvariable=self.lost_min).pack(side="left", padx=6)
        ttk.Label(lost, text="分钟", style="Card.TLabel").pack(side="left")
        self.cb_s = tk.BooleanVar(value=True)
        self.cb_v = tk.BooleanVar(value=True)
        self.cb_c = tk.BooleanVar(value=True)
        for text, v in (("吮吸", self.cb_s), ("震动", self.cb_v), ("电流", self.cb_c)):
            ttk.Checkbutton(lost, text=text, variable=v).pack(side="left", padx=4)
        ttk.Button(lost, text="开始", command=self.lost_start).pack(side="left", padx=6)
        ttk.Button(lost, text="取消", command=self.lost_stop).pack(side="left")

    # ------------------------------------------------------------ 传感器面板
    def _build_sensor(self, parent):
        card = ttk.Frame(parent, style="Card.TFrame", padding=10)
        card.pack(fill="x")
        ttk.Label(card, text="② 灵猫气压 (BMTR_1 · DG-LAB V4)",
                  style="H2.TLabel").pack(anchor="w")

        # ---- 配对信息：一条链接 + 二维码，按钮紧贴输入框 ----
        # 布局要点：右侧二维码 240px 高会把容器撑高。若左侧三行直接用 grid
        # 放在同一容器里，Tk 会把多余高度平均分给各行 —— 表现为行间空出
        # 一大片。解法：左侧三行装进一个子 frame 用 pack 紧密堆叠，
        # 该 frame 以 sticky="nw" 贴左上角，自身高度按内容收缩。
        self.relay_var = tk.StringVar(value=DEFAULT_RELAY)   # 内部状态，不显示
        pair = ttk.Frame(card, style="Card.TFrame")
        pair.pack(fill="x", pady=(8, 4))
        pair.columnconfigure(0, weight=1)

        left = ttk.Frame(pair, style="Card.TFrame")
        left.grid(row=0, column=0, sticky="nw")

        # 状态行：靠上
        self.pair_state = tk.StringVar(value="中继启动中…")
        ttk.Label(left, textvariable=self.pair_state,
                  style="CardBig.TLabel", anchor="w").pack(
                      side="top", anchor="w", fill="x")

        # 完整配对地址（局域网 IP + tid），只读但可选中复制
        self.pair_url_var = tk.StringVar(value="")
        self.pair_entry = ttk.Entry(left, textvariable=self.pair_url_var,
                                    state="readonly", width=30)
        self.pair_entry.pack(side="top", anchor="w", fill="x", pady=(4, 0))
        self.pair_entry.bind("<FocusIn>",
                             lambda e: self.pair_entry.selection_range(0, "end"))

        # 中继控制：紧贴地址框下方
        row = ttk.Frame(left, style="Card.TFrame")
        row.pack(side="top", anchor="w", pady=(6, 0))
        self.btn_relay = ttk.Button(row, text="启动中继", width=9,
                                    command=self.toggle_relay)
        self.btn_relay.pack(side="left", padx=(0, 4))
        self.btn_conn = ttk.Button(row, text="重连", width=6,
                                   command=self.reconnect_sensor)
        self.btn_conn.pack(side="left", padx=(0, 4))
        ttk.Button(row, text="断开", width=6,
                   command=self.stop_sensor).pack(side="left", padx=(0, 4))
        ttk.Button(row, text="↻", width=3,
                   command=self.refresh_lan_addr).pack(side="left")

        # 二维码：靠右上对齐
        self.qr_canvas = tk.Canvas(pair, width=self.QR_BOX,
                                   height=self.QR_BOX, bg=BG2,
                                   highlightthickness=0)
        self.qr_canvas.grid(row=0, column=1, sticky="ne", padx=(10, 0))

        # 气压显示
        disp = ttk.Frame(card, style="Card.TFrame")
        disp.pack(fill="x", pady=(6, 2))
        self.press_var = tk.StringVar(value="—")
        self.press_lbl = tk.Label(disp, textvariable=self.press_var, bg=BG2, fg=COL_PRESS,
                                  font=("Menlo", 34, "bold"))
        self.press_lbl.pack(side="left")
        ttk.Label(disp, text="气压", style="CardBig.TLabel").pack(side="left", padx=(4, 12))
        self.press_info = tk.StringVar(value="未连接")
        ttk.Label(disp, textvariable=self.press_info,
                  style="Card.TLabel").pack(side="left")

        self.plot = tk.Canvas(card, height=190, bg="#101014",
                              highlightthickness=0)
        self.plot.pack(fill="x", pady=6)

        # 阈值参数
        par = ttk.Frame(card, style="Card.TFrame")
        par.pack(fill="x", pady=(4, 0))
        ttk.Label(par, text="阈值", style="CardBig.TLabel").pack(side="left")
        self.thr_var = tk.DoubleVar(value=PRESS_THRESHOLD_DEFAULT)
        self.thr_entry = ttk.Entry(par, textvariable=self.thr_var, width=6)
        self.thr_entry.pack(side="left", padx=4)

        ttk.Label(par, text="回差", style="Card.TLabel").pack(side="left", padx=(8, 0))
        self.rel_delta_var = tk.DoubleVar(value=5)
        self.rel_entry = ttk.Entry(par, textvariable=self.rel_delta_var, width=4)
        self.rel_entry.pack(side="left", padx=3)

        ttk.Label(par, text="持续", style="Card.TLabel").pack(side="left", padx=(8, 0))
        self.dwell_var = tk.DoubleVar(value=0.15)
        self.dwell_entry = ttk.Entry(par, textvariable=self.dwell_var, width=4)
        self.dwell_entry.pack(side="left", padx=3)
        ttk.Label(par, text="s", style="Card.TLabel").pack(side="left")

        ttk.Label(par, text="冷却", style="Card.TLabel").pack(side="left", padx=(8, 0))
        self.cool_var = tk.DoubleVar(value=3.0)
        self.cool_entry = ttk.Entry(par, textvariable=self.cool_var, width=4)
        self.cool_entry.pack(side="left", padx=3)
        ttk.Label(par, text="s", style="Card.TLabel").pack(side="left")

        # 四个参数框必须都绑定 —— 之前只绑了阈值，改冷静/回差/持续按回车
        # 根本没传进状态机，看起来就是「冷静不生效」。
        for ent in (self.thr_entry, self.rel_entry,
                    self.dwell_entry, self.cool_entry):
            ent.bind("<Return>", self._on_param_enter)
            ent.bind("<FocusOut>", self._on_param_enter)
            ent.bind("<KP_Enter>", self._on_param_enter)

        par2 = ttk.Frame(card, style="Card.TFrame")
        par2.pack(fill="x", pady=(4, 2))
        # 量程：默认自动。灵猫气压量程因设备而异，固定 1000 会把真实波动
        # 压成底部一条线，所以按「阈值 + 数据」自适应，需要时也能手动锁定。
        self.auto_range_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(par2, text="量程自适应",
                        variable=self.auto_range_var,
                        command=self._on_auto_range).pack(side="left", padx=(0, 4))
        self.range_var = tk.DoubleVar(value=PRESS_PLOT_MAX)
        self.range_spin = ttk.Spinbox(par2, from_=10, to=10000, increment=10,
                                      width=6, textvariable=self.range_var,
                                      command=self._redraw_plot)
        self.range_spin.pack(side="left", padx=2)
        self.range_spin.state(["disabled"])
        self.auto_rearm_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(par2, text="回落自动重布防",
                        variable=self.auto_rearm_var,
                        command=self._sync_guard_params).pack(side="left", padx=6)
        self.cancel_lost_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(par2, text="触发时取消失控",
                        variable=self.cancel_lost_var).pack(side="left", padx=4)

        act = ttk.Frame(card, style="Card.TFrame")
        act.pack(fill="x", pady=(8, 2))
        self.btn_arm = ttk.Button(act, text="③ 布防（开始边控）",
                                  command=self.toggle_arm)
        self.btn_arm.pack(side="left", padx=3)
        ttk.Button(act, text="手动全停", command=self.stop_all).pack(side="left", padx=3)
        self.trip_var = tk.StringVar(value="触发 0 次")
        ttk.Label(act, textvariable=self.trip_var,
                  style="Card.TLabel").pack(side="left", padx=10)

        self.csv_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(act, text="记录气压 CSV", variable=self.csv_var,
                        command=self._toggle_csv).pack(side="left", padx=8)

        # ---- 触发上限 -> 自动撤防 ----
        # 语义：触发 N 次后，最后一次冷却结束时撤防并「冲刺」（输出一直保持）。
        limit = ttk.Frame(card, style="Card.TFrame")
        limit.pack(fill="x", pady=(4, 0))
        self.auto_disarm_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(limit, text="触发上限后自动撤防",
                        variable=self.auto_disarm_var,
                        command=self._on_limit_toggle).pack(side="left")
        ttk.Label(limit, text="次数", style="Card.TLabel").pack(side="left", padx=(10, 0))
        self.max_trips_var = tk.IntVar(value=5)
        self.max_trips_spin = ttk.Spinbox(limit, from_=1, to=999, width=5,
                                          textvariable=self.max_trips_var,
                                          command=self._sync_guard_params)
        self.max_trips_spin.pack(side="left", padx=4)
        self.max_trips_spin.bind("<Return>", lambda e: self._sync_guard_params())
        self.max_trips_spin.bind("<FocusOut>", lambda e: self._sync_guard_params())
        self.max_trips_spin.state(["disabled"])
        self.limit_hint = tk.StringVar(value="")
        ttk.Label(limit, textvariable=self.limit_hint,
                  style="Card.TLabel").pack(side="left", padx=(8, 0))

        ttk.Label(card, text="模拟气压（无硬件自测）", style="Card.TLabel").pack(
            anchor="w", pady=(8, 0))
        self.sim_var = tk.DoubleVar(value=0.0)
        self.sim_lbl = tk.StringVar(value=fmt_p(0.0))
        simrow = ttk.Frame(card, style="Card.TFrame")
        simrow.pack(fill="x")
        sim = tk.Scale(simrow, from_=0.0, to=PRESS_PLOT_MAX * 2,
                       resolution=0.1, orient="horizontal",
                       variable=self.sim_var, showvalue=False, bg=BG2, fg=FG,
                       troughcolor="#2b2b33", highlightthickness=0,
                       sliderrelief="flat", activebackground=COL_PRESS,
                       command=self._on_sim)
        sim.pack(side="left", fill="x", expand=True)
        ttk.Label(simrow, textvariable=self.sim_lbl, width=6,
                  style="CardBig.TLabel").pack(side="left", padx=(6, 0))
        self.sim_enable = tk.BooleanVar(value=False)
        ttk.Checkbutton(card, text="启用模拟源（覆盖真实灵猫数据）",
                        variable=self.sim_enable,
                        command=self._on_sim_toggle).pack(anchor="w")

    # ------------------------------------------------------------ 日志
    def _build_log(self, parent):
        f = ttk.Frame(parent)
        f.pack(fill="both", expand=True, pady=(10, 0))
        ttk.Label(f, text="日志", style="Big.TLabel").pack(anchor="w")
        self.txt = tk.Text(f, height=10, bg="#0e0e11", fg="#b9f5c9",
                           insertbackground="#fff", font=("Menlo", 10),
                           relief="flat", wrap="none")
        self.txt.pack(fill="both", expand=True)
        self.txt.tag_config("rx", foreground=COL_SUCK)
        self.txt.tag_config("er", foreground=COL_WARN)
        self.txt.tag_config("trip", foreground=COL_WARN,
                            font=("Menlo", 10, "bold"))
        self.txt.tag_config("ok", foreground=COL_PRESS)

    # ================================================================ 玩具动作
    def gv(self, k):
        try:
            return max(0, min(100, int(self.sliders[k].get())))
        except Exception:
            return 0

    def gm(self, k):
        try:
            return max(1, min(4, int(self.modes[k].get())))
        except Exception:
            return 1

    def _guard_blocks(self):
        """已触发时锁死输出，避免用户手动又点开。"""
        if self.guard.state == TRIPPED:
            self.log("边控已触发：输出锁定中。先「撤防」或等气压回落重新布防。", "er")
            return True
        return False

    def _queue_live_send(self, key):
        """滑杆/模式变化：只记录目标值，由发送循环负责平滑逼近。

        关键设计：**不排队历史值**。
        拖动 0→100 会产生上百个中间值，如果把每个都排进队列逐个发，
        设备会以固定间隔把刚才的轨迹重播一遍 —— 手感就是「咔咔咔」地
        慢半拍爬上去。正确做法是只保留「当前目标」，用插值让实际输出
        快速而平滑地追上去。
        """
        if self._guard_blocks():
            return
        self._target_live[key] = (self.gv(key), self.gm(key))
        self._start_live_loop()

    def _start_live_loop(self):
        if self._live_job is None:
            self._live_job = self.root.after(LIVE_TICK_MS, self._live_tick)

    def _live_tick(self):
        """一个 tick：把「实际输出」朝「目标值」推进一步，变化了才下发。

        用固定 tick（LIVE_TICK_MS）+ 每步限幅（LIVE_STEP_MAX）实现插值：
        目标跳变 40 时不会一次写 40，而是按步长分几次逼近，观感连续。
        """
        self._live_job = None

        frames = []
        for key, (tgt, tmode) in list(self._target_live.items()):
            # 首次没有 actual 时从 0 起算 —— 否则默认等于目标值，
            # 第一帧就判定「已到位」，什么都不发。
            cur, cmode = self._actual_live.get(key, (0, tmode))
            changed = False          # 每个通道独立判断，不能共用

            if cur != tgt:
                delta = tgt - cur
                # 自适应步长：差得远时迈大步（跟得上手速），
                # 接近目标时收小步（末段不跳变）。
                if abs(delta) > LIVE_STEP_MAX * 3:
                    step = abs(delta) // 2       # 大跳变：一半一半逼近
                else:
                    step = min(abs(delta), LIVE_STEP_MAX)
                cur += step if delta > 0 else -step
                changed = True
            if cmode != tmode:
                cmode = tmode
                changed = True

            if not changed:
                continue

            self._actual_live[key] = (cur, cmode)
            if key == "suck":
                frames.append(P.f_suck(cur, cmode))
            elif key == "vib":
                frames.append(P.f_vib(cur, cmode))
            elif key == "cur":
                frames.append(P.f_cur(cur, cmode))

        if frames:
            self._record_output(dict(self._actual_live))
            self.ble.post_live(frames)

        # 还有没追上的目标就继续 tick
        pending = any(self._actual_live.get(k) != v
                      for k, v in self._target_live.items())
        if pending:
            self._live_job = self.root.after(LIVE_TICK_MS, self._live_tick)

    def _flush_now(self, key):
        """松手时：直接把目标当成实际值一次发到位，避免停在半路。"""
        if self._guard_blocks():
            return
        tgt, tmode = self.gv(key), self.gm(key)
        self._target_live[key] = (tgt, tmode)
        self._actual_live[key] = (tgt, tmode)
        if key == "suck":
            f = P.f_suck(tgt, tmode)
        elif key == "vib":
            f = P.f_vib(tgt, tmode)
        else:
            f = P.f_cur(tgt, tmode)
        self._record_output({k: v for k, v in self._actual_live.items()})
        self.ble.post_live([f])

    def stop_all(self):
        for k in self.sliders:
            self.sliders[k].set(0)
        self.ble.post(self.ble.do_send, P.STOP, 0.4)
        self.log("全部停止", "ok")

    def toggle_breathe(self):
        self.ble.post(self.ble.do_send,
                      P.BREATHE_ON if self.breathe_var.get() else P.BREATHE_OFF, 0.3)

    def lost_start(self):
        mask = ((P.LOST_VIB if self.cb_v.get() else 0) |
                (P.LOST_SUCK if self.cb_s.get() else 0) |
                (P.LOST_CUR if self.cb_c.get() else 0))
        self.ble.post(self.ble.do_send, P.lost_start(self.lost_min.get(), mask), 0.4)

    def lost_stop(self):
        self.ble.post(self.ble.do_send, P.LOST_CANCEL, 0.4)

    # ================================================================ 边控
    def _sync_guard_params(self):
        try:
            self.guard.threshold = float(self.thr_var.get())
        except Exception:
            pass
        try:
            self.guard.release_delta = max(0.0, float(self.rel_delta_var.get()))
        except Exception:
            pass
        try:
            self.guard.dwell = max(0.0, float(self.dwell_var.get()))
        except Exception:
            pass
        try:
            self.guard.cooldown = max(0.0, float(self.cool_var.get()))
        except Exception:
            pass
        self.guard.auto_rearm = bool(self.auto_rearm_var.get())
        # 触发上限：勾了自动撤防才生效，否则 0 = 不限制
        try:
            n = int(self.max_trips_var.get())
        except Exception:
            n = 0
        self.guard.max_trips = max(0, n) if self.auto_disarm_var.get() else 0
        self._redraw_plot()

    def _on_param_enter(self, _e=None):
        """四个参数框的统一起效入口：回车 / 失焦都走这里。"""
        self._clamp_params()
        self._sync_guard_params()
        self.log("边控参数：阈值 %.1f  回差 %.1f  持续 %.2fs  冷却 %.1fs（从回落后起算）  自动重布防=%s"
                 % (self.guard.threshold, self.guard.release_delta,
                    self.guard.dwell, self.guard.cooldown,
                    self.guard.auto_rearm), "ok")

    def _clamp_params(self):
        """把输入框里的值夹到合理区间并统一精度。

        气压相关的量（阈值、回差）精确到一位小数 —— 界面展示、图表标注、
        状态机内部用的都是同一个值，不能出现「框里 45.5、内部 45」这种事。
        """
        limits = (
            # (变量, 下限, 上限, 小数位)
            (self.thr_var, 0.1, 10000.0, 1),
            (self.rel_delta_var, 0.0, 1000.0, 1),
            (self.dwell_var, 0.0, 60.0, 2),
            (self.cool_var, 0.0, 600.0, 1),
        )
        for var, lo, hi, digits in limits:
            try:
                v = float(var.get())
            except Exception:
                continue
            v = round(min(hi, max(lo, v)), digits)
            var.set(v)

    def apply_threshold(self):
        self._clamp_params()
        self._sync_guard_params()
        self.log("边控参数：阈值 %.1f  回差 %.1f  持续 %.2fs  冷却 %.1fs（从回落后起算）  自动重布防=%s"
                 % (self.guard.threshold, self.guard.release_delta,
                    self.guard.dwell, self.guard.cooldown,
                    self.guard.auto_rearm), "ok")

    def toggle_arm(self):
        self._sync_guard_params()
        if self.guard.state == IDLE:
            self.guard.arm()
        else:
            self.guard.disarm()
        self._refresh_guard_ui()

    def _tripped_text(self):
        """触发后分三阶段显示，让「冷却从回落后才算」这件事可见。"""
        # 数据断了：状态机不敢判断，显示实情而不是「剩余 0.0s」这种假象
        if time.time() - self.guard.last_ts > self.guard.stale_after:
            return "边控：已触发 · 气压数据中断"
        if self.guard.release_since is None:
            return "边控：已触发 · 等气压回落至 " + fmt_p(self.guard.release_level)
        left = self.guard.cooldown - (time.time() - self.guard.release_since)
        if left <= 0:
            return "边控：冷却结束，准备恢复"
        return "边控：冷却中 剩余 %.1fs" % left

    def _refresh_guard_ui(self):
        s = self.guard.state
        if s == ARMED:
            self.guard_status.set("边控：监视中 (阈值 " + fmt_p(self.guard.threshold) + ")")
            self.guard_lbl.configure(fg=COL_PRESS)
            self.btn_arm.configure(text="撤防")
        elif s == TRIPPED:
            self.guard_status.set(self._tripped_text())
            self.guard_lbl.configure(fg=COL_WARN)
            self.btn_arm.configure(text="撤防（解除锁定）")
        else:
            self.guard_status.set("边控：未布防")
            self.guard_lbl.configure(fg="#9a9aa8")
            self.btn_arm.configure(text="③ 布防（开始边控）")
        self.trip_var.set("触发 %d 次" % self.guard.trip_count)

    def _record_output(self, pending=None):
        """记录「用户设定」的三路强度+模式。

        这是恢复/冲刺时用的**唯一可信来源**：在下发的那一刻记录，而不是
        等触发时去读滑杆 —— 触发时滑杆可能已被归零（异步投递），读到的
        就是全 0，冲刺会误判「没开过」。

        pending 是节流批里刚下发的 {key: (val, mode)}，优先用它，
        这样记录的就是「真正发出去的值」。
        """
        snap = {k: (self.gv(k), self.gm(k)) for k in ("suck", "vib", "cur")}
        if pending:
            snap.update(pending)
        self.last_output = snap
        return snap

    def _on_trip(self, pressure, ts):
        """气压越线：立刻停掉全部输出。"""
        # 快照在下发时已经记好了（_record_output），这里只取用
        snap = getattr(self, "last_output", None)
        self.ui_q.put(("trip", (pressure, ts, snap)))
        self.ble.post(self.ble.do_emergency_stop, 3, self.cancel_lost_var.get())
        # 滑杆归零，避免界面与实际不一致
        self.ui_q.put(("zero_sliders", None))

    def _on_release(self):
        self.ui_q.put(("release", None))

    def _apply_snapshot(self, snap, reason):
        """把快照里的强度+模式重新下发，并让滑杆回到原值。"""
        if not snap:
            self.log("%s：没有可恢复的设定，跳过" % reason)
            return False
        # 三路全 0 说明触发前就没开，不必发帧
        if all(v == 0 for v, _m in snap.values()):
            self.log("%s：触发前输出为 0，跳过" % reason)
            return False

        for k, (v, m) in snap.items():
            self.sliders[k].set(v)
            self.modes[k].set(str(m))
        self.last_output = dict(snap)

        frames = P.combo_frames(snap["suck"][0], snap["suck"][1],
                                snap["vib"][0], snap["vib"][1],
                                snap["cur"][0], snap["cur"][1])
        self.ble.post(self.ble.do_send, frames, 0.3)
        self.log("%s：吮吸=%d/%d 震动=%d/%d 电流=%d/%d"
                 % (reason, snap["suck"][0], snap["suck"][1],
                    snap["vib"][0], snap["vib"][1],
                    snap["cur"][0], snap["cur"][1]), "ok")
        return True

    def _resume_output(self):
        """回落重新布防时，把触发前的强度+模式重新下发。"""
        self._apply_snapshot(getattr(self, "last_output", None), "回落恢复")

    def _on_finish(self, trips):
        """触发次数达到上限、最后一次冷却结束：撤防并冲刺。

        用户要的语义是「最后冲刺」：冷却结束后不再停，输出一直保持。
        所以这里恢复原设定后就把状态机彻底关掉（state=idle 由 guard 负责），
        之后气压再高也不会打断。
        """
        self.ui_q.put(("finish", trips))

    def _feed_pressure(self, value, ts, source="real"):
        """所有气压采样（真实 / 模拟）都从这里进。

        本方法运行在传感器线程；press_hist 与 CSV 都要和 GUI 线程互斥，
        否则绘图切片或切换记录时会撞上正在写入的 writer。

        source 用来区分数据来源。模拟数据绝不能留在图表里冒充真实读数 ——
        所以每次切换模拟源都会清空历史（见 _on_sim_toggle）。
        """
        # 气压统一一位小数：图表、CSV、状态机看到的是同一个精度
        try:
            value = round(float(value), 1)
        except (TypeError, ValueError):
            return
        with self.csv_lock:
            self.press_hist.append((ts, value, source))
            if len(self.press_hist) > 600:
                del self.press_hist[:200]
            if self.csv_w:
                try:
                    self.csv_w.writerow([time.strftime("%Y-%m-%d %H:%M:%S",
                                                       time.localtime(ts)),
                                         fmt_p(value), self.guard.state,
                                         source])
                except Exception:
                    pass
        if self.guard.state != IDLE:
            self.guard.feed(value, ts)

    def _on_sim(self, _v=None):
        self.sim_lbl.set(fmt_p(self.sim_var.get()))
        if not self.sim_enable.get():
            return
        self._feed_pressure(round(float(self.sim_var.get()), 1),
                            time.time(), "sim")

    def _on_sim_toggle(self):
        """切换模拟源时清空历史，避免模拟数据残留在真实曲线里。"""
        src = "模拟" if self.sim_enable.get() else "真实灵猫"
        with self.csv_lock:
            n = len(self.press_hist)
            self.press_hist.clear()
        # 状态机也要重置计时，否则旧数据会干扰 dwell / cooldown 判断
        self.guard.above_since = None
        self.guard.last_pressure = None
        self.log("气压数据源切换为「%s」，已清空 %d 个历史采样" % (src, n), "ok")
        if not self.sim_enable.get() and self.sensor:
            # 回到真实源：等下一帧真实数据即可
            self.press_info.set("等待灵猫数据…")
        self._redraw_plot()
    def _toggle_csv(self):
        if self.csv_var.get():
            os.makedirs("logs", exist_ok=True)
            path = os.path.join("logs", "pressure_%s.csv" %
                                time.strftime("%Y%m%d_%H%M%S"))
            with self.csv_lock:
                self.csv_fp = open(path, "w", newline="", encoding="utf-8")
                self.csv_w = csv.writer(self.csv_fp)
                self.csv_w.writerow(["time", "pressure", "guard_state", "source"])
            self.log("气压记录 -> %s" % path, "ok")
        else:
            with self.csv_lock:
                try:
                    if self.csv_fp:
                        self.csv_fp.flush()
                        self.csv_fp.close()
                except Exception:
                    pass
                self.csv_fp = None
                self.csv_w = None
            self.log("气压记录已关闭")

    # ------------------------------------------------------------ 中继
    def _pair_status_text(self):
        """配对区顶部那行状态，代替原来两条冗余地址。"""
        s = self.sensor
        if not self.relay:
            return "中继未启动"
        if not s or not s.target_id:
            return "中继已启动，等待配对 ID…"
        if s.app_client_id:
            if s.pressure_slot:
                return "灵猫已连接"
            return "APP 已接入，等待灵猫"
        return "用 DG-LAB 4 APP 扫右侧二维码"

    def refresh_lan_addr(self):
        """重新探测局域网 IP 并刷新配对地址 / 二维码。"""
        global LAN_IP
        ip = netutil.lan_ip()
        LAN_IP = ip
        port = netutil.port_of(self.relay_var.get(), 9998)
        self.relay_var.set(netutil.ws_url(ip, port))
        self.log("局域网地址已刷新：%s" % self.relay_var.get(), "ok")
        self._update_pair()

    def start_relay(self):
        if self.relay:
            self.log("本地中继已在运行")
            return
        port = netutil.port_of(self.relay_var.get(), 9998)
        try:
            import relay_v4
            # 监听 0.0.0.0：手机上的 DG-LAB APP 要跨设备连进来
            self.relay, self.relay_thread = relay_v4.serve_in_thread("0.0.0.0", port)
            self.btn_relay.configure(text="停止中继")
            self.log("本地中继已启动，监听 0.0.0.0:%d" % port, "ok")
            self.log("手机请连：%s" % netutil.ws_url(LAN_IP, port), "ok")
            # 地址栏若还是回环，自动换成局域网地址，免得二维码编码错地址
            if netutil.is_local_url(self.relay_var.get()):
                self.relay_var.set(netutil.ws_url(LAN_IP, port))
                self.log("已把中继地址改为局域网地址 %s" % self.relay_var.get(), "ok")
        except OSError as e:
            self.relay = None
            self.relay_thread = None
            self.btn_relay.configure(text="启动中继")
            if getattr(e, "errno", None) == 48:
                self.log("中继启动失败：端口 %d 已被占用。"
                         "换一个端口，或先关掉占用它的程序" % port, "er")
            else:
                self.log("中继启动失败：%s" % e, "er")
        except Exception as e:
            self.relay = None
            self.relay_thread = None
            self.btn_relay.configure(text="启动中继")
            self.log("中继启动失败：%s" % e, "er")

    def toggle_relay(self):
        if self.relay:
            self.stop_relay()
        else:
            self.start_relay()

    def stop_relay(self):
        if not self.relay:
            return
        try:
            self.relay.stop()
        except Exception:
            pass
        self.relay = None
        self.relay_thread = None
        self.btn_relay.configure(text="启动中继")
        self.log("本地中继已停止")

    def reconnect_sensor(self):
        """断开再连，用于 IP 变化或中继重启后重新拿 targetId。"""
        self.stop_sensor()
        self.root.after(200, self.start_sensor)

    # ------------------------------------------------------------ 传感器
    def start_sensor(self):
        if self.sensor:
            self.log("灵猫连接已存在")
            return
        url = self.relay_var.get().strip()
        if not url:
            self.log("先填中继地址", "er")
            return
        # 地址指向本机就必须有本地中继在跑，否则连了也是白等
        if not self.relay and netutil.is_self_url(url):
            self.log("本地中继没起来，先点「启动中继」", "er")
            return

        # 连中继：本地中继就走回环（不依赖局域网是否通）。
        # 二维码：必须编码手机能访问到的地址。
        conn_url = url
        public_url = url
        if self.relay:
            port = netutil.port_of(url, 9998)
            conn_url = "ws://127.0.0.1:%d/" % port
            public_url = netutil.ws_url(netutil.lan_ip(), port)

        self.sensor = EdgeSensor(
            conn_url, public_url=public_url,
            on_pressure=lambda p, ts, raw: self._feed_pressure(p, ts),
            on_state=lambda s: self.ui_q.put(("sstate", s)),
            on_log=lambda m: self.ui_q.put(("log", m)),
            on_devices=lambda d: self.ui_q.put(("devices", d)),
        )
        self.sensor.start()
        self.log("正在连接中继 %s（二维码地址 %s）" % (conn_url, public_url))

    def stop_sensor(self):
        if self.sensor:
            self.sensor.stop()
            self.sensor = None
        self.pair_url_var.set("")
        self.pair_state.set("中继未启动" if not self.relay else "已断开")
        self.qr_canvas.delete("all")
        self.press_info.set("未连接")
        self.log("灵猫连接已关闭")

    # ------------------------------------------------------------ 绘图
    def _on_limit_toggle(self):
        if self.auto_disarm_var.get():
            self.max_trips_spin.state(["!disabled"])
        else:
            self.max_trips_spin.state(["disabled"])
        self._sync_guard_params()
        self._update_limit_hint()

    def _update_limit_hint(self):
        if not self.auto_disarm_var.get():
            self.limit_hint.set("")
            return
        try:
            n = int(self.max_trips_var.get())
        except Exception:
            n = 0
        if n <= 0:
            self.limit_hint.set("")
            return
        left = max(0, n - self.guard.trip_count)
        if self.guard.finished:
            self.limit_hint.set("已撤防 · 冲刺中")
        elif left == 0:
            self.limit_hint.set("最后一次冷却后撤防")
        else:
            self.limit_hint.set("还差 %d 次" % left)

    def _on_auto_range(self):
        if self.auto_range_var.get():
            self.range_spin.state(["disabled"])
        else:
            self.range_spin.state(["!disabled"])

    def _plot_scale(self):
        """算 Y 轴上限。

        标定依据：灵猫正常波动在 0~40，越线一般 45 以上，所以默认上限 50。
        这样阈值线落在约 80% 高度 —— 上方留一点空间看「越线尖峰」，
        下面 80% 全是波动区，曲线不会挤成一条线。

        只有数据明显超过 50 时才扩展上限（避免曲线顶到天花板被削平）。
        """
        if not self.auto_range_var.get():
            try:
                return max(5.0, float(self.range_var.get()))
            except Exception:
                pass

        with self.csv_lock:
            recent = [h[1] for h in self.press_hist[-400:]]
        peak = max(recent) if recent else 0.0

        top = PRESS_PLOT_MAX
        # 数据或阈值超出默认上限才扩展，留 15% 余量
        need = max(peak, self.guard.threshold)
        if need > top:
            top = need * 1.15
        return max(5.0, top)

    @staticmethod
    def _over_segments(hist, thr, t0, span, w, y_of):
        """把「连续高于阈值」的采样切成若干段，每段一条折线。

        段的首尾要精确落在阈值线上（线性插值），这样红线不会斜着穿出
        阈值线外，视觉上也不会把两次独立的触发连成一整条。
        """
        def px(ts):
            return (ts - t0) / span * (w - 10) + 5

        def cross(t_a, v_a, t_b, v_b):
            """a、b 之间与阈值线的交点时间。"""
            if v_b == v_a:
                return t_b
            k = (thr - v_a) / (v_b - v_a)
            return t_a + (t_b - t_a) * k

        segs = []
        cur = []
        for i, (ts, v, *_rest) in enumerate(hist):
            if v >= thr:
                if not cur and i > 0:
                    # 刚跨过阈值：补一个精确的入点
                    pts, pv = hist[i - 1][0], hist[i - 1][1]
                    cur.extend([px(cross(pts, pv, ts, v)), y_of(thr)])
                cur.extend([px(ts), y_of(v)])
            else:
                if cur:
                    # 刚跌回阈值下：补一个精确的出点后收段
                    pts, pv = hist[i - 1][0], hist[i - 1][1]
                    cur.extend([px(cross(pts, pv, ts, v)), y_of(thr)])
                    segs.append(cur)
                    cur = []
        if cur:
            segs.append(cur)
        return segs

    @staticmethod
    def _nice_ticks(top):
        """把 0..top 分成 4 段，步进取整成 1/2/5/10/20/25/50 这类好读的数。"""
        # 让刻度落在 4~5 段之间，且顶部那一档必须能画出来。
        # 用 top/5 而不是 top/4 作下限，否则 top=50 会选到 20，只标出 0/20/40，
        # 顶部的 50 反而不见了。
        need = top / 5.0
        for step in (1, 2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000):
            if need <= step:
                break
        ticks = []
        v = 0.0
        while v <= top + 1e-9:
            ticks.append(v)
            v += step
        return ticks

    def _redraw_plot(self):
        c = self.plot
        c.delete("all")
        w = max(c.winfo_width(), 240)
        h = max(c.winfo_height(), 120)

        top = self._plot_scale()
        pad_t, pad_b = 14, 18

        def y_of(v):
            v = max(0.0, min(top, float(v)))
            return h - pad_b - (v / top) * (h - pad_t - pad_b)

        # ---- 网格与刻度：按 5 / 10 / 20 / 25 / 50 的整齐步进取值 ----
        for v in self._nice_ticks(top):
            y = y_of(v)
            c.create_line(0, y, w, y, fill="#1e1e26")
            c.create_text(w - 4, y - 7, text=fmt_p(v), fill="#4a4a58",
                          anchor="e", font=("Menlo", 8))

        # ---- 阈值线（红）与回差线（灰）----
        thr = self.guard.threshold
        rel = self.guard.release_level
        y_thr = y_of(thr)
        y_rel = y_of(rel)
        c.create_line(0, y_thr, w, y_thr, fill=COL_WARN, width=2, dash=(6, 4))
        c.create_text(6, y_thr - 9, text="阈值 " + fmt_p(thr), fill=COL_WARN,
                      anchor="w", font=("Menlo", 9, "bold"))
        # 阈值线右侧再标一次，数据曲线挡住左侧时也能看清
        if y_rel < h - pad_b - 1 and y_rel - y_thr > 12:
            c.create_line(0, y_rel, w, y_rel, fill="#6a6a80", dash=(2, 5))
            c.create_text(6, y_rel - 9, text="回落 " + fmt_p(rel), fill="#6a6a80",
                          anchor="w", font=("Menlo", 8))

        # ---- 曲线 ----
        with self.csv_lock:
            hist = list(self.press_hist[-300:])

        if len(hist) >= 2:
            t0, t1 = hist[0][0], hist[-1][0]
            span = max(1e-6, t1 - t0)
            is_sim = hist[-1][2] == "sim" if len(hist[-1]) > 2 else False
            line_col = COL_CUR if is_sim else COL_PRESS

            pts = []
            for smp in hist:
                x = (smp[0] - t0) / span * (w - 10) + 5
                pts.extend([x, y_of(smp[1])])
            c.create_line(*pts, fill=line_col, width=2, smooth=False)

            # 阈值以上的部分单独描红。必须按「连续段」分组画 ——
            # 把所有越线点塞进一条线里，中间低于阈值的空档会被直线跨过去，
            # 看起来就像两次触发之间连了一条红线。
            for seg in self._over_segments(hist, thr, t0, span, w, y_of):
                if len(seg) >= 4:
                    c.create_line(*seg, fill=COL_WARN, width=3)

            if is_sim:
                c.create_text(w - 6, 8, text="模拟", fill=COL_CUR, anchor="ne",
                              font=("Helvetica", 9, "bold"))
            last = hist[-1][1]
            c.create_text(w - 6, h - 4, text=fmt_p(last), fill=line_col,
                          anchor="se", font=("Menlo", 11, "bold"))
        else:
            c.create_text(w // 2, h // 2, text="等待气压数据…", fill="#4a4a58",
                          font=("Helvetica", 10))

    # 二维码画布边长（px）。
    # 实测每个模块至少 4px 才能稳定扫出：3px 时解码器在 53 模块上会失败。
    # 真实 payload 固定 49 模块（含 4 模块静区），240px 下正好 scale=4。
    QR_BOX = 240

    def _draw_qr(self, payload):
        """把二维码画成一张位图。

        三个坑都在这里踩过，缺一个都扫不出来：
          1. **颜色映射**：qrcode 的矩阵里 True = 深色模块，必须画黑；
             写反了就是白底白码（或者黑底黑码），看起来有图案但永远扫不出。
          2. **静区**：扫描器需要四周至少 4 模块白边，用 border=4 让矩阵自带。
          3. **尺寸**：位图必须塞得进画布，早期按 128px 画进 118px 画布，
             右侧和底部被裁掉。现在按整数倍放大并居中。
        """
        c = self.qr_canvas
        c.delete("all")
        self._qr_img = None          # 释放旧图引用
        if not payload:
            return

        if not HAVE_QR:
            c.create_text(self.QR_BOX // 2, self.QR_BOX // 2,
                          text="未安装 qrcode\npip3 install qrcode",
                          fill="#9a9aa8", width=self.QR_BOX - 10,
                          justify="center", font=("Helvetica", 9))
            return

        try:
            q = qrcode.QRCode(border=4, box_size=1,
                              error_correction=qrcode.constants.ERROR_CORRECT_M)
            q.add_data(payload)
            q.make(fit=True)
            m = q.get_matrix()
            n = len(m)                      # 已含 4 模块静区

            # 一次性写入整图（二维格式），比逐像素 put 快很多
            rows = []
            for r in range(n):
                rows.append("{" + " ".join(
                    "#000000" if m[r][col] else "#ffffff"
                    for col in range(n)) + "}")
            base = tk.PhotoImage(width=n, height=n)
            base.put(" ".join(rows), to=(0, 0))

            # 整数倍放大；倍数取「塞得进画布」的最大值
            scale = max(1, self.QR_BOX // n)
            img = base.zoom(scale, scale) if scale > 1 else base
            size = n * scale
            off = (self.QR_BOX - size) // 2

            self._qr_img = img           # 必须留引用，否则被 GC 掉变空白
            c.create_image(off, off, image=img, anchor="nw")
        except Exception as e:
            c.create_text(self.QR_BOX // 2, self.QR_BOX // 2,
                          text="QR 生成失败\n%s" % e, fill=COL_WARN,
                          width=self.QR_BOX - 10, justify="center",
                          font=("Helvetica", 8))

    # ------------------------------------------------------------ UI 泵
    def _drain_ui(self):
        try:
            for _ in range(120):
                try:
                    kind, payload = self.ui_q.get_nowait()
                except queue.Empty:
                    break
                self._handle_ui(kind, payload)
        except Exception as e:
            print("ui drain error:", e)

    def _pump_ui(self):
        # 顺序很重要：先处理已有事件 -> 推进状态机 -> 再处理状态机刚产生的事件。
        # 否则 tick 里触发的 trip / 滑杆归零要等到下一帧，界面会短暂显示不一致。
        self._drain_ui()
        self._tick_guard()
        self._drain_ui()
        # 冷却倒计时是连续的，必须每帧刷，不能只在状态变化时刷
        if self.guard.state == TRIPPED:
            self.guard_status.set(self._tripped_text())
        elif self._last_guard_state == TRIPPED:
            # 刚脱离触发态：把状态栏同步成新状态
            self._refresh_guard_ui()
        self._last_guard_state = self.guard.state
        self._refresh_pressure_display()
        self._redraw_plot()
        self.root.after(120, self._pump_ui)

    def _tick_guard(self):
        """兜底推进状态机：dwell / cooldown 到期不必等新采样。"""
        try:
            self.guard.tick()
        except Exception as e:
            print("guard tick error:", e)

    def _refresh_pressure_display(self):
        """大数字始终反映「当前生效的数据源」，不再被模拟开关卡住。"""
        if self.sim_enable.get():
            self.press_var.set(fmt_p(self.sim_var.get()))
            self.press_info.set("模拟源")
            self.press_lbl.configure(fg=COL_CUR)
            return

        s = self.sensor
        if not s:
            self.press_var.set("—")
            self.press_info.set("未连接")
            self.press_lbl.configure(fg="#9a9aa8")
            return

        p = s.last_pressure
        if p is None:
            self.press_var.set("—")
            if s.app_client_id:
                self.press_info.set("已接入，等待灵猫数据…")
            else:
                self.press_info.set("等待 APP 接入")
            self.press_lbl.configure(fg="#9a9aa8")
            return

        age = time.time() - s.last_pressure_ts
        self.press_var.set(fmt_p(p))
        if age > 3.0:
            self.press_info.set("数据 %.0fs 未更新" % age)
            self.press_lbl.configure(fg="#9a9aa8")
        else:
            self.press_info.set("灵猫在线 · %d 帧" % s.pressure_count)
            hot = self.guard.state != IDLE and p >= self.guard.threshold
            self.press_lbl.configure(fg=COL_WARN if hot else COL_PRESS)

    def _handle_ui(self, kind, payload):
        if kind == "log":
            self.log(payload)
        elif kind == "rx":
            self._pick_battery(payload)
            d = P.describe(payload)
            line = "  <- %s" % payload
            if d:
                line += "   [%s]" % d
            self.txt.insert("end", line + "\n", "rx")
            self.txt.see("end")
        elif kind == "found":
            self.addr_var.set(payload[0])
        elif kind == "state":
            self.ble_status.set({"idle": "蓝牙：未连接", "scan": "蓝牙：扫描中…",
                                 "conn": "蓝牙：连接中…", "on": "蓝牙：已连接",
                                 "disc": "蓝牙：已断开", "err": "蓝牙：错误"}[payload])
        elif kind == "trip":
            p, ts, snap = payload
            self.log("⚠ 气压 %.1f 越过阈值 %.1f —— 全部输出已停止"
                     % (p, self.guard.threshold), "trip")
            if snap and any(v for v, _m in snap.values()):
                self.log("   已记录待恢复设定：吮吸=%d 震动=%d 电流=%d"
                         % (snap["suck"][0], snap["vib"][0], snap["cur"][0]))
            self._refresh_guard_ui()
            self._update_limit_hint()
        elif kind == "finish":
            trips = payload
            self.log("★ 已达 %d 次触发上限 —— 最后一次冷却结束，撤防并保持输出"
                     % trips, "trip")
            self._apply_snapshot(getattr(self, "last_output", None), "最后冲刺")
            self._refresh_guard_ui()
            self._update_limit_hint()
        elif kind == "release":
            # 「回落自动重布防」就是回到触发前的状态：重新布防 + 恢复原强度。
            # 不设单独开关 —— 那等于同一个意思说两遍。
            self.log("气压回落，重新布防并恢复原设定", "ok")
            self._refresh_guard_ui()
            self._update_limit_hint()
            self._resume_output()
        elif kind == "zero_sliders":
            for k in self.sliders:
                self.sliders[k].set(0)
        elif kind == "gstate":
            s, info = payload
            self.log("[边控] %s" % info, "ok")
            self._refresh_guard_ui()
        elif kind == "sstate":
            if payload == "waiting":
                self.press_info.set("等待 APP 接入")
            elif payload == "attached":
                self.press_info.set("APP 已接入，等待灵猫")
            elif payload == "sensor":
                self.press_info.set("灵猫在线")
            elif payload == "connecting":
                self.press_info.set("连接中继…")
            elif payload in ("disconnected", "error"):
                self.press_info.set("中继断开")
            self._update_pair()
        elif kind == "devices":
            self._update_pair()
        elif kind == "pair":
            self.pair_url_var.set(payload)

    def _pick_battery(self, hexstr):
        """从上报帧里取电量（attr 0x0005），有就更新顶栏。"""
        try:
            rep = P.parse(hexstr)
        except Exception:
            return
        v = rep.get(P.ATTR_BATT)
        if not v:
            return
        try:
            pct = v[0]
        except (IndexError, TypeError):
            return
        self.batt_var.set("电量 %d%%" % pct)
        # 低电量变红提示
        if pct <= 20:
            self.batt_lbl.configure(fg=COL_WARN)
        elif pct <= 40:
            self.batt_lbl.configure(fg=COL_CUR)
        else:
            self.batt_lbl.configure(fg=COL_PRESS)

    def _update_pair(self):
        """配对区只展示一条完整地址（局域网 IP + tid）+ 二维码。"""
        self.pair_state.set(self._pair_status_text())
        s = self.sensor
        if not s or not s.target_id:
            self.pair_url_var.set("")
            self.qr_canvas.delete("all")
            return
        url = s.pair_url()
        self.pair_url_var.set(url)
        self._draw_qr(s.qr_payload())

    # ------------------------------------------------------------ 日志辅助
    def log(self, msg, tag=None):
        self.txt.insert("end", "[%s] %s\n" % (time.strftime("%H:%M:%S"), msg),
                        tag or ())
        self.txt.see("end")


def main():
    root = tk.Tk()
    app = App(root)

    def on_close():
        try:
            if app.sensor:
                app.sensor.stop()
            if app.relay:
                app.relay.stop()
            app.ble.post(app.ble.do_disconnect)
        except Exception:
            pass
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
