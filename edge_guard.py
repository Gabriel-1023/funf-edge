#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
edge_guard.py — 边控（edging）状态机

职责单一：吃气压采样，输出「是否应该停」的事件。不碰 BLE，不碰 Tk。
这样它可以在没有硬件的情况下被单测覆盖。

状态机：
    IDLE     未布防。气压随便涨，什么都不做。
    ARMED    已布防，监视中。气压 >= threshold 持续 dwell 秒 -> TRIPPED
    TRIPPED  已触发。已经回调 on_trip()，玩具全停。
             必须等气压 <= release（阈值 - 回差）且冷静期 >= cooldown 秒：
               - auto_rearm=True  -> 回 ARMED，回调 on_release()（恢复输出）
               - auto_rearm=False -> 回 IDLE，**不**回调 on_release()（保持停止）

两个防抖参数的意义：
    dwell       —— 连续超阈时长，滤掉呼吸/体动造成的瞬时尖峰
    release     —— 回差（hysteresis），防止在阈值附近抖动导致疯狂开关
    stale_after —— 采样超过这么久没更新就视为数据失效，不再据此触发/恢复

线程模型：feed() 由传感器线程调用，tick() 由 GUI 线程调用，内部加锁。
回调在持锁状态下触发，因此回调必须极短且不得回调本对象（本项目里都是
「投递队列」或「投递 BLE 命令」这种非阻塞动作）。
"""
import threading
import time

IDLE = "idle"
ARMED = "armed"
TRIPPED = "tripped"


class EdgeGuard:
    def __init__(self, threshold=45.0, release_delta=5.0, dwell=0.15,
                 cooldown=3.0, auto_rearm=True, stale_after=3.0,
                 max_trips=0, on_trip=None, on_release=None, on_state=None,
                 on_finish=None):
        self.threshold = float(threshold)
        self.release_delta = float(release_delta)
        self.dwell = float(dwell)
        self.cooldown = float(cooldown)
        self.auto_rearm = bool(auto_rearm)
        # 采样超过这么久没更新就视为数据失效，不再据此触发/恢复
        self.stale_after = float(stale_after)
        # 触发次数上限。>0 时达到上限后：走完最后一次冷却 -> 撤防并「冲刺」
        # （保持输出不再停）。0 表示不限制。
        self.max_trips = int(max_trips)

        self.on_trip = on_trip or (lambda p, ts: None)
        self.on_release = on_release or (lambda: None)
        self.on_state = on_state or (lambda s, info: None)
        # 达到上限、最后一次冷却结束时回调：此时应当「一直输出、不再停」
        self.on_finish = on_finish or (lambda trips: None)

        self._lock = threading.RLock()
        self.state = IDLE
        self.above_since = None
        self.trip_time = 0.0
        # 气压「降到回落线以下」的起始时刻。冷却期从这里开始算，
        # 不是从触发时刻 —— 否则气压一直高着，冷静期会在旁边空转。
        self.release_since = None
        self.trip_count = 0
        self.finished = False
        self.last_pressure = None
        self.last_ts = 0.0
        self.peak = 0.0

    # ---------------------------------------------------------------- 控制
    def arm(self):
        with self._lock:
            self.state = ARMED
            self.above_since = None
            self.release_since = None
            self.peak = 0.0
            self.finished = False
        self._emit_state("已布防")

    def disarm(self):
        with self._lock:
            self.state = IDLE
            self.above_since = None
            self.release_since = None
        self._emit_state("已撤防")

    def toggle(self):
        if self.state == IDLE:
            self.arm()
        else:
            self.disarm()
        return self.state

    def reset_counter(self):
        self.trip_count = 0

    @property
    def release_level(self):
        return self.threshold - self.release_delta

    # ---------------------------------------------------------------- 采样
    def feed(self, pressure, ts=None):
        """喂一个气压值。返回本次产生的动作：None / 'trip' / 'release'。"""
        ts = ts or time.time()
        with self._lock:
            self.last_pressure = pressure
            self.last_ts = ts
            if self.state == ARMED and pressure >= self.threshold:
                if self.above_since is None:
                    self.above_since = ts
                self.peak = max(self.peak, pressure)
            elif pressure < self.threshold:
                self.above_since = None
        return self.tick(ts)

    def tick(self, now=None):
        """不带新采样地推进状态机。

        关键点：dwell 的到期判断不能只靠「下一条采样」，否则传感器一旦停推，
        越线就永远不会触发。GUI 每 120ms 调一次本方法兜底。
        """
        now = now or time.time()
        with self._lock:
            return self._tick_locked(now)

    def _tick_locked(self, now):
        if self.state == IDLE:
            return None

        stale = (now - self.last_ts) > self.stale_after

        if self.state == TRIPPED:
            if stale:
                return None      # 数据断了，不敢判断「已回落」

            below = (self.last_pressure is not None
                     and self.last_pressure <= self.release_level)

            if not below:
                # 还在回落线以上：冷却计时作废，等它真正降下来再重新计时
                self.release_since = None
                return None

            # 刚降下来：从这里开始算冷却
            if self.release_since is None:
                self.release_since = now
                if self.cooldown > 0:
                    self._emit_state("气压已降到 %.1f 以下，冷却 %.1fs 开始"
                                     % (self.release_level, self.cooldown))

            if (now - self.release_since) >= self.cooldown:
                self.release_since = None

                # 达到触发上限：最后一次冷却走完 -> 撤防并进入「冲刺」，
                # 输出保持开启，不再被气压打断。
                if self.max_trips > 0 and self.trip_count >= self.max_trips:
                    self.state = IDLE
                    self.finished = True
                    self._emit_state(
                        "已达 %d 次上限，撤防并保持输出" % self.max_trips)
                    self.on_finish(self.trip_count)
                    return "finish"

                if self.auto_rearm:
                    self.state = ARMED
                    self.above_since = None
                    self._emit_state("冷却结束，重新布防")
                    # 只有「重新布防」才恢复输出。auto_rearm=False 是撤防，
                    # 不该把玩具重新开起来 —— 那是用户主动关掉的。
                    self.on_release()
                    return "release"
                self.state = IDLE
                self._emit_state("冷却结束，撤防（不恢复输出）")
                return "disarm"
            return None

        # ARMED
        if stale:
            self.above_since = None
            return None
        if self.last_pressure is not None and \
                self.last_pressure >= self.threshold and \
                self.above_since is not None and \
                (now - self.above_since) >= self.dwell:
            self.state = TRIPPED
            self.trip_time = now
            self.trip_count += 1
            self._emit_state("触发！气压 %.1f >= %.1f" %
                             (self.last_pressure, self.threshold))
            self.on_trip(self.last_pressure, now)
            return "trip"
        return None

    def _emit_state(self, info):
        try:
            self.on_state(self.state, info)
        except Exception:
            pass

    # ---------------------------------------------------------------- 展示
    def status_text(self):
        if self.state == ARMED:
            return "监视中 (阈值 %.1f)" % self.threshold
        if self.state == TRIPPED:
            return "已触发 · 冷静中 (回落到 %.1f 恢复)" % self.release_level
        return "未布防"


if __name__ == "__main__":
    g = EdgeGuard(threshold=45, release_delta=5, dwell=0.1, cooldown=1.0,
                  on_trip=lambda p, t: print("TRIP", p),
                  on_state=lambda s, i: print("  state", s, i))
    g.arm()
    for v in (10, 30, 49, 55, 60, 40, 35, 20):
        print("feed", v, "->", g.feed(v))
