#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
netutil.py — 局域网地址探测

手机上的 DG-LAB 4 APP 要扫码连过来，二维码里必须是**手机能访问到的地址**，
不能是 127.0.0.1（那是手机自己）。所以中继监听 0.0.0.0，展示/编码用本机
在局域网里的 IPv4。

取 IP 的办法：UDP socket connect 一个外部地址（不真发包，只让内核选路由），
然后读本地端地址。这比解析 ifconfig 稳，也不会被 VPN / 多网卡顺序坑到
——内核会挑真正默认路由出去的那张网卡。
"""
import socket


def lan_ip(fallback="127.0.0.1"):
    """返回本机默认出口网卡的 IPv4；探测失败返回 fallback。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 8.8.8.8 只用于选路，UDP 不会真的建立连接
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        if ip and not ip.startswith("127."):
            return ip
    except OSError:
        pass
    finally:
        try:
            s.close()
        except OSError:
            pass

    # 兜底：枚举主机名解析出的地址
    try:
        for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
            if not ip.startswith("127."):
                return ip
    except OSError:
        pass
    return fallback


def is_local_url(url):
    """判断中继地址是不是指向本机（需要改写成局域网地址的那种）。"""
    if not url:
        return False
    host = url
    for pre in ("ws://", "wss://"):
        if host.startswith(pre):
            host = host[len(pre):]
            break
    host = host.split("/")[0].split("?")[0]
    if ":" in host:
        host = host.rsplit(":", 1)[0]
    return host in ("127.0.0.1", "localhost", "::1", "0.0.0.0", "")


def host_of(url):
    """从 ws(s)://host:port/path 里取出 host。"""
    host = url or ""
    for pre in ("ws://", "wss://"):
        if host.startswith(pre):
            host = host[len(pre):]
            break
    host = host.split("/")[0].split("?")[0]
    if host.startswith("["):          # IPv6 字面量 [::1]:9998
        return host.split("]")[0][1:]
    if ":" in host:
        host = host.rsplit(":", 1)[0]
    return host


def is_self_url(url):
    """地址是否指向本机（回环，或本机自己的局域网 IP）。"""
    host = host_of(url)
    if host in ("127.0.0.1", "localhost", "::1", "0.0.0.0", ""):
        return True
    try:
        if host == lan_ip():
            return True
        # 本机所有解析出的地址
        local = set(socket.gethostbyname_ex(socket.gethostname())[2])
        local.add(lan_ip())
        return host in local
    except OSError:
        return False


def ws_url(ip, port, scheme="ws"):
    return "%s://%s:%d/" % (scheme, ip, port)


def port_of(url, default=9998):
    """从 ws://host:port/ 里取端口。"""
    try:
        host = url
        for pre in ("ws://", "wss://"):
            if host.startswith(pre):
                host = host[len(pre):]
                break
        host = host.split("/")[0].split("?")[0]
        if ":" in host:
            return int(host.rsplit(":", 1)[1])
    except (ValueError, IndexError):
        pass
    return default


if __name__ == "__main__":
    ip = lan_ip()
    print("局域网 IP:", ip)
    print("中继地址:", ws_url(ip, 9998))
    print("is_local_url('ws://127.0.0.1:9998/') =", is_local_url("ws://127.0.0.1:9998/"))
    print("is_local_url('ws://%s:9998/') = %s" % (ip, is_local_url(ws_url(ip, 9998))))
    print("is_local_url('wss://trex.dungeon-lab.cn/v4') =",
          is_local_url("wss://trex.dungeon-lab.cn/v4"))
    print("port_of:", port_of("ws://%s:9998/" % ip))
