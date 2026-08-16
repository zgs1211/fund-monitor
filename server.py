# -*- coding: utf-8 -*-
"""
实时基金买卖点监控 - 本地数据与分析服务

用法:
    python server.py
    然后浏览器打开 http://127.0.0.1:8765

功能:
    · 实时估值 + 历史净值 + 分时数据(场内LOF/ETF分钟线, 场外基金盘中估值曲线自动累积)
    · 日线级别: MA20偏离 / RSI14 / 布林带 / KDJ / 3年分位 / 当日涨跌 / MACD / 均线排列
      + 动量 / 连续涨跌 / 距高低点 / 量价 等多角度打分 → 每日买卖点建议
    · 盘中级别: 均价线偏离σ + 趋势确认 + 冷却 → 实时做T买卖点(分时图上标注)
    · 预测准确度量化: 对历史同类信号回测, 统计其后5日/20日胜率与平均收益
    · 仅使用 Python 标准库
    · 仅供个人学习研究, 不构成投资建议
"""
import json
import math
import re
import ssl
import subprocess
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PORT = 8765

# 注意: fundmobapi 对完整桌面 Chrome UA 会触发风控(61136403)，需使用简短 UA
HEADERS = {'User-Agent': 'Mozilla/5.0'}
SSL_CTX = ssl.create_default_context()

FUND_INFO_API = ('https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo'
                 '?pageIndex=1&pageSize=30&plat=Android&appType=ttjj'
                 '&product=EFund&Version=1&Uid=1&deviceid=1&Fcodes={}')
PINGZHONG = 'https://fund.eastmoney.com/pingzhongdata/{}.js'
TRENDS2 = ('https://push2his.eastmoney.com/api/qt/stock/trends2/get?secid={secid}'
           '&ndays=5&iscr=0&fields1=f1,f2,f3,f7,f8'
           '&fields2=f51,f52,f53,f54,f55,f56,f57,f58'
           '&ut=fa5fd1943c7b386f172d6893dbfba10b')

# ---------------- 缓存与状态 ----------------
_lock = threading.Lock()
_hist_cache = {}    # code -> (day_key, data)  历史净值按天缓存
_est_cache = {}     # code -> (ts, data)       实时估值 15 秒缓存
_intr_cache = {}    # code -> (ts, data)       分时数据 20 秒缓存
_market_cache = {}  # code -> market(0=深/1=沪) 记住基金所属市场，避免重复探测
_bt_cache = {}      # code -> (day_key, data)  信号回测按天缓存
_est_curve = {}     # (code, day) -> [[HH:MM, 估算净值], ...]  场外盘中估值曲线(服务端持续累积)


def http_get(url, timeout=12):
    """urllib 会被东财部分接口的 TLS 指纹风控掐断(RemoteDisconnected)，失败时回退系统 curl"""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
            return r.read().decode('utf-8', errors='replace')
    except Exception:
        pass
    r = subprocess.run(['curl', '-s', '-m', str(timeout), '-A', HEADERS['User-Agent'], url],
                       capture_output=True, timeout=timeout + 5)
    if r.returncode == 0 and r.stdout:
        return r.stdout.decode('utf-8', errors='replace')
    raise RuntimeError('请求失败: ' + url[:80])


def extract_js_array(text, varname):
    """从 pingzhongdata 的 JS 中稳健提取数组字面量（括号计数，正确处理字符串）"""
    m = re.search(re.escape(varname) + r'\s*=\s*', text)
    if not m:
        return None
    i = m.end()
    while i < len(text) and text[i] in ' \t\r\n':
        i += 1
    if i >= len(text) or text[i] != '[':
        return None
    depth, j, in_str, esc = 0, i, False, False
    while j < len(text):
        c = text[j]
        if in_str:
            if esc:
                esc = False
            elif c == '\\':
                esc = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == '[':
                depth += 1
            elif c == ']':
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[i:j + 1])
                    except Exception:
                        return None
        j += 1
    return None


def ts_date(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000).date().isoformat()


# ---------------- 数据获取 ----------------
def fetch_estimate(code):
    """实时估值/基金信息（fundmobapi，服务端调用不受浏览器限制）"""
    now = time.time()
    with _lock:
        hit = _est_cache.get(code)
        if hit and now - hit[0] < 15:
            return hit[1]
    data = None
    try:
        j = json.loads(http_get(FUND_INFO_API.format(code)))
        d = (j.get('Datas') or [None])[0]
        if d:
            data = {
                'name': d.get('SHORTNAME') or '',
                'navDate': d.get('PDATE'), 'nav': d.get('NAV'),
                'navChg': d.get('NAVCHGRT'),
                'gsz': d.get('GSZ'), 'gszzl': d.get('GSZZL'), 'gztime': d.get('GZTIME'),
            }
    except Exception:
        data = None
    with _lock:
        _est_cache[code] = (now, data)
    return data


def fetch_history(code):
    """历史净值（pingzhongdata：单位净值 + 累计净值），按天缓存"""
    day_key = date.today().isoformat()
    with _lock:
        hit = _hist_cache.get(code)
        if hit and hit[0] == day_key:
            return hit[1]
    js = http_get(PINGZHONG.format(code) + '?v=' + day_key)
    m = re.search(r'fS_name\s*=\s*"([^"]*)"', js)
    name = m.group(1) if m else ''
    trend = extract_js_array(js, 'Data_netWorthTrend') or []
    ac = extract_js_array(js, 'Data_ACWorthTrend') or []
    acmap = {}
    for a in ac:
        if isinstance(a, (list, tuple)) and len(a) == 2:
            try:
                acmap[int(a[0])] = float(a[1])
            except Exception:
                pass
    points = []
    for p in trend:
        try:
            u = float(p['y'])
        except Exception:
            continue
        points.append({'t': int(p['x']), 'u': u,
                       'ret': p.get('equityReturn'),
                       'a': acmap.get(int(p['x']))})
    data = {'name': name, 'points': points}
    with _lock:
        _hist_cache[code] = (day_key, data)
    return data


def http_get_via_curl(url, timeout=12):
    """直接经系统 curl 请求（push2his 对 urllib 的 TLS 指纹敏感，直连省一次无效连接）"""
    try:
        r = subprocess.run(['curl', '-s', '-m', str(timeout), '-A', HEADERS['User-Agent'], url],
                           capture_output=True, timeout=timeout + 5)
        if r.returncode == 0 and r.stdout:
            return r.stdout.decode('utf-8', errors='replace')
    except Exception:
        pass
    return None


def parse_trends(body):
    """解析 trends2 响应为 {preClose, days}；无数据返回 None"""
    try:
        d = (json.loads(body) or {}).get('data')
        if not d or not d.get('trends'):
            return None
        days = {}
        for row in d['trends']:
            parts = row.split(',')
            if len(parts) < 8:
                continue
            dpart, tpart = parts[0].split(' ')
            day = days.setdefault(dpart, {'minutes': [], 'amt': 0.0})
            try:
                day['minutes'].append({
                    't': tpart[:5], 'c': float(parts[2]),
                    'avg': float(parts[7]) if parts[7] else float(parts[2]),
                    'vol': float(parts[5]) if parts[5] else 0.0,
                })
                day['amt'] += float(parts[6]) if parts[6] else 0.0
            except Exception:
                continue
        if not days:
            return None
        return {'preClose': d.get('preClose'), 'days': days}
    except Exception:
        return None


def fetch_intraday(code):
    """场内基金(LOF/ETF)分时分钟线 + 均价线，近5个交易日；非上市基金返回 None。
       记住基金所属市场避免重复探测；请求间加间隔，避免触发东财连接频率风控。"""
    now = time.time()
    with _lock:
        hit = _intr_cache.get(code)
        if hit and now - hit[0] < 20:
            return hit[1]
        mkt = _market_cache.get(code)
    markets = [mkt] if mkt is not None else [0, 1]
    data = None
    for market in markets:
        url = TRENDS2.format(secid=f'{market}.{code}')
        for _attempt in range(2):
            parsed = parse_trends(http_get_via_curl(url))
            if parsed:
                parsed['type'] = 'exchange'
                data = parsed
                break
            time.sleep(0.5)      # 避免连续请求触发风控
        if data:
            with _lock:
                _market_cache[code] = market
            break
    with _lock:
        _intr_cache[code] = (now, data)
    return data


def append_est_curve(code, est):
    """场外基金：服务端每次轮询记录盘中估算净值，累积当日估值曲线"""
    if not est or est.get('gsz') is None or not est.get('gztime'):
        return None
    gz = est['gztime']        # 形如 2026-08-17 14:30
    day, tm = gz.split(' ')[0], gz.split(' ')[1][:5]
    key = (code, day)
    with _lock:
        curve = _est_curve.setdefault(key, [])
        if not curve or curve[-1][0] != tm:
            curve.append([tm, float(est['gsz'])])
        pts = list(curve)
    return {'day': day, 'points': pts}


# ---------------- 指标 ----------------
def sma(vals, n):
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n


def rsi(vals, period=14):
    if len(vals) < period + 1:
        return None
    g = l = 0.0
    for i in range(1, period + 1):
        d = vals[i] - vals[i - 1]
        if d > 0:
            g += d
        else:
            l -= d
    ag, al = g / period, l / period
    for i in range(period + 1, len(vals)):
        d = vals[i] - vals[i - 1]
        ag = (ag * (period - 1) + max(d, 0)) / period
        al = (al * (period - 1) + max(-d, 0)) / period
    if al == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + ag / al)


def boll(vals, n=20, k=2.0):
    if len(vals) < n:
        return None
    w = vals[-n:]
    m = sum(w) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in w) / n)
    return m, m + k * sd, m - k * sd


def macd(vals, fast=12, slow=26, sig=9):
    if len(vals) < slow + sig:
        return None
    ef = es = vals[0]
    difs = []
    for v in vals:
        ef += (v - ef) * 2 / (fast + 1)
        es += (v - es) * 2 / (slow + 1)
        difs.append(ef - es)
    dea = difs[0]
    for dv in difs:
        dea += (dv - dea) * 2 / (sig + 1)
    return difs[-1], dea, (difs[-1] - dea) * 2


def kdj(closes, n=9):
    """以收盘价滚动最高/最低替代高低价（净值序列无高低价）"""
    if len(closes) < n:
        return None
    k = d = 50.0
    for i in range(n - 1, len(closes)):
        w = closes[i - n + 1:i + 1]
        hh, ll = max(w), min(w)
        rsv = 50.0 if hh == ll else (closes[i] - ll) / (hh - ll) * 100
        k = k * 2 / 3 + rsv / 3
        d = d * 2 / 3 + k / 3
    return k, d, 3 * k - 2 * d


def pct_change(vals, n):
    if len(vals) < n + 1 or vals[-n - 1] == 0:
        return None
    return (vals[-1] / vals[-n - 1] - 1) * 100


def percentile_rank(vals, v, window=750, min_len=60):
    w = vals[-window:]
    if len(w) < min_len:
        return None
    return sum(1 for x in w if x <= v) / len(w) * 100


def max_drawdown(vals, window=250):
    w = vals[-window:]
    if len(w) < 2:
        return None
    peak, mdd = w[0], 0.0
    for v in w:
        peak = max(peak, v)
        if peak > 0:
            mdd = min(mdd, v / peak - 1)
    return mdd * 100


def annualized_vol(unit_vals, window=60):
    if len(unit_vals) < window + 1:
        window = len(unit_vals) - 1
    if window < 20:
        return None
    rets = []
    w = unit_vals[-(window + 1):]
    for i in range(1, len(w)):
        if w[i - 1] > 0:
            rets.append(w[i] / w[i - 1] - 1)
    if len(rets) < 10:
        return None
    m = sum(rets) / len(rets)
    sd = math.sqrt(sum((x - m) ** 2 for x in rets) / len(rets))
    return sd * math.sqrt(250) * 100


# ---------------- 日线信号 ----------------
def core_score(vals, day_chg):
    """四类核心规则打分（历史回溯、回测、今日共用）：正=买入倾向"""
    score = 0
    if not vals:
        return 0
    last = vals[-1]
    ma20 = sma(vals, 20)
    if ma20:
        dev = (last - ma20) / ma20 * 100
        if dev <= -3:
            score += 2
        elif dev <= -1.5:
            score += 1
        elif dev >= 3:
            score -= 2
        elif dev >= 1.5:
            score -= 1
    r = rsi(vals, 14)
    if r is not None:
        if r <= 30:
            score += 2
        elif r <= 40:
            score += 1
        elif r >= 70:
            score -= 2
        elif r >= 60:
            score -= 1
    b = boll(vals, 20, 2.0)
    if b:
        _, up, low = b
        if last <= low:
            score += 1
        elif last >= up:
            score -= 1
    if day_chg is not None:
        if day_chg <= -2:
            score += 1
        elif day_chg >= 3:
            score -= 1
    return score


def detect_events(points):
    """历史逐日回溯，滞回状态机生成日线买卖点事件（进入 ±2，回到 0 退出）"""
    pts = points[-500:]
    use_acc = sum(1 for p in pts if p.get('a') is not None) > len(pts) * 0.9
    vals, events = [], []
    state = 0
    for i, p in enumerate(pts):
        vals.append(p['a'] if use_acc else p['u'])
        if len(vals) < 25:
            continue
        try:
            ret = float(p['ret']) if p.get('ret') is not None else None
        except Exception:
            ret = None
        s = core_score(vals, ret)
        if state != 1 and s >= 2:
            events.append({'date': ts_date(p['t']), 'side': 'buy', 'score': s})
            state = 1
        elif state != -1 and s <= -2:
            events.append({'date': ts_date(p['t']), 'side': 'sell', 'score': s})
            state = -1
        elif state == 1 and s <= 0:
            state = 0
        elif state == -1 and s >= 0:
            state = 0
    return events


def score_label(s):
    if s >= 3:
        return '强买', 'buy'
    if s >= 1:
        return '买入', 'buy'
    if s <= -3:
        return '强卖', 'sell'
    if s <= -1:
        return '卖出', 'sell'
    return '观望', 'flat'


def daily_signal_table(points, use_acc, n=15):
    """最近 n 个交易日的每日信号表：日期/净值/评分/信号 + 次日与5日涨跌（事后验证）"""
    vals_all = [p['a'] if use_acc else p['u'] for p in points]
    rows = []
    start = max(60, len(points) - n)
    for i in range(start, len(points)):
        try:
            ret = float(points[i]['ret']) if points[i].get('ret') is not None else None
        except Exception:
            ret = None
        s = core_score(vals_all[:i + 1], ret)
        label, cls = score_label(s)
        next1 = (vals_all[i + 1] / vals_all[i] - 1) * 100 if (i + 1 < len(points) and vals_all[i] > 0) else None
        next5 = (vals_all[i + 5] / vals_all[i] - 1) * 100 if (i + 5 < len(points) and vals_all[i] > 0) else None
        rows.append({'date': ts_date(points[i]['t']), 'nav': round(points[i]['u'], 4),
                     'score': s, 'label': label, 'cls': cls,
                     'next1': round(next1, 2) if next1 is not None else None,
                     'next5': round(next5, 2) if next5 is not None else None})
    return rows


# ---------------- 盘中做T信号 ----------------
def detect_t_signals(minutes):
    """单日分钟序列上的做T信号（全部为触发时刻的实时预测，只用当时及之前的行情）：
       1) T买：价格低于均价线1.5σ + 近3分钟回升确认 + 量能/资金配合
       2) T卖-冲高：价格高于均价线1.5σ + 近3分钟回落确认（上涨日的减仓点）
       3) T卖-配对止盈：T买之后价格反弹接近均价线且滞涨、且高于买入价 → 给出同日卖出点
       上涨分钟量估算主动买盘、下跌分钟量估算主动卖盘；★=量价资金共振强信号。"""
    events = []
    if len(minutes) < 30:
        return events, None
    closes = [m['c'] for m in minutes]
    vols = [float(m.get('vol') or 0.0) for m in minutes]
    has_vol = sum(vols) > 0
    devs = [m['c'] - m['avg'] for m in minutes]

    # 量能前缀和：总成交量 / 上涨分钟量(估主动买) / 下跌分钟量(估主动卖)
    PV, PB, PS = [0.0], [0.0], [0.0]
    for i, v in enumerate(vols):
        d = closes[i] - closes[i - 1] if i > 0 else 0.0
        PV.append(PV[-1] + v)
        PB.append(PB[-1] + (v if d > 0 else 0.0))
        PS.append(PS[-1] + (v if d < 0 else 0.0))

    def vol_ctx(i):
        if not has_vol:
            return None, None
        avg_v = PV[i] / i if i > 0 else 0.0
        vr = vols[i] / avg_v if avg_v > 0 else None
        vnote = '缩量' if (vr is not None and vr < 0.8) else ('放量' if (vr is not None and vr > 1.5) else '平量')
        lo15 = max(0, i - 14)
        b15 = PB[i + 1] - PB[lo15]
        s15 = PS[i + 1] - PS[lo15]
        br15 = (b15 / (b15 + s15) * 100) if (b15 + s15) > 0 else None
        return vnote, br15

    cooldown_until = -1
    holding = False          # 是否持有T仓（每个T买必配对一个T卖）
    entry = 0.0
    n = len(minutes)
    take_price = stop_price = None
    below_cnt = 0            # 价格低于均价线的分钟数（累计判断单边下行）
    for i in range(30, n):
        w = devs[max(0, i - 60):i + 1]
        mu = sum(w) / len(w)
        sd = math.sqrt(sum((x - mu) ** 2 for x in w) / len(w))
        if sd < 1e-9:
            continue
        z = devs[i] / sd
        if devs[i] < 0:
            below_cnt += 1
        c, c3 = closes[i], closes[max(0, i - 3)]
        vnote, br15 = vol_ctx(i)

        if holding:
            # 持仓中：实时跟踪三类卖出点（提前公布规则，触发即提示）
            if c <= stop_price:                      # 止损保护（比买价低，用于截断亏损）
                events.append({'t': minutes[i]['t'], 'side': 'sell', 'price': round(c, 4),
                               'conf': '中', 'vol': vnote,
                               'br15': round(br15, 1) if br15 is not None else None,
                               'why': '止损保护'})
                holding, cooldown_until = False, i + 5
            elif c >= take_price and c < c3:         # 反弹到目标且滞涨 → 止盈
                events.append({'t': minutes[i]['t'], 'side': 'sell', 'price': round(c, 4),
                               'conf': '强', 'vol': vnote,
                               'br15': round(br15, 1) if br15 is not None else None,
                               'why': '反弹止盈'})
                holding, cooldown_until = False, i + 5
            elif i >= n - 3:                         # 尾盘强制平仓（T+0纪律）
                events.append({'t': minutes[i]['t'], 'side': 'sell', 'price': round(c, 4),
                               'conf': '中', 'vol': vnote,
                               'br15': round(br15, 1) if br15 is not None else None,
                               'why': '尾盘平仓'})
                holding = False
            continue

        if i < cooldown_until:
            continue

        # 当日状态（供快照提示）：45分钟均价线斜率 + 累计低于均价线占比
        i45 = max(0, i - 45)
        slope = (minutes[i]['avg'] - minutes[i45]['avg']) / minutes[i45]['avg'] * 100 if minutes[i45]['avg'] else 0.0
        downtrend = slope < -0.08

        # T买：偏离均价线1.5σ + 回升确认 + 基本量价配合（缩量或资金回流任一即可）
        # 注：不对单一交易日过拟合趋势门槛；靠止损/止盈纪律与状态提示控制风险
        if z <= -1.5 and c > c3:
            ok = (not has_vol) or (br15 is not None and br15 >= 55) or (vnote == '缩量')
            if ok:
                conf = '强' if (has_vol and vnote != '放量' and br15 is not None and br15 >= 55) else '中'
                events.append({'t': minutes[i]['t'], 'side': 'buy', 'price': round(c, 4),
                               'conf': conf, 'vol': vnote,
                               'br15': round(br15, 1) if br15 is not None else None,
                               'why': '低位回升'})
                holding, entry = True, c
                take_price = round(entry + max(0.002 * entry, 0.8 * sd), 4)
                stop_price = round(entry - max(0.004 * entry, 1.2 * sd), 4)
                cooldown_until = i + 5
            continue

        if z >= 1.5 and c < c3:
            # T卖-冲高：上涨日高点回落（对隔夜持仓的减仓点）
            conf = '中'
            if has_vol and vnote != '放量' and br15 is not None and br15 <= 45:
                conf = '强'
            events.append({'t': minutes[i]['t'], 'side': 'sell', 'price': round(c, 4),
                           'conf': conf, 'vol': vnote,
                           'br15': round(br15, 1) if br15 is not None else None,
                           'why': '冲高回落'})
            cooldown_until = i + 15
        elif downtrend and -0.2 <= z <= 0.8 and c < c3 and (i - 30) > 0:
            # 下跌日反弹至均价线滞涨 → 持仓者减仓卖点
            events.append({'t': minutes[i]['t'], 'side': 'sell', 'price': round(c, 4),
                           'conf': '中', 'vol': vnote,
                           'br15': round(br15, 1) if br15 is not None else None,
                           'why': '反弹至均价线减仓'})
            cooldown_until = i + 15

    # 当前快照
    last = minutes[-1]
    hi, lo = max(closes), min(closes)
    w = devs[-60:]
    mu = sum(w) / len(w)
    sd = math.sqrt(sum((x - mu) ** 2 for x in w) / len(w)) or 1e-9
    z_now = devs[-1] / sd
    m15 = closes[-1] - closes[max(0, len(closes) - 15)]

    vol_info = None
    if has_vol:
        n = len(vols)
        day_v = PV[n]
        br_day = (PB[n] / day_v * 100) if day_v > 0 else None
        lo15 = max(0, n - 15)
        b15, s15 = PB[n] - PB[lo15], PS[n] - PS[lo15]
        br15 = (b15 / (b15 + s15) * 100) if (b15 + s15) > 0 else None
        avg_v = PV[n] / max(1, n)
        vr5 = (sum(vols[-5:]) / min(5, n)) / avg_v if avg_v > 0 else None
        vol_info = {'buyRatioDay': round(br_day, 1) if br_day is not None else None,
                    'buyRatio15': round(br15, 1) if br15 is not None else None,
                    'volRatio': round(vr5, 2) if vr5 is not None else None}

    vw0s = minutes[30]['avg']
    frac_final = below_cnt / max(1, n - 31)
    crash_final = (last['avg'] - vw0s) / vw0s < -0.4 and frac_final > 0.75
    regime = 'crash' if crash_final else ('down' if downtrend else 'normal')

    advice, level = '均价附近波动，建议观望等待', 'watch'
    if holding:
        advice = (f"T买持仓中（成本{round(entry,4)}）：反弹至 {take_price} 滞涨即止盈，"
                  f"跌破 {stop_price} 止损离场")
        level = 'holdsell'
    elif crash_final:
        advice, level = '单边下行日：不宜T买（持币观望）；持仓者利用反弹至均价线的机会减仓', 'crash'
    elif z_now <= -1.5:
        level = 'buy'
        advice = '低于均价线1.5σ且回升，T买点（短线低吸）'
        if vol_info and vol_info['buyRatio15'] is not None and vol_info['buyRatio15'] >= 55:
            advice = f"低于均价线1.5σ且回升，近15分主买占{vol_info['buyRatio15']}%，T买点确认（资金回流）"
    elif z_now <= -0.8:
        advice, level = '偏低于均价线，接近T买观察区（关注资金回流信号）', 'watchbuy'
    elif z_now >= 1.5:
        level = 'sell'
        advice = '高于均价线1.5σ且回落，T卖点（冲高减仓）'
        if vol_info and vol_info['buyRatio15'] is not None and vol_info['buyRatio15'] <= 45:
            advice = f"高于均价线1.5σ且回落，近15分主买仅{vol_info['buyRatio15']}%，T卖点确认（买盘乏力）"
    elif z_now >= 0.8:
        advice, level = '偏高于均价线，接近T卖观察区（关注量能萎缩）', 'watchsell'
    snapshot = {
        'time': last['t'], 'price': round(last['c'], 4), 'vwap': round(last['avg'], 4),
        'z': round(z_now, 2), 'high': round(hi, 4), 'low': round(lo, 4),
        'amp': round((hi - lo) / (hi if lo == 0 else lo) * 100, 2) if lo > 0 else None,
        'm15': round(m15, 4), 'advice': advice, 'level': level,
        'sd': round(sd, 5),
        'tBuy': round(last['avg'] - 1.5 * sd, 4), 'tSell': round(last['avg'] + 1.5 * sd, 4),
        'vol': vol_info,
        'holding': holding,
        'holdEntry': round(entry, 4) if holding else None,
        'takePrice': take_price if holding else None,
        'stopPrice': stop_price if holding else None,
        'regime': regime,
    }
    return events, snapshot


# ---------------- 历史回测（预测准确度） ----------------
def backtest_signal(code, points, use_acc):
    """对历史逐日计算核心信号并分桶，统计各桶其后5日/20日的真实收益：
       返回每个桶的样本数/胜率/平均收益；并回测历史买卖点事件的有效性"""
    day_key = date.today().isoformat()
    with _lock:
        hit = _bt_cache.get(code)
        if hit and hit[0] == day_key:
            return hit[1]
    vals = [p['a'] if use_acc else p['u'] for p in points]
    n = len(vals)
    buckets = {}
    events_eff = {'n': 0, 'win5': 0.0}
    if n >= 120:
        for i in range(80, n - 5):
            try:
                ret = float(points[i]['ret']) if points[i].get('ret') is not None else None
            except Exception:
                ret = None
            s = core_score(vals[:i + 1], ret)
            bucket = 'buy' if s >= 2 else ('sell' if s <= -2 else 'hold')
            r5 = (vals[i + 5] / vals[i] - 1) * 100 if vals[i] > 0 else None
            r20 = (vals[i + 20] / vals[i] - 1) * 100 if (i + 20 < n and vals[i] > 0) else None
            b = buckets.setdefault(bucket, {'n': 0, 'w5': 0, 's5': 0.0, 'w20': 0, 's20': 0.0, 'n20': 0})
            b['n'] += 1
            if r5 is not None:
                b['s5'] += r5
                if (r5 > 0) == (bucket != 'sell'):
                    b['w5'] += 1
            if r20 is not None:
                b['n20'] += 1
                b['s20'] += r20
                if (r20 > 0) == (bucket != 'sell'):
                    b['w20'] += 1
        # 历史买卖点事件后5日表现
        date_idx = {ts_date(p['t']): i for i, p in enumerate(points)}
        for ev in detect_events(points):
            i = date_idx.get(ev['date'])
            if i is None or i + 5 >= n or vals[i] <= 0:
                continue
            r5 = (vals[i + 5] / vals[i] - 1) * 100
            events_eff['n'] += 1
            if (r5 > 0) == (ev['side'] == 'buy'):
                events_eff['win5'] += 1
        if events_eff['n']:
            events_eff['win5'] = round(events_eff['win5'] / events_eff['n'] * 100, 1)
    result = {'buckets': buckets, 'events': events_eff}
    with _lock:
        _bt_cache[code] = (day_key, result)
    return result


def summarize_predict(bt, today_bucket):
    """把回测统计转成今日预测面板数据（附置信度与买卖桶参考）"""
    buckets = (bt or {}).get('buckets', {})

    def s_of(name):
        b = buckets.get(name)
        if not b or b['n'] < 8:
            return None
        return {'n': b['n'],
                'win5': round(b['w5'] / b['n'] * 100, 1),
                'avg5': round(b['s5'] / b['n'], 2),
                'win20': round(b['w20'] / b['n20'] * 100, 1) if b.get('n20') else None,
                'avg20': round(b['s20'] / b['n20'], 2) if b.get('n20') else None}

    out = {'bucket': today_bucket, 'refBuy': s_of('buy'), 'refSell': s_of('sell')}
    b = s_of(today_bucket)
    if not b:
        out.update({'n': 0, 'conf': '低', 'note': '历史同类样本不足，预测参考意义有限'})
        return out
    out.update(b)
    if today_bucket == 'hold':
        out['conf'] = '基准'
    elif b['n'] >= 30 and b['win5'] >= 60:
        out['conf'] = '高'
    elif b['n'] >= 15:
        out['conf'] = '中'
    else:
        out['conf'] = '低'
    out['evN'] = (bt or {}).get('events', {}).get('n')
    out['evWin5'] = (bt or {}).get('events', {}).get('win5')
    return out


# ---------------- 综合分析 ----------------
CAT_TEXT = {
    'strongBuy': '强烈买入 · 低估区', 'buy': '建议买入 / 加仓', 'hold': '持有 · 观望',
    'sell': '建议减仓 / 止盈', 'strongSell': '强烈减仓 · 高估区',
}
ACTION_TEXT = {
    'strongBuy': '今日建议：低估区，分批买入（建议拆成3~4笔，单笔不超过计划加仓额的1/3）',
    'buy': '今日建议：估值偏低，可小额加仓或开启/加大定投',
    'hold': '今日建议：持有观望，保持现有仓位不动，等待信号明确',
    'sell': '今日建议：估值偏高，减仓1/4~1/3 锁定收益',
    'strongSell': '今日建议：显著高估，分批止盈（可保留底仓）',
}
SCORE_MAX = 8


def analyze_fund(code):
    out = {'code': code, 'ok': False, 'error': None, 'name': '', 'est': None,
           'nav': None, 'series': None, 'events': [], 'today': None, 'stats': None,
           'intraday': None, 'predict': None}
    try:
        est = fetch_estimate(code)
        hist = fetch_history(code)
    except Exception as e:
        out['error'] = '数据接口请求失败: %s' % e
        return out
    if not est and not hist:
        out['error'] = '接口无数据，请检查基金代码是否正确'
        return out

    name = (est or {}).get('name') or (hist or {}).get('name') or ''
    out['name'] = name
    if est:
        out['est'] = {'value': est.get('gsz'), 'chgPct': est.get('gszzl'),
                      'time': est.get('gztime')}
        out['nav'] = {'date': est.get('navDate'), 'unit': est.get('nav'),
                      'acc': None, 'chgPct': est.get('navChg')}
    if not hist or len(hist.get('points', [])) < 25:
        out['error'] = out['error'] or '历史净值数据不足，无法计算信号'
        return out

    pts = hist['points'][-500:]
    unit = [p['u'] for p in pts]
    use_acc = sum(1 for p in pts if p.get('a') is not None) > len(pts) * 0.9
    base = [p['a'] if p.get('a') is not None else p['u'] for p in pts] if use_acc else unit
    dates = [ts_date(p['t']) for p in pts]

    # 货币/理财型: 波动极小不适用
    w60 = unit[-60:]
    if min(w60) > 0 and (max(w60) - min(w60)) / min(w60) < 0.005:
        out.update(ok=True, flat=True,
                   today={'score': 0, 'cat': 'hold',
                          'action': '该基金净值波动极小（疑似货币/理财型），不适合趋势信号模型',
                          'items': [], 'trend': ''})
        out['series'] = {'dates': dates, 'unit': unit, 'ma20': [None] * len(unit)}
        return out

    # ---- 今日序列：并入盘中估算（折算到累计口径） ----
    today_key = date.today().isoformat()
    est_val, est_chg = None, None
    if est and est.get('gsz'):
        try:
            est_val = float(est['gsz'])
            est_chg = float(est['gszzl']) if est.get('gszzl') is not None else None
        except Exception:
            est_val = None
    merged = list(base)
    merged_unit = list(unit)
    has_today = dates[-1] == today_key
    if est_val and not has_today:
        merged.append(base[-1] + (est_val - unit[-1]))
        merged_unit.append(est_val)
        dates = dates + [today_key]

    # ---- 多角度指标 ----
    score = core_score(merged, est_chg if (est_val and not has_today) else None)
    items = []
    last = merged[-1]
    ma5, ma10, ma20v = sma(merged, 5), sma(merged, 10), sma(merged, 20)
    dev = (last - ma20v) / ma20v * 100 if ma20v else None
    r14 = rsi(merged, 14)
    bm, bu, bl = boll(merged, 20, 2.0) or (None, None, None)
    p3y = percentile_rank(merged, last)
    kd = kdj(merged)

    if dev is not None:
        if dev <= -3:    d, st, sc = 'buy', '深度低估', 2
        elif dev <= -1.5: d, st, sc = 'buy', '偏低估', 1
        elif dev >= 3:   d, st, sc = 'sell', '明显高估', -2
        elif dev >= 1.5: d, st, sc = 'sell', '偏高', -1
        else:            d, st, sc = 'flat', '正常区间', 0
        items.append({'name': '均线偏离 MA20', 'value': f'{dev:+.2f}%', 'dir': d, 'status': st,
                      'tip': '净值相对20日均线偏离度：深跌低估适合分批买入，大幅高于均线考虑止盈'})
    if r14 is not None:
        if r14 <= 30:   d, st, sc = 'buy', '超卖', 2
        elif r14 <= 40: d, st, sc = 'buy', '偏弱', 1
        elif r14 >= 70: d, st, sc = 'sell', '超买', -2
        elif r14 >= 60: d, st, sc = 'sell', '偏强', -1
        else:           d, st, sc = 'flat', '中性', 0
        items.append({'name': 'RSI(14)', 'value': f'{r14:.1f}', 'dir': d, 'status': st,
                      'tip': '≤30 超卖（买入时机），≥70 超买（警惕回调）'})
    if bu is not None:
        pos = (last - bl) / (bu - bl) * 100 if bu > bl else None
        if last <= bl:    d, st, sc = 'buy', '跌破下轨', 1
        elif last >= bu:  d, st, sc = 'sell', '突破上轨', -1
        elif pos is not None and last < bm: d, st, sc = 'flat', '下半区', 0
        else:             d, st, sc = 'flat', '上半区', 0
        items.append({'name': '布林带(20,2σ)位置', 'value': f'{pos:.0f}%' if pos is not None else '--',
                      'dir': d, 'status': st, 'tip': '0%=下轨 100%=上轨；触下轨偏买入，触上轨偏卖出'})
    if kd:
        _, _, jv = kd
        if jv <= 10:   d, st, sc = 'buy', '超卖', 1
        elif jv >= 90: d, st, sc = 'sell', '超买', -1
        else:          d, st, sc = 'flat', '中性', 0
        items.append({'name': 'KDJ(9) J值', 'value': f'{jv:.1f}', 'dir': d, 'status': st,
                      'tip': 'J≤10 超卖偏买，J≥90 超买偏卖（以收盘价滚动高低计算）'})
        score += sc
    if est_val and not has_today and est_chg is not None:
        if est_chg <= -2: d, st, sc = 'buy', '大跌·逢跌加仓', 1
        elif est_chg >= 3: d, st, sc = 'sell', '大涨·考虑止盈', -1
        else:              d, st, sc = 'flat', '波动正常', 0
        items.append({'name': '当日估算涨跌', 'value': f'{est_chg:+.2f}%', 'dir': d, 'status': st,
                      'tip': '单日大跌分批加仓、大涨分批减仓的纪律化策略'})
        score += sc
    else:
        items.append({'name': '当日估算涨跌', 'value': '--', 'dir': 'flat',
                      'status': '非交易时段' if est else '暂无估值',
                      'tip': '交易时段内根据盘中估算涨跌触发：大跌加仓、大涨减仓'})
    if p3y is not None:
        if p3y <= 20:  d, st, sc = 'buy', '低估分位', 1
        elif p3y >= 80: d, st, sc = 'sell', '高估分位', -1
        else:           d, st, sc = 'flat', '中位', 0
        items.append({'name': '近3年净值分位', 'value': f'{p3y:.0f}%', 'dir': d, 'status': st,
                      'tip': '当前净值在近3年区间中的位置：≤20% 低估，≥80% 高估'})
        score += sc

    # 附加观察角度（不参与打分）
    rets = []
    for i in range(1, len(base)):
        if base[i - 1] > 0:
            rets.append(base[i] / base[i - 1] - 1)
    streak = 0
    if rets:
        sgn = 1 if rets[-1] > 0 else (-1 if rets[-1] < 0 else 0)
        for r in reversed(rets):
            if (r > 0 and sgn > 0) or (r < 0 and sgn < 0):
                streak += 1
            else:
                break
    if streak:
        items.append({'name': '连续涨/跌天数', 'value': f'{streak}天',
                      'dir': 'buy' if sgn < 0 else 'sell',
                      'status': ('连跌' if sgn < 0 else '连涨'),
                      'tip': '连跌后反弹概率上升，连涨后回调风险上升（观察项）'})
    w60v = base[-60:]
    hi60, lo60 = max(w60v), min(w60v)
    if hi60 > 0:
        pos60 = (last - lo60) / (hi60 - lo60) * 100 if hi60 > lo60 else 50
        items.append({'name': '60日区间位置', 'value': f'{pos60:.0f}%',
                      'dir': 'buy' if pos60 <= 25 else ('sell' if pos60 >= 75 else 'flat'),
                      'status': '接近低位' if pos60 <= 25 else ('接近高位' if pos60 >= 75 else '中位'),
                      'tip': '当前净值在近60日高低点区间中的位置（观察项）'})
    r10 = pct_change(merged, 10)
    if r10 is not None:
        items.append({'name': '10日动量', 'value': f'{r10:+.2f}%',
                      'dir': 'buy' if r10 < -5 else ('sell' if r10 > 8 else 'flat'),
                      'status': '超跌反弹观察' if r10 < -5 else ('短期过热' if r10 > 8 else '正常'),
                      'tip': '短期累计涨跌幅：超跌关注反弹，过热防回调（观察项）'})

    score = max(-SCORE_MAX, min(SCORE_MAX, score))
    if score >= 3:     cat = 'strongBuy'
    elif score >= 1:   cat = 'buy'
    elif score <= -3:  cat = 'strongSell'
    elif score <= -1:  cat = 'sell'
    else:              cat = 'hold'

    trend = []
    if ma5 and ma20v:
        trend.append('短期均线上穿中期均线（偏多）' if ma5 >= ma20v else '短期均线下穿中期均线（偏空）')
    if ma5 and ma10 and ma20v:
        if ma5 > ma10 > ma20v:
            trend.append('均线多头排列')
        elif ma5 < ma10 < ma20v:
            trend.append('均线空头排列')
    md = macd(merged)
    if md:
        dif, dea, _ = md
        trend.append('MACD 多头（DIF>DEA）' if dif > dea else 'MACD 空头（DIF<DEA）')

    out.update(ok=True, flat=False,
               today={'score': score, 'cat': cat, 'action': ACTION_TEXT[cat],
                      'items': items, 'trend': '，'.join(trend) or '趋势中性'})

    # ---- 图表序列 ----
    ma20_series = []
    for i in range(len(merged_unit)):
        ma20_series.append(round(sum(merged_unit[i - 19:i + 1]) / 20, 4) if i >= 19 else None)
    out['series'] = {'dates': dates,
                     'unit': [round(v, 4) for v in merged_unit],
                     'ma20': ma20_series}
    out['events'] = detect_events(hist['points'])

    # ---- 统计 ----
    def fmt(v):
        return None if v is None else round(v, 2)
    out['stats'] = {
        'r20': fmt(pct_change(base, 20)), 'r60': fmt(pct_change(base, 60)),
        'r250': fmt(pct_change(base, 250)),
        'vol': fmt(annualized_vol(unit)),
        'mdd': fmt(max_drawdown(base)),
        'p3y': fmt(p3y),
    }

    # ---- 预测：历史同类信号回测 ----
    try:
        ret_last = None
        try:
            ret_last = float(pts[-1]['ret']) if pts[-1].get('ret') is not None else None
        except Exception:
            pass
        core_today = core_score(merged, est_chg if (est_val and not has_today) else ret_last)
        today_bucket = 'buy' if core_today >= 2 else ('sell' if core_today <= -2 else 'hold')
        bt = backtest_signal(code, hist['points'], use_acc)
        out['predict'] = summarize_predict(bt, today_bucket)
    except Exception:
        core_today = score  # 兜底：用今日综合评分
        out['predict'] = None

    # ---- 每日买卖点表（近15个交易日 + 今日实时行） ----
    try:
        rows = daily_signal_table(hist['points'], use_acc, 15)
        if rows and rows[-1]['date'] == today_key:
            rows[-1]['isToday'] = True
        else:
            lbl, cls = score_label(core_today)
            rows.append({'date': today_key, 'nav': round(merged_unit[-1], 4),
                         'score': core_today, 'label': lbl, 'cls': cls,
                         'next1': None, 'next5': None, 'isToday': True})
        out['daily'] = rows
    except Exception as e:
        print(f'[daily] {code} 生成失败: {e}', flush=True)
        out['daily'] = None

    # ---- 参考价位（日线触发价，按单位净值口径，与页面展示一致） ----
    ma20_u = sma(merged_unit, 20)
    if ma20_u:
        b_u = boll(merged_unit, 20, 2.0)
        bu_u, bl_u = (b_u[1], b_u[2]) if b_u else (None, None)
        out['levels'] = {
            'buyTrigger': round(min(ma20_u * 0.985, bl_u if bl_u else ma20_u * 0.985), 4),
            'sellTrigger': round(max(ma20_u * 1.015, bu_u if bu_u else ma20_u * 1.015), 4),
        }

    # ---- 盘中分时与做T信号 ----
    try:
        intr = fetch_intraday(code)
        if intr:
            days = intr['days']
            last_day = max(days.keys())
            minutes = days[last_day]['minutes']
            evs, snap = detect_t_signals(minutes)
            # 5日视图数据（每日分钟收盘价）
            days5 = {d: [[m['t'], m['c']] for m in v['minutes']] for d, v in
                     sorted(days.items())[-5:]}
            # 量价：今日成交额 vs 前4日均值
            amts = sorted(days.items())
            vol_note = None
            if len(amts) >= 2:
                today_amt = amts[-1][1]['amt']
                prev_avg = sum(v['amt'] for _, v in amts[-5:-1]) / max(1, len(amts) - 1) if len(amts) >= 2 else None
                if prev_avg and prev_avg > 0:
                    vol_note = round(today_amt / prev_avg, 2)
            out['intraday'] = {
                'type': 'exchange', 'date': last_day, 'preClose': intr.get('preClose'),
                'points': [[m['t'], m['c'], m['avg']] for m in minutes],
                'signals': evs, 'snapshot': snap, 'days5': days5, 'amtRatio': vol_note,
            }
        else:
            curve = append_est_curve(code, est)
            if curve and len(curve['points']) >= 10:
                # 场外基金：以服务端累积的估值曲线计算做T参考（均值线=累计均值）
                mlist, run = [], 0.0
                for i, (tm, v) in enumerate(curve['points']):
                    run += v
                    mlist.append({'t': tm, 'c': v, 'avg': run / (i + 1)})
                evs, snap = detect_t_signals(mlist)
                out['intraday'] = {
                    'type': 'estimate', 'date': curve['day'],
                    'ref': float(est['nav']) if est and est.get('nav') else None,
                    'points': [[m['t'], m['c'], round(m['avg'], 4)] for m in mlist],
                    'signals': evs, 'snapshot': snap, 'days5': None, 'amtRatio': None,
                }
            else:
                out['intraday'] = None
    except Exception as e:
        print(f'[intraday] {code} 分析失败: {e}', flush=True)
        out['intraday'] = None

    # 量价观察项（仅场内有成交数据）
    if out.get('intraday') and out['intraday'].get('amtRatio'):
        ratio = out['intraday']['amtRatio']
        items.append({'name': '今日量能/4日均量', 'value': f'{ratio:.2f}倍',
                      'dir': 'flat',
                      'status': '放量' if ratio >= 1.3 else ('缩量' if ratio <= 0.7 else '平量'),
                      'tip': '场内成交额对比：放量下跌风险大，缩量回调压力小（观察项）'})
    return out


# ---------------- HTTP 服务 ----------------
class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, fmt, *args):
        pass

    def _send(self, body, ctype):
        self.send_response(200)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split('?')[0]
        try:
            if path.startswith('/api/analysis'):
                qs = self.path.split('?', 1)[1] if '?' in self.path else ''
                codes = []
                for kv in qs.split('&'):
                    if kv.startswith('codes='):
                        raw = kv[6:].split(',')
                        codes = [c.strip() for c in raw if re.fullmatch(r'\d{6}', c.strip())][:20]
                if not codes:
                    return self._json({'funds': []})
                with ThreadPoolExecutor(max_workers=4) as ex:
                    funds = list(ex.map(analyze_fund, codes))
                return self._json({'serverTime': datetime.now().isoformat(timespec='seconds'),
                                   'funds': funds})
            if path in ('/', '/index.html'):
                return self._send((ROOT / 'index.html').read_bytes(),
                                  'text/html; charset=utf-8')
            self._json({'error': 'not found'}, 404)
        except BrokenPipeError:
            pass
        except Exception as e:
            try:
                self._json({'error': str(e)}, 500)
            except Exception:
                pass


def main():
    srv = ThreadingHTTPServer(('127.0.0.1', PORT), Handler)
    srv.daemon_threads = True
    print(f'[基金监控] 本地服务已启动: http://127.0.0.1:{PORT}')
    print('[基金监控] 数据来源: 东方财富/天天基金公开接口, 仅供学习研究, 不构成投资建议')
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print('\n[基金监控] 已停止')


if __name__ == '__main__':
    main()
