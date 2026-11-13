"""
Арбитражный сканер-сигнальщик.

Что делает:
  1. Связки с переводом: купить на бирже A -> вывести по сети X -> продать на бирже B.
  2. Хедж-связки: спот -> фьючерс и фьючерс -> фьючерс (без перевода).
Считает ЧИСТЫЙ % по реальному стакану под твой объём, с комиссиями и выводом,
и шлёт готовую связку в Telegram. Сам не торгует и ключей для торговли не требует.
"""
import asyncio
import html
import logging
import os
import re
import time

import aiohttp
import ccxt.async_support as ccxt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("arb")


def env(name, default):
    return os.getenv(name, str(default)).strip()


# ----------------------------- настройки (через переменные окружения) ---------
TG_TOKEN = env("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = env("TELEGRAM_CHAT_ID", "")    # куда слать сигналы (личка или группа)
TG_ADMIN = env("TELEGRAM_ADMIN_ID", "")  # user id того, кто управляет ботом
EXCHANGES = [e for e in env("EXCHANGES", "bybit,okx,gate,kucoin,mexc,bitget,htx,bingx").split(",") if e]
# по умолчанию — pre-IPO перпы: на разных биржах это разные синтетические контракты, цены не обязаны сходиться
BLACKLIST = {c.upper() for c in env("BLACKLIST", "OPENAI,ANTHROPIC").split(",") if c}

CFG = {
    "size": float(env("SIZE_USDT", 500)),           # объём одной связки, USDT
    "min_net": float(env("MIN_NET_PCT", 1.0)),      # мин. чистый % для связки с переводом
    "min_hedge": float(env("MIN_HEDGE_PCT", 1.0)),  # мин. чистый % для хедж-связки
    "paused": False,
}
MAX_SPREAD = float(env("MAX_SPREAD_PCT", 20))       # выше — почти всегда разные монеты/закрытый вывод
MIN_VOLUME = float(env("MIN_VOLUME_USDT", 100000))  # мин. оборот за 24ч на каждой стороне
SCAN_EVERY = int(env("SCAN_INTERVAL_SEC", 30))
COOLDOWN = int(env("COOLDOWN_MIN", 30)) * 60        # не повторять ту же связку чаще
MAX_CHECKS = int(env("MAX_CHECKS", 12))             # сколько кандидатов проверять стаканом за цикл
ALLOW_UNVERIFIED = env("ALLOW_UNVERIFIED", "0") == "1"  # слать связки, где сеть не удалось проверить
MAX_NEG_FUNDING = float(env("MAX_NEG_FUNDING_PCT", 0.1))  # хедж: макс. фандинг против позиции за период, %
ENABLE_TRANSFER = env("ENABLE_TRANSFER", "1") == "1"
ENABLE_HEDGE = env("ENABLE_HEDGE", "1") == "1"

# Комиссии тейкера (доля). Поправь под свои уровни, если есть скидки.
SPOT_FEE = {"default": 0.001, "htx": 0.002, "mexc": 0.0005}
FUT_FEE = {"default": 0.0006, "bybit": 0.00055, "okx": 0.0005, "gate": 0.0005, "mexc": 0.0002}

# глубина стакана: у части бирж разрешены только свои значения
BOOK_LIMIT = {"htx": 150, "kucoin": 100}

NAMES = {"bybit": "Bybit", "okx": "OKX", "gate": "Gate", "kucoin": "KuCoin", "mexc": "MEXC",
         "bitget": "Bitget", "htx": "HTX", "bingx": "BingX", "binance": "Binance"}

NET_ALIASES = {
    "ERC20": ["ETH", "ERC20", "ETHEREUM", "ETHERC20"],
    "TRC20": ["TRX", "TRC20", "TRON"],
    "BEP20": ["BSC", "BEP20", "BNB", "BNBSMARTCHAIN", "BEP20BSC", "BSCBEP20"],
    "SOL": ["SOL", "SOLANA", "SPL"],
    "ARBITRUM": ["ARB", "ARBITRUM", "ARBONE", "ARBITRUMONE", "ARBEVM"],
    "OPTIMISM": ["OP", "OPTIMISM", "OPTIMISMETH", "OPETH"],
    "POLYGON": ["MATIC", "POLYGON", "POL"],
    "PLASMA": ["PLASMA", "XPL"],
    "MONAD": ["MON", "MONAD"],
    "KAIA": ["KAIA", "KLAY", "KLAYTN"],
    "STARKNET": ["STARK", "STARKNET"],
    "SONIC": ["S", "SONIC"],
    "POLKADOT": ["DOT", "POLKADOT"],
    "BERACHAIN": ["BERA", "BERACHAIN"],
    "AVAXC": ["AVAXC", "AVAXCCHAIN", "CCHAIN", "AVALANCHEC", "AVAXCC"],
    "BASE": ["BASE", "BASEEVM"],
    "TON": ["TON", "TONCOIN"],
    "APTOS": ["APT", "APTOS"],
    "SUI": ["SUI"],
    "NEAR": ["NEAR"],
    "ZKSYNC": ["ZKSYNC", "ZKSYNCERA", "ZKSERA"],
    "LINEA": ["LINEA"],
    "KAVAEVM": ["KAVAEVM"],
}
NET_MAP = {alias: canon for canon, aliases in NET_ALIASES.items() for alias in aliases}


def norm_net(name):
    key = re.sub(r"[^A-Z0-9]", "", str(name).upper())
    return NET_MAP.get(key, key)


def fee(table, ex_id):
    return table.get(ex_id, table["default"])


def nm(ex_id):
    return NAMES.get(ex_id, ex_id.capitalize())


def fp(x):
    return f"{x:.6g}"


# ----------------------------- математика стакана -----------------------------
def buy_for_quote(asks, quote):
    """Сколько базовой монеты купим на `quote` USDT, идя по аскам. None — мало глубины."""
    left, qty = quote, 0.0
    for price, amount in asks:
        cost = price * amount
        if cost >= left:
            return qty + left / price
        qty += amount
        left -= cost
    return None


def sell_base(bids, qty):
    """Сколько USDT получим, продав `qty` монеты по бидам. None — мало глубины."""
    left, got = qty, 0.0
    for price, amount in bids:
        if amount >= left:
            return got + left * price
        got += amount * price
        left -= amount
    return None


def transfer_net(asks, bids, size, fee_buy, fee_sell, withdraw_fee):
    """Чистый результат связки с переводом. Возвращает (net_доля, цена_покупки, цена_продажи) или None."""
    qty = buy_for_quote(asks, size)
    if not qty:
        return None
    qty_out = qty * (1 - fee_buy) - (withdraw_fee or 0.0)
    if qty_out <= 0:
        return None
    usdt = sell_base(bids, qty_out)
    if usdt is None:
        return None
    return usdt * (1 - fee_sell) / size - 1, size / qty, usdt / qty_out


def hedge_net(asks, bids, size, fee_long, fee_short):
    """Хедж: лонг по аскам, шорт по бидам. (спред, чистыми после 4 комиссий, цена лонга, цена шорта)."""
    qty = buy_for_quote(asks, size)
    if not qty:
        return None
    usdt = sell_base(bids, qty)
    if usdt is None:
        return None
    p_long, p_short = size / qty, usdt / qty
    spread = p_short / p_long - 1
    return spread, spread - 2 * (fee_long + fee_short), p_long, p_short


# ----------------------------- сканер -----------------------------------------
class Scanner:
    def __init__(self, spot, swap, send):
        self.spot = spot      # {id: ccxt-клиент спота}
        self.swap = swap      # {id: ccxt-клиент фьючерсов}
        self.send = send      # async-функция отправки текста
        self.currencies = {}  # {id: {COIN: currency}}
        self.sent = {}        # ключ связки -> (время, net)
        self.stats = {"cycles": 0, "signals": 0, "spot_ok": [], "swap_ok": [], "nets_ok": [], "last": 0}

    # ---- загрузка данных ----
    async def load(self):
        async def one(kind, ex_id, ex):
            try:
                await ex.load_markets()
                return kind, ex_id, True
            except Exception as e:
                log.warning("load %s %s: %s", kind, ex_id, str(e)[:150])
                return kind, ex_id, False

        jobs = [one("spot", i, e) for i, e in self.spot.items()] + [one("swap", i, e) for i, e in self.swap.items()]
        for kind, ex_id, ok in await asyncio.gather(*jobs):
            if not ok:
                (self.spot if kind == "spot" else self.swap).pop(ex_id, None)

    async def refresh_currencies(self):
        async def one(ex_id, ex):
            try:
                cur = await ex.fetch_currencies()
                if cur and any((c or {}).get("networks") for c in cur.values()):
                    self.currencies[ex_id] = cur
            except Exception as e:
                log.warning("currencies %s: %s", ex_id, str(e)[:150])

        await asyncio.gather(*[one(i, e) for i, e in self.spot.items()])
        self.stats["nets_ok"] = sorted(self.currencies)

    async def tickers(self, clients, want_swap):
        async def one(ex_id, ex):
            try:
                raw = await asyncio.wait_for(ex.fetch_tickers(), 25)
            except Exception as e:
                log.warning("tickers %s: %s", ex_id, str(e)[:120])
                return ex_id, {}
            out, now = {}, time.time() * 1000
            for sym, t in raw.items():
                m = ex.markets.get(sym)
                if not m or m.get("active") is False or m.get("quote") != "USDT":
                    continue
                if want_swap and not (m.get("swap") and m.get("linear") and m.get("settle") == "USDT"):
                    continue
                if not want_swap and not m.get("spot"):
                    continue
                bid, ask = t.get("bid"), t.get("ask")
                if not bid or not ask or bid <= 0 or ask < bid:
                    continue
                if t.get("timestamp") and now - t["timestamp"] > 120000:
                    continue
                vol = t.get("quoteVolume")
                if vol is None and t.get("baseVolume"):
                    vol = t["baseVolume"] * (m.get("contractSize") or 1 if want_swap else 1) * bid
                base = m["base"].upper()
                if base in BLACKLIST:
                    continue
                out[base] = {"sym": sym, "bid": bid, "ask": ask, "vol": vol or 0}
            return ex_id, out

        res = await asyncio.gather(*[one(i, e) for i, e in clients.items()])
        return {i: t for i, t in res if t}

    async def book(self, ex, sym):
        try:
            ob = await asyncio.wait_for(ex.fetch_order_book(sym, BOOK_LIMIT.get(ex.id, 50)), 15)
        except (ccxt.BadRequest, ccxt.ArgumentsRequired, ccxt.NotSupported):
            # биржа не приняла такую глубину — берём её глубину по умолчанию
            ob = await asyncio.wait_for(ex.fetch_order_book(sym), 15)
        m = ex.markets.get(sym) or {}
        k = (m.get("contractSize") or 1) if m.get("contract") else 1
        return [(p, a * k) for p, a, *_ in ob["asks"]], [(p, a * k) for p, a, *_ in ob["bids"]]

    # ---- сети ----
    def route(self, coin, src, dst, price):
        """Лучшая общая сеть src -> dst. ('ok', сеть, комиссия_в_монете|None) / ('closed',..) / ('unknown',..)."""
        a = (self.currencies.get(src) or {}).get(coin)
        b = (self.currencies.get(dst) or {}).get(coin)
        if not a or not b or not a.get("networks") or not b.get("networks"):
            return "unknown", None, None
        dst_nets = {norm_net(k): v for k, v in b["networks"].items()}
        best, unknown = None, False
        for k, w in a["networks"].items():
            d = dst_nets.get(norm_net(k))
            if not d:
                continue
            wd, dp = w.get("withdraw"), d.get("deposit")
            if wd is False or dp is False or w.get("active") is False or d.get("active") is False:
                continue
            if wd is None or dp is None:
                unknown = True
                continue
            f = w.get("fee")
            cost = f * price if f is not None else float("inf")
            if best is None or cost < best[0]:
                best = (cost, norm_net(k), f)
        if best:
            return "ok", best[1], best[2]
        return ("unknown" if unknown else "closed"), None, None

    def locked(self, coin, ex_id):
        """True, если на бирже по монете закрыт вывод или депозит во всех сетях (цена может не сойтись)."""
        nets = ((self.currencies.get(ex_id) or {}).get(coin) or {}).get("networks") or {}
        return bool(nets) and all(n.get("withdraw") is False or n.get("deposit") is False for n in nets.values())

    def fresh(self, key, net, mark=True):
        """Одна связка на монету и тип: повтор после кулдауна или если стало лучше в 1.5 раза (и минимум на 1 п.п.)."""
        prev = self.sent.get(key)
        now = time.time()
        if prev and now - prev[0] < COOLDOWN and net < max(prev[1] + 0.01, prev[1] * 1.5):
            return False
        if mark:
            self.sent[key] = (now, net)
        return True

    # ---- один проход ----
    async def cycle(self):
        spot = await self.tickers(self.spot, False)
        swap = await self.tickers(self.swap, True) if ENABLE_HEDGE else {}
        self.stats.update(spot_ok=sorted(spot), swap_ok=sorted(swap), last=time.time())
        self.stats["cycles"] += 1
        lo, hi = 0.0, MAX_SPREAD / 100

        def pairs(buy_side, sell_side, same_ok, floor):
            found = []
            coins = set().union(*[set(v) for v in buy_side.values()]) if buy_side else set()
            for coin in coins:
                for b_id, b in buy_side.items():
                    q = b.get(coin)
                    if not q or q["vol"] < MIN_VOLUME:
                        continue
                    for s_id, s in sell_side.items():
                        if s_id == b_id and not same_ok:
                            continue
                        r = s.get(coin)
                        if not r or r["vol"] < MIN_VOLUME:
                            continue
                        raw = r["bid"] / q["ask"] - 1
                        if max(lo, floor) <= raw <= hi:
                            found.append((raw, coin, b_id, s_id, q, r))
            return sorted(found, key=lambda x: -x[0])

        if ENABLE_TRANSFER:
            checked = 0
            for raw, coin, b_id, s_id, q, r in pairs(spot, spot, False, CFG["min_net"] / 100):
                if checked >= MAX_CHECKS:
                    break
                if not self.fresh(("T", coin), raw, mark=False):  # net <= raw, стаканы можно не трогать
                    continue
                status, net_name, wfee = self.route(coin, b_id, s_id, q["ask"])
                if status == "closed" or (status == "unknown" and not ALLOW_UNVERIFIED):
                    continue
                checked += 1
                try:
                    asks, _ = await self.book(self.spot[b_id], q["sym"])
                    _, bids = await self.book(self.spot[s_id], r["sym"])
                except Exception as e:
                    log.info("book %s %s/%s: %s", coin, b_id, s_id, str(e)[:100])
                    continue
                res = transfer_net(asks, bids, CFG["size"], fee(SPOT_FEE, b_id), fee(SPOT_FEE, s_id), wfee)
                if not res or res[0] * 100 < CFG["min_net"]:
                    continue
                net, p_buy, p_sell = res
                if not self.fresh(("T", coin), net):
                    continue
                await self.emit(self.msg_transfer(coin, b_id, s_id, net, p_buy, p_sell, raw, status, net_name, wfee, q, r))

        if ENABLE_HEDGE and swap:
            cands = [("SF",) + c for c in pairs(spot, swap, True, CFG["min_hedge"] / 100)]
            cands += [("FF",) + c for c in pairs(swap, swap, False, CFG["min_hedge"] / 100)]
            cands.sort(key=lambda x: -x[1])
            checked = 0
            for kind, raw, coin, b_id, s_id, q, r in cands:
                if checked >= MAX_CHECKS:
                    break
                if not self.fresh(("H", coin), raw, mark=False):
                    continue
                checked += 1
                long_ex = self.spot[b_id] if kind == "SF" else self.swap[b_id]
                fee_long = fee(SPOT_FEE if kind == "SF" else FUT_FEE, b_id)
                try:
                    asks, _ = await self.book(long_ex, q["sym"])
                    _, bids = await self.book(self.swap[s_id], r["sym"])
                except Exception as e:
                    log.info("book %s %s/%s: %s", coin, b_id, s_id, str(e)[:100])
                    continue
                res = hedge_net(asks, bids, CFG["size"], fee_long, fee(FUT_FEE, s_id))
                if not res or res[1] * 100 < CFG["min_hedge"]:
                    continue
                spread, net, p_long, p_short = res
                f_short = await self.funding(s_id, r["sym"])
                f_long = await self.funding(b_id, q["sym"]) if kind == "FF" else None
                # спред, который держится на фандинге против нас, — не арбитраж
                if ((f_short or 0) - (f_long or 0)) * 100 < -MAX_NEG_FUNDING:
                    continue
                if not self.fresh(("H", coin), net):
                    continue
                locked = kind == "SF" and self.locked(coin, b_id)
                if kind == "SF" and not locked and not ((self.currencies.get(b_id) or {}).get(coin) or {}).get("networks"):
                    locked = None  # статус сетей на споте неизвестен
                await self.emit(self.msg_hedge(kind, coin, b_id, s_id, spread, net, p_long, p_short, f_long, f_short, locked))

    async def funding(self, ex_id, sym):
        try:
            fr = await asyncio.wait_for(self.swap[ex_id].fetch_funding_rate(sym), 10)
            return fr.get("fundingRate")
        except Exception:
            return None

    async def emit(self, text):
        self.stats["signals"] += 1
        await self.send(text)

    # ---- тексты ----
    def msg_transfer(self, coin, b_id, s_id, net, p_buy, p_sell, raw, status, net_name, wfee, q, r):
        size = CFG["size"]
        c = html.escape(coin)
        if status == "ok":
            fee_txt = f"комиссия {fp(wfee)} {c} ≈ ${wfee * p_buy:.2f}" if wfee is not None else "комиссию вывода уточни на бирже"
            step2 = f"2. Вывести сетью <b>{html.escape(net_name)}</b> ({fee_txt})"
        else:
            step2 = "2. Вывести — ⚠️ сеть НЕ проверена, смотри вручную"
        return (
            f"🔁 <b>{c}</b> — перевод, чистыми <b>+{net * 100:.2f}%</b>\n\n"
            f"1. Купить на <b>{nm(b_id)}</b> (спот) ≈ {fp(p_buy)}\n"
            f"{step2}\n"
            f"3. Продать на <b>{nm(s_id)}</b> (спот) ≈ {fp(p_sell)}\n\n"
            f"Объём ${size:.0f} → прибыль ≈ <b>${size * net:.2f}</b>\n"
            f"Грязный спред {raw * 100:.2f}% · оборот 24ч ${q['vol'] / 1000:.0f}K / ${r['vol'] / 1000:.0f}K\n"
            f"Перед входом сверь контракт монеты и что депозит открыт."
        )

    def msg_hedge(self, kind, coin, b_id, s_id, spread, net, p_long, p_short, f_long, f_short, locked=False):
        c = html.escape(coin)
        title = "спот → фьючерс" if kind == "SF" else "фьючерс → фьючерс"
        leg1 = (f"1. Купить спот на <b>{nm(b_id)}</b> ≈ {fp(p_long)}" if kind == "SF"
                else f"1. Лонг перп на <b>{nm(b_id)}</b> ≈ {fp(p_long)}")
        fund_txt = ""
        if f_short is not None and (kind == "SF" or f_long is not None):
            carry = (f_short - (f_long or 0)) * 100  # сколько связка получает (+) или платит (-) за период
            side = "тебе платят" if carry > 0 else ("платишь ты" if carry < 0 else "ноль")
            fund_txt = f"Фандинг связки: {carry:+.3f}% за период ({side})\n"
        return (
            f"⚖️ <b>{c}</b> — {title}, спред <b>{spread * 100:.2f}%</b>\n\n"
            f"{leg1}\n"
            f"2. Шорт перп на <b>{nm(s_id)}</b> ≈ {fp(p_short)}\n"
            f"3. Закрыть обе ноги, когда цены сойдутся\n\n"
            f"После комиссий (вход+выход) ≈ <b>+{net * 100:.2f}%</b>, на ${CFG['size']:.0f} ≈ ${CFG['size'] * net:.2f}\n"
            f"{fund_txt}"
            + (f"⚠️ На {nm(b_id)} по {c} закрыт вывод/депозит — спот может долго не сходиться.\n" if locked else "")
            + (f"❔ Статус вывода {c} на {nm(b_id)} неизвестен (нужен API-ключ) — проверь вручную.\n" if locked is None else "")
            + f"Плечо 1–2x и запас маржи: цена может сначала разойтись сильнее."
        )


# ----------------------------- Telegram ---------------------------------------
class Telegram:
    def __init__(self, token, chat):
        self.base = f"https://api.telegram.org/bot{token}/"
        self.chat = chat
        self.session = None
        self.offset = 0

    async def call(self, method, **params):
        if self.session is None:
            self.session = aiohttp.ClientSession()
        try:
            async with self.session.post(self.base + method, json=params,
                                         timeout=aiohttp.ClientTimeout(total=40)) as r:
                return await r.json()
        except Exception as e:
            log.warning("telegram %s: %s", method, str(e)[:120])
            return {}

    async def send(self, text, chat=None):
        target = chat or self.chat
        if not target:
            log.info("нет TELEGRAM_CHAT_ID, сигнал:\n%s", text)
            return
        await self.call("sendMessage", chat_id=target, text=text, parse_mode="HTML",
                        disable_web_page_preview=True)

    async def poll(self, scanner):
        while True:
            data = await self.call("getUpdates", offset=self.offset, timeout=25)
            for upd in data.get("result", []):
                self.offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                text, chat = (msg.get("text") or "").strip(), str((msg.get("chat") or {}).get("id", ""))
                uid = str((msg.get("from") or {}).get("id", ""))
                if not text.startswith("/"):
                    continue
                cmd = text.split()[0].split("@")[0].lower()
                ids = f"id этого чата: <code>{chat}</code>\nтвой user id: <code>{uid}</code>"
                # управлять может только админ (TELEGRAM_ADMIN_ID); если он не задан — только чат сигналов
                allowed = (uid == TG_ADMIN) if TG_ADMIN else (not self.chat or chat == self.chat)
                if not allowed:
                    if cmd in ("/start", "/id") and (not TG_ADMIN or not self.chat):
                        await self.send(ids, chat)  # подсказка id, пока настройка не закончена
                    continue
                await self.send(ids if cmd == "/id" else command(text, chat, scanner), chat)
            if not data.get("ok"):
                await asyncio.sleep(5)


def command(text, chat, sc):
    parts = text.split()
    cmd, arg = parts[0].split("@")[0].lower(), (parts[1] if len(parts) > 1 else "")

    def num():
        try:
            v = float(arg.replace(",", "."))
            return v if v > 0 else None
        except ValueError:
            return None

    if cmd == "/start" or cmd == "/help":
        return (f"Арбитражный сканер.\nТвой chat id: <code>{chat}</code>\n\n"
                "/status — состояние\n/min 1.5 — мин. чистый % (перевод)\n/hedge 1.5 — мин. чистый % (хедж)\n"
                "/size 500 — объём связки в USDT\n/pause и /resume — пауза\n/id — id чата и твой user id")
    if cmd == "/status":
        s = sc.stats
        ago = f"{time.time() - s['last']:.0f} сек назад" if s["last"] else "ещё не было"
        return (f"{'⏸ пауза' if CFG['paused'] else '▶️ работает'}\n"
                f"Объём ${CFG['size']:.0f} · мин. перевод {CFG['min_net']}% · мин. хедж {CFG['min_hedge']}%\n"
                f"Спот: {', '.join(s['spot_ok']) or '—'}\nФьючерсы: {', '.join(s['swap_ok']) or '—'}\n"
                f"Сети проверяются: {', '.join(s['nets_ok']) or '—'}\n"
                f"Циклов {s['cycles']}, сигналов {s['signals']}, последний скан {ago}")
    if cmd in ("/min", "/hedge", "/size"):
        v = num()
        if v is None:
            return "Нужно число, например: " + cmd + " 1.5"
        CFG[{"/min": "min_net", "/hedge": "min_hedge", "/size": "size"}[cmd]] = v
        return f"Ок: {cmd[1:]} = {v}"
    if cmd == "/pause":
        CFG["paused"] = True
        return "Пауза."
    if cmd == "/resume":
        CFG["paused"] = False
        return "Работаю."
    return "Не знаю такой команды. /help"


# ----------------------------- запуск -----------------------------------------
def make_clients():
    spot, swap = {}, {}
    for ex_id in EXCHANGES:
        if not hasattr(ccxt, ex_id):
            log.warning("ccxt не знает биржу %s", ex_id)
            continue
        cfg = {"enableRateLimit": True, "timeout": 20000}
        up = ex_id.upper()
        if os.getenv(f"{up}_API_KEY"):  # необязательные read-only ключи — нужны только для статуса сетей
            cfg.update(apiKey=os.getenv(f"{up}_API_KEY"), secret=os.getenv(f"{up}_SECRET", ""))
            if os.getenv(f"{up}_PASSWORD"):
                cfg["password"] = os.getenv(f"{up}_PASSWORD")
        spot[ex_id] = getattr(ccxt, ex_id)({**cfg, "options": {"defaultType": "spot"}})
        if ENABLE_HEDGE:
            swap[ex_id] = getattr(ccxt, ex_id)({"enableRateLimit": True, "timeout": 20000,
                                               "options": {"defaultType": "swap"}})
    return spot, swap


async def main():
    if not TG_TOKEN:
        raise SystemExit("Задай TELEGRAM_BOT_TOKEN")
    tg = Telegram(TG_TOKEN, TG_CHAT)
    spot, swap = make_clients()
    sc = Scanner(spot, swap, tg.send)
    asyncio.create_task(tg.poll(sc))
    await sc.load()
    await sc.refresh_currencies()
    await tg.send("Сканер запущен.\n" + command("/status", TG_CHAT, sc), TG_ADMIN or None)
    last_cur = time.time()
    try:
        while True:
            started = time.time()
            if not CFG["paused"]:
                try:
                    await sc.cycle()
                except Exception:
                    log.exception("cycle")
            if time.time() - last_cur > 900:
                await sc.refresh_currencies()
                last_cur = time.time()
            await asyncio.sleep(max(3, SCAN_EVERY - (time.time() - started)))
    finally:
        for ex in list(spot.values()) + list(swap.values()):
            await ex.close()


if __name__ == "__main__":
    asyncio.run(main())
