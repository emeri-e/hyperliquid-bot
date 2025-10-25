import asyncio
import json
import time
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Deque, Dict, Set, Optional, List, Tuple

import aiohttp
import websockets
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message

@dataclass
class TradePoint:
    ts_ms: int
    price: float

#utils
def trim_window(window: Deque[TradePoint]):
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - 24 * 3600 * 1000
    while window and window[0].ts_ms < cutoff:
        window.popleft()

def compute_pct_change(window: Deque[TradePoint], min_trades: int) -> Optional[float]:
    if len(window) < min_trades:
        return None
    old = window[0].price
    new = window[-1].price
    if old <= 0:
        return None
    return (new - old) / old * 100.0

#rest helpers
async def fetch_candle_snapshot(session: aiohttp.ClientSession, info_url: str,
                                token: str, start_ms: int, end_ms: int, gran_ms: int):
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": token, "startTime": start_ms, "endTime": end_ms, "interval": "1m"}
    }
    try:
        async with session.post(info_url, json=payload, timeout=20) as resp:
            if resp.status != 200:
                print(f"[prepopulate] HTTP {resp.status} for {token}")
                return None
            return await resp.json()
    except Exception as e:
        print(f"[prepopulate] exception for {token}: {e}")
        return None

def parse_candles(resp) -> Optional[List[dict]]:
    if not resp:
        return None
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        for k in ("candles", "data", "result", "rows"):
            if k in resp and isinstance(resp[k], list):
                return resp[k]
        if "time" in resp and ("close" in resp or "c" in resp):
            return [resp]
    return None

def add_candles_to_window(window: Deque[TradePoint], candles: List[dict]) -> int:
    added = 0
    for c in candles:
        try:
            t = int(c.get("time") or c.get("t") or c.get("ts") or 0)
            close = float(c.get("close") or c.get("c") or c.get("px") or 0)
            if t <= 0 or close <= 0:
                continue
            window.append(TradePoint(ts_ms=t, price=close))
            added += 1
        except Exception:
            continue

    sorted_list = sorted(window, key=lambda x: x.ts_ms)
    window.clear()
    for p in sorted_list:
        window.append(p)
    trim_window(window)
    return added

async def prepopulate_if_needed(session: aiohttp.ClientSession, info_url: str,
                                windows: Dict[str, Deque[TradePoint]], token: str,
                                min_trades: int, gran_ms: int):
    if len(windows.get(token, deque())) >= min_trades:
        return
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 24 * 3600 * 1000 - 5 * 60 * 1000
    resp = await fetch_candle_snapshot(session, info_url, token, start_ms, now_ms, gran_ms)
    candles = parse_candles(resp)
    if not candles:
        print(f"[prepopulate] no candles for {token}")
        return
    windows.setdefault(token, deque())
    added = add_candles_to_window(windows[token], candles)
    print(f"[prepopulate] added {added} points for {token} (window now {len(windows[token])})")

#tg helpers
async def tg_send(session: aiohttp.ClientSession, bot_token: str, chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload, timeout=10) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"[tg] send failed {resp.status} -> {body}")
    except Exception as e:
        print("[tg] exception:", e)

#ws funcs
async def ws_subscribe(ws, token: str):
    # sub_msg = { "method": "subscribe", "subscription": { "type": "candle", "coin": token, "interval": "1m" } }
    sub_msg = {"method": "subscribe", "subscription": {"type": "trades", "coin": token}}
    try:
        await ws.send(json.dumps(sub_msg))
    except Exception as e:
        print(f"[ws] subscribe {token} error: {e}")

async def handle_ws_message(raw: str, windows: Dict[str, Deque[TradePoint]]):
    try:
        msg = json.loads(raw)
    except Exception:
        return
    if isinstance(msg, dict) and "payload" in msg and isinstance(msg["payload"], dict):
        msg = msg["payload"]

    if isinstance(msg, dict) and msg.get("channel"):
        ch = msg.get("channel", "").lower()
        if ch in ("trades", "recenttrades"):
            arr = msg.get("data")
            for t in arr:
                coin = t.get("coin")
                ts_ms = int(t.get("time") or t.get("t") or int(time.time() * 1000))
                price = float(t.get("px") or t.get("price") or 0)
                if price > 0:
                    windows.setdefault(coin, deque()).append(TradePoint(ts_ms=ts_ms, price=price))
            trim_window(windows.setdefault(coin, deque()))
            return

async def run_ws_worker(ws_url: str,
                        info_url: str,
                        session: aiohttp.ClientSession,
                        bot_token: str,
                        windows: Dict[str, Deque[TradePoint]],
                        token_subscribers: Dict[str, Set[int]],
                        user_subscriptions: Dict[int, Set[str]],
                        ws_subscribe_queue: asyncio.Queue,
                        min_trades: int,
                        gran_ms: int,
                        threshold_pct: float,
                        alert_cooldown: int,
                        stop_event: asyncio.Event):
    last_alert_ts: Dict[str, float] = defaultdict(lambda: 0.0)

    for tok in list(token_subscribers.keys()):
        await prepopulate_if_needed(session, info_url, windows, tok, min_trades, gran_ms)

    while not stop_event.is_set():
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10, max_size=2**24) as ws:
                print("[ws] connected")
                # subscribe to current tokens
                for tok in list(token_subscribers.keys()):
                    await ws_subscribe(ws, tok)
                    await asyncio.sleep(0.01)

                async def repl_subscribe_loop():
                    while not stop_event.is_set():
                        try:
                            tok = await ws_subscribe_queue.get()
                            await ws_subscribe(ws, tok)
                        except asyncio.CancelledError:
                            break
                        except Exception as e:
                            print(f"[ws subscribe loop] error: {e}")

                subscribe_task = asyncio.create_task(repl_subscribe_loop())

                async for raw in ws:
                    print("[ws] message received")
                    await handle_ws_message(raw, windows)

                    now_ts = time.time()
                    for token, subs in list(token_subscribers.items()):
                        if not subs:
                            continue
                        window = windows.get(token)
                        if not window:
                            continue
                        pct = compute_pct_change(window, min_trades)
                        if pct is None:
                            continue
                        if pct >= threshold_pct and (now_ts - last_alert_ts.get(token, 0.0) >= alert_cooldown):
                            newest = window[-1].price
                            oldest = window[0].price
                            msg_text = f"🚨 <b>{token}</b> pumped {pct:.1f}% in last 24h\nNow: {newest:.6f}\n24h ago: {oldest:.6f}"
                            # send alert to all subscribers of token
                            for chat_id in list(subs):
                                await tg_send(session, bot_token, chat_id, msg_text)
                            last_alert_ts[token] = now_ts

                    if stop_event.is_set():
                        break

                subscribe_task.cancel()
        except Exception as e:
            print("[ws] connection error:", e)
            await asyncio.sleep(5)

#tg funcs
def register_aiogram_handlers(dp: Dispatcher,
                              session: aiohttp.ClientSession,
                              bot_token: str,
                              windows: Dict[str, Deque[TradePoint]],
                              token_subscribers: Dict[str, Set[int]],
                              user_subscriptions: Dict[int, Set[str]],
                              info_url: str,
                              min_trades: int,
                              gran_ms: int,
                              ws_subscribe_queue: asyncio.Queue):
    # subscribe
    async def cmd_subscribe(message: Message):
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply("Usage: /subscribe <TOKEN>")
            return
        token = parts[1].upper()
        user_subscriptions.setdefault(message.chat.id, set()).add(token)
        token_subscribers.setdefault(token, set()).add(message.chat.id)
        windows.setdefault(token, deque())

        await prepopulate_if_needed(session, info_url, windows, token, min_trades, gran_ms)
        await ws_subscribe_queue.put(token)
        await message.reply(f"Subscribed to {token}. You will receive alerts for it.")

    # unsubscribe
    async def cmd_unsubscribe(message: Message):
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply("Usage: /unsubscribe <TOKEN>")
            return
        token = parts[1].upper()
        token_subscribers.get(token, set()).discard(message.chat.id)
        user_subscriptions.get(message.chat.id, set()).discard(token)
        await message.reply(f"Unsubscribed from {token}.")

    # list global tokens
    async def cmd_list(message: Message):
        tokens = ", ".join(sorted(token_subscribers.keys())) or "(none)"
        await message.reply(f"Watching (global): {tokens}")

    # list personal
    async def cmd_mytokens(message: Message):
        my = ", ".join(sorted(user_subscriptions.get(message.chat.id, set()))) or "(none)"
        await message.reply(f"Your subscriptions: {my}")

    # help
    async def cmd_help(message: Message):
        await message.reply("/subscribe <TOKEN>\n/unsubscribe <TOKEN>\n/list\n/mytokens\n/help")

    dp.message.register(cmd_subscribe, Command(commands=["subscribe"]))
    dp.message.register(cmd_unsubscribe, Command(commands=["unsubscribe"]))
    dp.message.register(cmd_list, Command(commands=["list"]))
    dp.message.register(cmd_mytokens, Command(commands=["mytokens"]))
    dp.message.register(cmd_help, Command(commands=["help"]))

async def start_bot(
        bot_token,
        threshold_pct,
        windows: Dict[str, Deque[TradePoint]] = {},
        token_subscribers: Dict[str, Set[int]] = defaultdict(set),
        user_subscriptions: Dict[int, Set[str]] = defaultdict(set),
        ws_subscribe_queue: asyncio.Queue = asyncio.Queue(),
        stop_event = asyncio.Event(),
):
    
    async with aiohttp.ClientSession() as session:
        bot = Bot(token=bot_token) #, parse_mode="HTML")
        dp = Dispatcher()

        register_aiogram_handlers(dp, session, bot_token, windows, token_subscribers,
                                  user_subscriptions, "https://api.hyperliquid.xyz/info",
                                  4, 60 * 1000, ws_subscribe_queue)

        async def start_aiogram():
            try:
                await dp.start_polling(bot)
            except Exception as e:
                print("[aiogram] polling error:", e)

        aiogram_task = asyncio.create_task(start_aiogram())

        # run websocket worker
        ws_task = asyncio.create_task(run_ws_worker(
            ws_url="wss://api.hyperliquid.xyz/ws",
            info_url="https://api.hyperliquid.xyz/info",
            session=session,
            bot_token=bot_token,
            windows=windows,
            token_subscribers=token_subscribers,
            user_subscriptions=user_subscriptions,
            ws_subscribe_queue=ws_subscribe_queue,
            min_trades=4,
            gran_ms=60 * 1000,
            threshold_pct=threshold_pct,
            alert_cooldown=60 * 60,
            stop_event=stop_event
        ))

        print("[main] Bot running. Use /subscribe <TOKEN> in Telegram to subscribe.")
        try:
            await asyncio.gather(aiogram_task, ws_task)
        except asyncio.CancelledError:
            pass
        finally:
            stop_event.set()
            await bot.session.close()

if __name__ == "__main__":
    BOT_TOKEN='8055323488:AAEe5ZBr203KyMbKq7GF9HXlb-OejkpSvfk'
    THRESHOLD_PCT=20.0

    asyncio.run(start_bot(
        bot_token=BOT_TOKEN,
        threshold_pct=THRESHOLD_PCT
    ))
