#!/usr/bin/env python3
import asyncio
import json
import os
import re
import subprocess
import time
from decimal import Decimal, ROUND_CEILING
from html import escape as html_escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes


load_dotenv()


def _env_str(key: str, default: str) -> str:
    v = os.getenv(key)
    return v if v is not None and v != "" else default


def _env_float(key: str, default: float) -> float:
    try:
        return float(_env_str(key, str(default)))
    except Exception:
        return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(_env_str(key, str(default)))
    except Exception:
        return default


TELEGRAM_BOT_TOKEN = _env_str("TELEGRAM_BOT_TOKEN", "")
ALLOWED_CHAT_IDS = {
    int(x.strip()) for x in _env_str("ALLOWED_CHAT_IDS", "").split(",") if x.strip()
}
STATE_FILE = _env_str("STATE_FILE", "state.json")
MANUAL_LOG_FILE = _env_str("MANUAL_LOG_FILE", "manual_execution.log")
TELEGRAM_RUNTIME_STATE = _env_str("TELEGRAM_RUNTIME_STATE", "telegram_runtime_state.json")
BOTLOGIC_PATH = _env_str("BOTLOGIC_PATH", "botlogic.py")
PYTHON_BIN = _env_str("PYTHON_BIN", "python3")
SNAPSHOT_TOP_N = _env_int("SNAPSHOT_TOP_N", 10)
SNAPSHOT_TTL_HOURS = _env_float("SNAPSHOT_TTL_HOURS", 2.0)
CMD_TIMEOUT_SEC = _env_int("CMD_TIMEOUT_SEC", 240)

_CHAT_LOCKS: Dict[int, asyncio.Lock] = {}


def _chat_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in _CHAT_LOCKS:
        _CHAT_LOCKS[chat_id] = asyncio.Lock()
    return _CHAT_LOCKS[chat_id]


def _load_runtime_state() -> Dict[str, Any]:
    p = Path(TELEGRAM_RUNTIME_STATE)
    if not p.exists():
        return {"by_chat": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("by_chat", {})
            return data
    except Exception:
        pass
    return {"by_chat": {}}


def _save_runtime_state(state: Dict[str, Any]) -> None:
    p = Path(TELEGRAM_RUNTIME_STATE)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(p)


def _set_last_snapshot_id(chat_id: int, snapshot_id: str) -> None:
    state = _load_runtime_state()
    by_chat = state.setdefault("by_chat", {})
    rec = by_chat.setdefault(str(chat_id), {})
    rec["last_snapshot_id"] = snapshot_id
    rec["updated_ts"] = time.time()
    _save_runtime_state(state)


def _get_last_snapshot_id(chat_id: int) -> Optional[str]:
    state = _load_runtime_state()
    by_chat = state.get("by_chat", {})
    rec = by_chat.get(str(chat_id), {})
    sid = rec.get("last_snapshot_id")
    return str(sid) if sid else None


def _load_main_state() -> Dict[str, Any]:
    p = Path(STATE_FILE)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _get_snapshot(snapshot_id: str) -> Optional[Dict[str, Any]]:
    state = _load_main_state()
    snaps = state.get("snapshots", {})
    snap = snaps.get(snapshot_id)
    return snap if isinstance(snap, dict) else None


def _compact_leg_name(s: Any, max_words: int = 2, max_len: int = 30) -> str:
    txt = str(s or "").strip()
    if not txt:
        return "unknown"
    txt = re.sub(r"^\s*Will\s+", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\?\s*$", "", txt).strip()
    m_cond = re.search(
        r"\bbe\s+(less than|greater than|more than|under|over|between|at least|at most|exactly)\s+(.+)$",
        txt,
        flags=re.IGNORECASE,
    )
    if m_cond:
        txt = f"{m_cond.group(1).lower()} {m_cond.group(2).strip()}"
    elif re.search(r"\bnot meet\b", txt, flags=re.IGNORECASE):
        txt = "Not meet"
    else:
        m_meet = re.search(r"\bmeet next in\s+(.+)$", txt, flags=re.IGNORECASE)
        if m_meet:
            txt = m_meet.group(1).strip()
        else:
            cut = re.split(r"\s+(win|be|have|get|become|make|take)\b", txt, maxsplit=1, flags=re.IGNORECASE)
            if cut and cut[0].strip():
                txt = cut[0].strip()
    words = txt.split()
    if max_words > 0 and len(words) > max_words:
        txt = " ".join(words[:max_words])
    if len(txt) <= max_len:
        return txt
    return txt[: max_len - 3].rstrip() + "..."


def _ceil_percent(value: float) -> int:
    # Use Decimal to avoid float precision edge cases when applying ceil.
    pct = Decimal(str(value)) * Decimal("100")
    return int(pct.to_integral_value(rounding=ROUND_CEILING))


def _candidate_line(c: Dict[str, Any]) -> str:
    status = str(c.get("status") or "unknown")
    idx = int(c.get("idx") or 0)
    title = str(c.get("event_title") or "unknown")
    snap = c.get("snapshot_metrics") or {}
    latest = c.get("latest_metrics") or {}
    metrics = latest if latest else snap

    a_side = str(metrics.get("depth_a_is") or "leg1")
    b_side = str(metrics.get("depth_b_is") or "leg2")
    leg1 = str(c.get("leg1_name") or "")
    leg2 = str(c.get("leg2_name") or "")
    a_name = _compact_leg_name(leg1 if a_side == "leg1" else leg2)
    b_name = _compact_leg_name(leg2 if b_side == "leg2" else leg1)

    total_cost = float(latest.get("total_cost", snap.get("total_cost", 0.0)) or 0.0)
    a_size = float(latest.get("a_size", snap.get("a_size", 0.0)) or 0.0)
    b_size = float(latest.get("b_size", snap.get("b_size", 0.0)) or 0.0)
    a_vwap = float(latest.get("a_vwap", snap.get("a_vwap", 0.0)) or 0.0)
    b_vwap = float(latest.get("b_vwap", snap.get("b_vwap", 0.0)) or 0.0)
    edge = float(latest.get("a_win_roi", snap.get("a_win_roi", 0.0)) or 0.0)

    return (
        f"[{idx}] {status} | {title} | total_cost=${total_cost:,.2f} "
        f"A={a_name} {a_size:.2f}@{a_vwap:.4f} "
        f"B={b_name} {b_size:.2f}@{b_vwap:.4f} edge={edge:.2%}"
    )


def _candidate_view(c: Dict[str, Any]) -> Dict[str, Any]:
    status = str(c.get("status") or "unknown")
    idx = int(c.get("idx") or 0)
    title = str(c.get("event_title") or "unknown")
    snap = c.get("snapshot_metrics") or {}
    latest = c.get("latest_metrics") or {}
    metrics = latest if latest else snap

    a_side = str(metrics.get("depth_a_is") or "leg1")
    b_side = str(metrics.get("depth_b_is") or "leg2")
    leg1 = str(c.get("leg1_name") or "")
    leg2 = str(c.get("leg2_name") or "")
    a_name = _compact_leg_name(leg1 if a_side == "leg1" else leg2)
    b_name = _compact_leg_name(leg2 if b_side == "leg2" else leg1)

    total_cost = float(latest.get("total_cost", snap.get("total_cost", 0.0)) or 0.0)
    a_size = float(latest.get("a_size", snap.get("a_size", 0.0)) or 0.0)
    b_size = float(latest.get("b_size", snap.get("b_size", 0.0)) or 0.0)
    a_vwap = float(latest.get("a_vwap", snap.get("a_vwap", 0.0)) or 0.0)
    b_vwap = float(latest.get("b_vwap", snap.get("b_vwap", 0.0)) or 0.0)
    edge = float(latest.get("a_win_roi", snap.get("a_win_roi", 0.0)) or 0.0)
    return {
        "status": status,
        "idx": idx,
        "title": title,
        "total_cost": total_cost,
        "a_name": a_name,
        "b_name": b_name,
        "a_size": a_size,
        "b_size": b_size,
        "a_vwap_pct": _ceil_percent(a_vwap),
        "b_vwap_pct": _ceil_percent(b_vwap),
        "edge_pct": edge * 100.0,
    }


def _snapshot_lines(snapshot: Dict[str, Any], include_statuses: Optional[set] = None) -> List[str]:
    out: List[str] = []
    cands = snapshot.get("candidates") or []
    for c in cands:
        if not isinstance(c, dict):
            continue
        st = str(c.get("status") or "")
        if include_statuses is not None and st not in include_statuses:
            continue
        out.append(_candidate_line(c))
    return out


def _render_snapshot_report(
    snapshot_id: str,
    snapshot: Dict[str, Any],
    include_statuses: Optional[set] = None,
    title: str = "SNAPSHOT REPORT",
    include_status_on_title: bool = False,
) -> str:
    cands = snapshot.get("candidates") or []
    rows: List[Dict[str, Any]] = []
    for c in cands:
        if not isinstance(c, dict):
            continue
        st = str(c.get("status") or "")
        if include_statuses is not None and st not in include_statuses:
            continue
        rows.append(_candidate_view(c))

    parts: List[str] = [
        f"📊 <b>{html_escape(title)}</b>",
        f"🆔 <code>{html_escape(snapshot_id)}</code>",
        f"📦 Candidates: <b>{len(rows)}</b>",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    for r in rows:
        name_line = html_escape(r["title"])
        if include_status_on_title:
            name_line = f"{name_line} <i>({html_escape(r['status'])})</i>"
        parts.extend(
            [
                "",
                f"<b>[{r['idx']}]</b> {name_line}",
                f"💰 Cost: ${r['total_cost']:,.2f} | 📈 Edge: {r['edge_pct']:.2f}%",
                f"🔵 A: {html_escape(r['a_name'])} — {r['a_size']:,.2f} @ {r['a_vwap_pct']:.2f}",
                f"🟠 B: {html_escape(r['b_name'])} — {r['b_size']:,.2f} @ {r['b_vwap_pct']:.2f}",
            ]
        )
    return "\n".join(parts)


async def _reply_long_html(update: Update, text: str) -> None:
    if not update.message:
        return
    max_len = 3800
    if len(text) <= max_len:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
        return
    chunks: List[str] = []
    cur = ""
    for line in text.splitlines(keepends=True):
        if len(cur) + len(line) > max_len and cur:
            chunks.append(cur)
            cur = line
        else:
            cur += line
    if cur:
        chunks.append(cur)
    for ch in chunks:
        await update.message.reply_text(ch, parse_mode=ParseMode.HTML)


def _run_botlogic(args: List[str]) -> Tuple[int, str, str]:
    cmd = [PYTHON_BIN, BOTLOGIC_PATH] + args
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=CMD_TIMEOUT_SEC)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


async def _deny_if_not_allowed(update: Update) -> bool:
    chat = update.effective_chat
    if chat is None:
        return True
    if ALLOWED_CHAT_IDS and chat.id not in ALLOWED_CHAT_IDS:
        if update.message:
            await update.message.reply_text("Unauthorized chat.")
        return True
    return False


async def _with_lock(update: Update):
    chat = update.effective_chat
    if chat is None:
        return None
    lock = _chat_lock(chat.id)
    await lock.acquire()
    return lock


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _deny_if_not_allowed(update):
        return
    lock = await _with_lock(update)
    try:
        args = [
            "snapshot", "create",
            "--state-file", STATE_FILE,
            "--manual-log", MANUAL_LOG_FILE,
            "--top-n", str(SNAPSHOT_TOP_N),
            "--snapshot-ttl-hours", str(SNAPSHOT_TTL_HOURS),
        ]
        rc, out, err = await asyncio.to_thread(_run_botlogic, args)
        if rc != 0:
            await update.message.reply_text(f"scan failed\n{err or out}")
            return
        m = re.search(r"snapshot_id:\s*(\S+)", out)
        if not m:
            await update.message.reply_text(f"scan completed, but snapshot_id not found\n{out}")
            return
        sid = m.group(1)
        _set_last_snapshot_id(update.effective_chat.id, sid)
        snap = _get_snapshot(sid)
        if not snap:
            await update.message.reply_text(f"snapshot created: {sid}\n(no snapshot payload found)")
            return
        text = _render_snapshot_report(sid, snap, title="SNAPSHOT REPORT")
        await _reply_long_html(update, text)
    finally:
        if lock:
            lock.release()


async def _resolve_snapshot_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    if context.args:
        first = context.args[0].strip()
        if first.startswith("snap_"):
            return first
    sid = _get_last_snapshot_id(update.effective_chat.id)
    if not sid and update.message:
        await update.message.reply_text("No active snapshot. Run /scan first.")
    return sid


async def cmd_show(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _deny_if_not_allowed(update):
        return
    lock = await _with_lock(update)
    try:
        sid = await _resolve_snapshot_id(update, context)
        if not sid:
            return
        snap = _get_snapshot(sid)
        if not snap:
            await update.message.reply_text(f"snapshot not found: {sid}")
            return
        text = _render_snapshot_report(sid, snap, title="SNAPSHOT REPORT", include_status_on_title=True)
        await _reply_long_html(update, text)
    finally:
        if lock:
            lock.release()


async def cmd_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _deny_if_not_allowed(update):
        return
    lock = await _with_lock(update)
    try:
        if not context.args:
            await update.message.reply_text("usage: /select 1,3,7")
            return
        sid = _get_last_snapshot_id(update.effective_chat.id)
        if not sid:
            await update.message.reply_text("No active snapshot. Run /scan first.")
            return
        idxs = context.args[0].strip()
        args = [
            "snapshot", "select", sid, idxs,
            "--state-file", STATE_FILE,
            "--manual-log", MANUAL_LOG_FILE,
            "--snapshot-ttl-hours", str(SNAPSHOT_TTL_HOURS),
        ]
        rc, out, err = await asyncio.to_thread(_run_botlogic, args)
        if rc != 0:
            await update.message.reply_text(f"select failed\n{err or out}")
            return
        snap = _get_snapshot(sid)
        if not snap:
            await update.message.reply_text(f"selected, but snapshot missing: {sid}")
            return
        text = _render_snapshot_report(
            sid,
            snap,
            include_statuses={"selected"},
            title="SELECTED CANDIDATES",
            include_status_on_title=True,
        )
        await _reply_long_html(update, text)
    finally:
        if lock:
            lock.release()


async def cmd_revalidate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _deny_if_not_allowed(update):
        return
    lock = await _with_lock(update)
    try:
        sid = _get_last_snapshot_id(update.effective_chat.id)
        if not sid:
            await update.message.reply_text("No active snapshot. Run /scan first.")
            return
        args = [
            "snapshot", "revalidate", sid,
            "--state-file", STATE_FILE,
            "--manual-log", MANUAL_LOG_FILE,
            "--snapshot-ttl-hours", str(SNAPSHOT_TTL_HOURS),
        ]
        if context.args:
            args.extend(["--indices", context.args[0].strip()])
        rc, out, err = await asyncio.to_thread(_run_botlogic, args)
        if rc != 0:
            await update.message.reply_text(f"revalidate failed\n{err or out}")
            return
        snap = _get_snapshot(sid)
        if not snap:
            await update.message.reply_text(f"revalidated, but snapshot missing: {sid}")
            return
        head = [
            "📊 <b>REVALIDATION REPORT</b>",
            f"🆔 <code>{html_escape(sid)}</code>",
            "━━━━━━━━━━━━━━━━━━━━",
        ]
        good = _render_snapshot_report(
            sid, snap, include_statuses={"revalidated"}, title="REVALIDATED", include_status_on_title=False
        )
        bad = _render_snapshot_report(
            sid, snap, include_statuses={"expired"}, title="EXPIRED", include_status_on_title=True
        )
        text = "\n".join(head) + "\n\n" + good + "\n\n" + bad
        await _reply_long_html(update, text)
    finally:
        if lock:
            lock.release()


async def cmd_approve(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _deny_if_not_allowed(update):
        return
    lock = await _with_lock(update)
    try:
        sid = _get_last_snapshot_id(update.effective_chat.id)
        if not sid:
            await update.message.reply_text("No active snapshot. Run /scan first.")
            return
        args = [
            "snapshot", "approve", sid,
            "--state-file", STATE_FILE,
            "--manual-log", MANUAL_LOG_FILE,
            "--snapshot-ttl-hours", str(SNAPSHOT_TTL_HOURS),
        ]
        if context.args:
            args.extend(["--indices", context.args[0].strip()])
        rc, out, err = await asyncio.to_thread(_run_botlogic, args)
        if rc != 0:
            await update.message.reply_text(f"approve failed\n{err or out}")
            return
        snap = _get_snapshot(sid)
        if not snap:
            await update.message.reply_text(f"approved, but snapshot missing: {sid}")
            return
        text = _render_snapshot_report(
            sid,
            snap,
            include_statuses={"approved_manual"},
            title="APPROVED FOR MANUAL EXECUTION",
            include_status_on_title=True,
        )
        await _reply_long_html(update, text)
    finally:
        if lock:
            lock.release()


async def cmd_filled(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _deny_if_not_allowed(update):
        return
    lock = await _with_lock(update)
    try:
        if not context.args:
            await update.message.reply_text("usage: /filled 1,3")
            return
        sid = _get_last_snapshot_id(update.effective_chat.id)
        if not sid:
            await update.message.reply_text("No active snapshot. Run /scan first.")
            return
        args = [
            "snapshot", "filled", sid, context.args[0].strip(),
            "--state-file", STATE_FILE,
            "--manual-log", MANUAL_LOG_FILE,
        ]
        rc, out, err = await asyncio.to_thread(_run_botlogic, args)
        if rc != 0:
            await update.message.reply_text(f"filled failed\n{err or out}")
            return
        snap = _get_snapshot(sid)
        if not snap:
            await update.message.reply_text(f"snapshot missing: {sid}")
            return
        text = _render_snapshot_report(
            sid,
            snap,
            include_statuses={"filled_manual"},
            title="MANUALLY FILLED",
            include_status_on_title=True,
        )
        await _reply_long_html(update, text)
    finally:
        if lock:
            lock.release()


async def cmd_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _deny_if_not_allowed(update):
        return
    lock = await _with_lock(update)
    try:
        if len(context.args) < 2:
            await update.message.reply_text("usage: /skip <idx> <reason>")
            return
        sid = _get_last_snapshot_id(update.effective_chat.id)
        if not sid:
            await update.message.reply_text("No active snapshot. Run /scan first.")
            return
        idx = context.args[0].strip()
        reason = " ".join(context.args[1:]).strip()
        args = [
            "snapshot", "skip", sid, idx, reason,
            "--state-file", STATE_FILE,
            "--manual-log", MANUAL_LOG_FILE,
        ]
        rc, out, err = await asyncio.to_thread(_run_botlogic, args)
        if rc != 0:
            await update.message.reply_text(f"skip failed\n{err or out}")
            return
        await update.message.reply_text(f"snapshot_id: {sid}\nskipped idx={idx} reason={reason}")
    finally:
        if lock:
            lock.release()


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _deny_if_not_allowed(update):
        return
    lock = await _with_lock(update)
    try:
        sid = _get_last_snapshot_id(update.effective_chat.id)
        if not sid:
            await update.message.reply_text("No active snapshot. Run /scan first.")
            return
        snap = _get_snapshot(sid)
        if not snap:
            await update.message.reply_text(f"snapshot not found: {sid}")
            return
        counts: Dict[str, int] = {}
        for c in snap.get("candidates", []):
            if not isinstance(c, dict):
                continue
            st = str(c.get("status") or "unknown")
            counts[st] = counts.get(st, 0) + 1
        parts = [f"{k}={v}" for k, v in sorted(counts.items())]
        await update.message.reply_text(f"snapshot_id: {sid}\n" + " ".join(parts))
    finally:
        if lock:
            lock.release()


def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("show", cmd_show))
    app.add_handler(CommandHandler("select", cmd_select))
    app.add_handler(CommandHandler("revalidate", cmd_revalidate))
    app.add_handler(CommandHandler("approve", cmd_approve))
    app.add_handler(CommandHandler("filled", cmd_filled))
    app.add_handler(CommandHandler("skip", cmd_skip))
    app.add_handler(CommandHandler("status", cmd_status))
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
