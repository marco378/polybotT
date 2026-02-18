#!/usr/bin/env python3
"""
deapthbot.py — Polymarket read-only CLI scanner + VWAP orderbook depth sizing + our own locg checks

Enhanced with:
- Hard A-cap (max VWAP for A leg, e.g. ~0.77)
- Tiered cost bands (under 0.90 / 0.80 / 0.70 / 0.60)
- Signal-driven selection with top2_sum as signal, not binary gate
"""

import argparse
import asyncio
import json
import sys
import time
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import aiohttp

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"

# ---------------------------
# Tier Configuration
# ---------------------------

class CostTier(Enum):
    """Cost tiers for combined YES positions"""
    TIER_90 = 0.90
    TIER_80 = 0.80
    TIER_70 = 0.70
    TIER_60 = 0.60
    
    @classmethod
    def get_tier(cls, cost: float) -> Optional['CostTier']:
        """Get the tier for a given total cost"""
        if cost < cls.TIER_60.value:
            return cls.TIER_60
        elif cost < cls.TIER_70.value:
            return cls.TIER_70
        elif cost < cls.TIER_80.value:
            return cls.TIER_80
        elif cost < cls.TIER_90.value:
            return cls.TIER_90
        return None

# ---------------------------
# Configuration Dataclass
# ---------------------------

@dataclass
class ScanConfig:
    """Configuration for scanning and filtering"""
    # Core filters
    min_top2_signal: float = 0.52  # Realistic lower bound
    max_top2_signal: float = 0.99  # Upper bound before tier filtering
    price_buffer: float = 0.03  # Soft check: price shouldn't be much worse than probability
    min_leg_yes: float = 0.00
    min_candidates: int = 2
    min_event_vol: float = 0
    min_event_total_vol: float = 0
    max_event_total_vol: Optional[float] = None
    max_a_dominance: float = 0.77  # A leg can't be > 77% (prevents illiquid high-prob legs)
    max_third_candidate: float = 0.15  # Third candidate absolute cap (tail dispersion check)
    resolve_within_days: Optional[float] = None  # Only include markets resolving within N days

    # Blacklists
    exclude_tag_ids: List[str] = None
    exclude_keywords: List[str] = None
    
    # VWAP constraints
    a_cap: Optional[float] = 0.77  # Hard cap for A leg VWAP
    b_max_vwap: Optional[float] = None  # Optional cap for B leg
    min_b_fill: float = 100.0  # Minimum shares for B leg
    
    # Tier preferences
    target_tiers: List[CostTier] = None  # Which tiers to include
    
    # Orderbook params
    b_target_shares: float = 1000.0
    max_levels: int = 10
    
    def __post_init__(self):
        if self.target_tiers is None:
            self.target_tiers = [CostTier.TIER_60, CostTier.TIER_70, CostTier.TIER_80, CostTier.TIER_90]
        if self.exclude_tag_ids is None:
            self.exclude_tag_ids = []
        if self.exclude_keywords is None:
            self.exclude_keywords = []

# ---------------------------
# Helpers
# ---------------------------

def _safe_json_loads(x: Any, default: Any):
    if x is None:
        return default
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return default
        try:
            return json.loads(s)
        except Exception:
            return default
    return default

def _to_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def _to_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default

def _is_open_market(m: Dict[str, Any]) -> bool:
    return bool(m.get("active", False))

def _outcomes(m: Dict[str, Any]) -> List[str]:
    raw = m.get("outcomes", [])
    arr = _safe_json_loads(raw, [])
    if isinstance(arr, list) and all(isinstance(x, str) for x in arr):
        return arr
    return []

def _outcome_prices(m: Dict[str, Any]) -> List[float]:
    raw = m.get("outcomePrices", [])
    arr = _safe_json_loads(raw, [])
    out: List[float] = []
    if isinstance(arr, list):
        for x in arr:
            v = _to_float(x)
            if v is not None:
                out.append(v)
    return out

def _is_yes_no_market(m: Dict[str, Any]) -> bool:
    outs = [o.strip().lower() for o in _outcomes(m)]
    prices = _outcome_prices(m)
    return len(prices) == 2 and len(outs) == 2 and ("yes" in outs) and ("no" in outs)

def _yes_price(m: Dict[str, Any]) -> Optional[float]:
    outs = _outcomes(m)
    prices = _outcome_prices(m)
    if len(outs) != 2 or len(prices) != 2:
        return None
    outs_l = [o.strip().lower() for o in outs]
    try:
        i_yes = outs_l.index("yes")
        return prices[i_yes]
    except Exception:
        return prices[0]

def _candidate_name(m: Dict[str, Any]) -> str:
    q = m.get("question")
    if isinstance(q, str) and q.strip():
        return q.strip()
    t = m.get("title")
    if isinstance(t, str) and t.strip():
        return t.strip()
    return "Unknown candidate"

def _event_title(ev: Dict[str, Any]) -> str:
    t = ev.get("title")
    if isinstance(t, str) and t.strip():
        return t.strip()
    return "Unknown event"

def _event_tag_ids(ev: Dict[str, Any]) -> List[str]:
    raw = ev.get("tags") or ev.get("tagIds") or ev.get("tag_ids")
    tags = _safe_json_loads(raw, [])
    out: List[str] = []
    if isinstance(tags, list):
        for t in tags:
            if isinstance(t, dict):
                tid = t.get("id")
            else:
                tid = t
            if tid is None:
                continue
            out.append(str(tid))
    return out

def _event_is_blacklisted(ev: Dict[str, Any], config: ScanConfig) -> bool:
    # Tag blacklist
    if config.exclude_tag_ids:
        ev_tags = set(_event_tag_ids(ev))
        if ev_tags.intersection(config.exclude_tag_ids):
            return True
    # Keyword blacklist (title only)
    if config.exclude_keywords:
        title = _event_title(ev).lower()
        for kw in config.exclude_keywords:
            if kw and kw.lower() in title:
                return True
    return False

def _market_id(m: Dict[str, Any]) -> str:
    return str(m.get("id") or m.get("conditionId") or "unknown")

def _condition_id(m: Dict[str, Any]) -> str:
    return str(m.get("conditionId") or "unknown")

def _market_volume_24h(m: Dict[str, Any]) -> float:
    v = _to_float(m.get("volume24hr"), 0.0)
    return float(v or 0.0)

def _market_volume_total(m: Dict[str, Any]) -> float:
    for k in ("volume", "volumeAll", "volumeAllTime", "totalVolume", "totalVolumeUSD", "volumeUSD"):
        v = _to_float(m.get(k), None)
        if v is not None:
            return float(v)
    return 0.0

def _end_date_key(m: Dict[str, Any]) -> str:
    q = m.get("question", "")
    if isinstance(q, str):
        match = re.search(r'by\s+(\w+\s+\d{1,2}(?:,?\s+\d{4})?)', q, re.IGNORECASE)
        if match:
            return match.group(1)

    for k in ("endDate", "endDateIso", "end_date", "closeTime", "closedTime", "expirationTime"):
        v = m.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    for k in ("endDateTs", "closeTimeTs", "expirationTimeTs"):
        v = m.get(k)
        if isinstance(v, (int, float)):
            return str(int(v))
    return ""

def _parse_end_date_to_ts(end_key: str) -> Optional[float]:
    if not end_key:
        return None
    s = end_key.strip()
    if not s:
        return None

    # Epoch seconds/milliseconds
    if re.fullmatch(r"\d{9,13}", s):
        try:
            v = int(s)
            if len(s) > 10:
                return float(v) / 1000.0
            return float(v)
        except Exception:
            return None

    # ISO-like
    iso = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        pass

    # Common date formats (assume UTC midnight)
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            continue

    return None

# ---------------------------
# Gamma fetch
# ---------------------------

async def fetch_events(
    session: aiohttp.ClientSession,
    limit_events: int,
    tag_id: Optional[int],
    timeout_s: float,
    offset: int = 0,
    end_date_min: Optional[str] = None,
    end_date_max: Optional[str] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "active": "true",
        "closed": "false",
        "limit": limit_events,
        "orderBy": "volume24hr",
        "orderDirection": "desc",
        "offset": offset,
    }
    if tag_id is not None:
        params["tag_id"] = tag_id
    if end_date_min:
        params["end_date_min"] = end_date_min
    if end_date_max:
        params["end_date_max"] = end_date_max

    url = f"{GAMMA_URL}/events"
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    async with session.get(url, params=params, timeout=timeout) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"Gamma /events HTTP {resp.status}: {text[:200]}")
        return await resp.json()

async def fetch_market_by_gamma_id(session: aiohttp.ClientSession, gamma_market_id: str, timeout_s: float) -> Dict[str, Any]:
    url = f"{GAMMA_URL}/markets/{gamma_market_id}"
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    async with session.get(url, timeout=timeout) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"Gamma /markets/{gamma_market_id} HTTP {resp.status}: {text[:200]}")
        return await resp.json()

def _extract_yes_token_id_from_market(m: Dict[str, Any]) -> Optional[str]:
    outs = _outcomes(m)
    if not outs:
        return None

    raw = m.get("clobTokenIds") or m.get("clobTokenIDs") or m.get("clob_token_ids")
    token_ids = _safe_json_loads(raw, [])
    if not isinstance(token_ids, list) or len(token_ids) != len(outs):
        return None

    outs_l = [o.strip().lower() for o in outs]
    try:
        i_yes = outs_l.index("yes")
        tok = token_ids[i_yes]
        if tok is None:
            return None
        return str(tok)
    except Exception:
        return str(token_ids[0]) if token_ids else None

# ---------------------------
# CLOB orderbook fetch + VWAP depth sizing
# ---------------------------

async def fetch_orderbook(session: aiohttp.ClientSession, token_id: str, timeout_s: float) -> Dict[str, Any]:
    url = f"{CLOB_URL}/book"
    params = {"token_id": token_id}
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    async with session.get(url, params=params, timeout=timeout) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"CLOB /book HTTP {resp.status}: {text[:200]}")
        return await resp.json()

def _levels_from_book_asks(book: Dict[str, Any]) -> List[Tuple[float, float]]:
    """
    book["asks"] is expected to be a list of {"price":"0.059", "size":"370"}.
    Returns list sorted low->high price as (price, size) floats.
    """
    asks = book.get("asks", [])
    out: List[Tuple[float, float]] = []
    if isinstance(asks, list):
        for lvl in asks:
            if not isinstance(lvl, dict):
                continue
            p = _to_float(lvl.get("price"))
            s = _to_float(lvl.get("size"))
            if p is None or s is None:
                continue
            if s <= 0:
                continue
            out.append((p, s))
    out.sort(key=lambda x: x[0])
    return out

def _levels_from_book_bids(book: Dict[str, Any]) -> List[Tuple[float, float]]:
    """
    book["bids"] is expected to be a list of {"price":"0.059", "size":"370"}.
    Returns list sorted high->low price as (price, size) floats.
    """
    bids = book.get("bids", [])
    out: List[Tuple[float, float]] = []
    if isinstance(bids, list):
        for lvl in bids:
            if not isinstance(lvl, dict):
                continue
            p = _to_float(lvl.get("price"))
            s = _to_float(lvl.get("size"))
            if p is None or s is None:
                continue
            if s <= 0:
                continue
            out.append((p, s))
    out.sort(key=lambda x: x[0], reverse=True)
    return out

def _best_bid_ask(book: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    bids = _levels_from_book_bids(book)
    asks = _levels_from_book_asks(book)
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    return best_bid, best_ask

def vwap_fill_to_target(
    asks: List[Tuple[float, float]],
    target_shares: float,
    max_vwap: Optional[float],
    max_levels: int,
) -> Dict[str, Any]:
    """Fill orderbook to target share count, respecting VWAP cap"""
    filled = 0.0
    cost = 0.0
    used = []
    levels = 0

    for price, size in asks:
        if levels >= max_levels:
            return {"filled": filled, "cost": cost, "vwap": (cost/filled if filled else None), "used": used, "reason": "max_levels"}
        if filled >= target_shares - 1e-12:
            break

        take = min(size, target_shares - filled)
        new_cost = cost + take * price
        new_filled = filled + take
        new_vwap = new_cost / new_filled if new_filled > 0 else None

        if max_vwap is not None and new_vwap is not None and new_vwap > max_vwap:
            return {"filled": filled, "cost": cost, "vwap": (cost/filled if filled else None), "used": used, "reason": "vwap_cap"}

        filled = new_filled
        cost = new_cost
        used.append({"price": price, "shares": take})
        levels += 1

    reason = "target_met" if filled >= target_shares - 1e-12 else "insufficient_liq"
    return {"filled": filled, "cost": cost, "vwap": (cost/filled if filled else None), "used": used, "reason": reason}

def budget_fill(
    asks: List[Tuple[float, float]],
    budget: float,
    max_vwap: Optional[float],
    max_levels: int,
) -> Dict[str, Any]:
    """Fill orderbook up to budget limit, respecting VWAP cap"""
    filled = 0.0
    cost = 0.0
    used = []
    levels = 0

    for price, size in asks:
        if levels >= max_levels:
            return {"filled": filled, "cost": cost, "vwap": (cost/filled if filled else None), "used": used, "reason": "max_levels"}
        remaining = budget - cost
        if remaining <= 1e-12:
            break

        affordable = remaining / price
        take = min(size, affordable)
        if take <= 1e-12:
            break

        new_cost = cost + take * price
        new_filled = filled + take
        new_vwap = new_cost / new_filled if new_filled > 0 else None

        if max_vwap is not None and new_vwap is not None and new_vwap > max_vwap:
            return {"filled": filled, "cost": cost, "vwap": (cost/filled if filled else None), "used": used, "reason": "vwap_cap"}

        filled = new_filled
        cost = new_cost
        used.append({"price": price, "shares": take})
        levels += 1

    reason = "budget_used" if cost >= budget - 1e-8 else "insufficient_liq"
    return {"filled": filled, "cost": cost, "vwap": (cost/filled if filled else None), "used": used, "reason": reason}

def _worse_liq(fill1: Dict[str, Any], fill2: Dict[str, Any]) -> int:
    """
    Return 1 if fill1 is worse liquidity, 2 if fill2 is worse.
    Worse = smaller filled shares under the same constraints.
    Tie-break: higher vwap, then reason priority.
    """
    f1 = float(fill1.get("filled") or 0.0)
    f2 = float(fill2.get("filled") or 0.0)
    if f1 < f2 - 1e-9:
        return 1
    if f2 < f1 - 1e-9:
        return 2

    v1 = fill1.get("vwap")
    v2 = fill2.get("vwap")
    v1 = float(v1) if v1 is not None else 1e9
    v2 = float(v2) if v2 is not None else 1e9
    if v1 > v2 + 1e-12:
        return 1
    if v2 > v1 + 1e-12:
        return 2

    # Reason priority: cap/max_levels/insufficient are "worse" than target_met
    pr = {"vwap_cap": 3, "max_levels": 2, "insufficient_liq": 2, "target_met": 0}
    r1 = pr.get(str(fill1.get("reason")), 1)
    r2 = pr.get(str(fill2.get("reason")), 1)
    return 1 if r1 > r2 else 2

# ---------------------------
# Cost Calculation
# ---------------------------

def _calculate_effective_cost(hit: Dict[str, Any]) -> float:
    """Calculate total cost for sorting/filtering"""
    if "total_cost" in hit:
        return hit["total_cost"]
    # Fallback to top2_sum if depth data not available
    return hit["top2_sum"]

# ---------------------------
# Persistent State (JSON)
# ---------------------------

def _now_ts() -> float:
    return time.time()

def _load_state(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                data.setdefault("seen_pairs", {})
                data.setdefault("traded_pairs", {})
                data.setdefault("positions", {})
                data.setdefault("snapshots", {})
                return data
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return {"seen_pairs": {}, "traded_pairs": {}, "positions": {}, "snapshots": {}}

def _save_state(path: str, state: Dict[str, Any]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    try:
        import os
        os.replace(tmp, path)
    except Exception:
        # Best-effort fallback
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)

def _prune_state(state: Dict[str, Any], ttl_sec: float, now: float) -> None:
    if ttl_sec <= 0:
        return
    for key in ("seen_pairs", "traded_pairs"):
        bucket = state.get(key, {})
        if not isinstance(bucket, dict):
            state[key] = {}
            continue
        drop = []
        for pid, ts in bucket.items():
            try:
                if now - float(ts) > ttl_sec:
                    drop.append(pid)
            except Exception:
                drop.append(pid)
        for pid in drop:
            bucket.pop(pid, None)

def _pair_id_from_hit(hit: Dict[str, Any]) -> str:
    event_id = str(hit.get("event_id") or "unknown_event")
    end_date = str(hit.get("end_date") or "unknown_end")
    c1 = str(hit.get("leg1_condition_id") or "c1")
    c2 = str(hit.get("leg2_condition_id") or "c2")
    pair = "|".join(sorted([c1, c2]))
    return f"{event_id}:{end_date}:{pair}"

def _seen_recent(state: Dict[str, Any], pair_id: str, ttl_sec: float, now: float) -> bool:
    if ttl_sec <= 0:
        return False
    ts = state.get("seen_pairs", {}).get(pair_id)
    if ts is None:
        return False
    try:
        return now - float(ts) <= ttl_sec
    except Exception:
        return False

def _mark_seen(state: Dict[str, Any], pair_id: str, now: float) -> None:
    state.setdefault("seen_pairs", {})[pair_id] = now

def _mark_traded(state: Dict[str, Any], pair_id: str, now: float) -> None:
    state.setdefault("traded_pairs", {})[pair_id] = now

def _snapshot_bucket(state: Dict[str, Any]) -> Dict[str, Any]:
    snaps = state.get("snapshots")
    if not isinstance(snaps, dict):
        snaps = {}
        state["snapshots"] = snaps
    return snaps

def _snapshot_terminal_statuses() -> set:
    return {"filled_manual", "skipped_manual", "expired", "invalidated"}

def _snapshot_ttl_sec(hours: float) -> float:
    return max(60.0, float(hours) * 3600.0)

def _snapshot_id(now: float) -> str:
    return f"snap_{int(now)}_{int((now - int(now)) * 1000):03d}"

def _snapshot_config_dict(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "min_top2_signal": float(args.min_top2_signal),
        "max_top2_signal": float(args.max_top2_signal),
        "min_leg_yes": float(args.min_leg),
        "min_candidates": int(args.min_candidates),
        "min_event_vol": float(args.min_event_vol),
        "min_event_total_vol": float(args.min_event_total_vol),
        "max_event_total_vol": float(args.max_event_total_vol) if args.max_event_total_vol is not None else None,
        "a_cap": float(args.a_cap) if args.a_cap is not None else None,
        "b_max_vwap": float(args.b_max_vwap) if args.b_max_vwap is not None else None,
        "min_b_fill": float(args.min_b_fill),
        "tiers": str(args.tiers),
        "b_target_shares": float(args.b_target),
        "max_levels": int(args.max_levels),
        "exclude_tag_ids": parse_csv_list(args.exclude_tags),
        "exclude_keywords": parse_csv_list(args.exclude_keywords),
        "resolve_within_days": float(args.resolve_within_days) if args.resolve_within_days is not None else None,
    }

def _scan_config_from_snapshot_cfg(cfg: Dict[str, Any]) -> ScanConfig:
    return ScanConfig(
        min_top2_signal=float(cfg.get("min_top2_signal", 0.52)),
        max_top2_signal=float(cfg.get("max_top2_signal", 0.99)),
        min_leg_yes=float(cfg.get("min_leg_yes", 0.0)),
        min_candidates=int(cfg.get("min_candidates", 2)),
        min_event_vol=float(cfg.get("min_event_vol", 0.0)),
        min_event_total_vol=float(cfg.get("min_event_total_vol", 0.0)),
        max_event_total_vol=cfg.get("max_event_total_vol"),
        a_cap=cfg.get("a_cap"),
        b_max_vwap=cfg.get("b_max_vwap"),
        min_b_fill=float(cfg.get("min_b_fill", 100.0)),
        target_tiers=parse_tier_arg(str(cfg.get("tiers", "60,70,80,90"))),
        b_target_shares=float(cfg.get("b_target_shares", 1000.0)),
        max_levels=int(cfg.get("max_levels", 10)),
        exclude_tag_ids=list(cfg.get("exclude_tag_ids", [])),
        exclude_keywords=list(cfg.get("exclude_keywords", [])),
        resolve_within_days=cfg.get("resolve_within_days"),
    )

def _parse_index_csv(idxs: str) -> List[int]:
    out: List[int] = []
    for raw in str(idxs).split(","):
        s = raw.strip()
        if not s:
            continue
        try:
            v = int(s)
            if v > 0:
                out.append(v)
        except Exception:
            continue
    return sorted(set(out))

def _candidate_from_hit(hit: Dict[str, Any], idx: int, now: float) -> Dict[str, Any]:
    return {
        "idx": idx,
        "pair_id": _pair_id_from_hit(hit),
        "status": "candidate",
        "event_id": hit.get("event_id"),
        "event_title": hit.get("event_title"),
        "end_date": hit.get("end_date"),
        "leg1_gamma_market_id": hit.get("leg1_gamma_market_id"),
        "leg2_gamma_market_id": hit.get("leg2_gamma_market_id"),
        "leg1_condition_id": hit.get("leg1_condition_id"),
        "leg2_condition_id": hit.get("leg2_condition_id"),
        "leg1_name": hit.get("leg1_name"),
        "leg2_name": hit.get("leg2_name"),
        "leg1_yes": hit.get("leg1_yes"),
        "leg2_yes": hit.get("leg2_yes"),
        "group_key": hit.get("group_key"),
        "pair_rank": hit.get("pair_rank"),
        "event_market_ids": list(hit.get("event_market_ids") or []),
        "base_hit": {
            "event_id": hit.get("event_id"),
            "event_title": hit.get("event_title"),
            "end_date": hit.get("end_date"),
            "group_key": hit.get("group_key"),
            "pair_rank": hit.get("pair_rank"),
            "leg1_gamma_market_id": hit.get("leg1_gamma_market_id"),
            "leg1_condition_id": hit.get("leg1_condition_id"),
            "leg1_name": hit.get("leg1_name"),
            "leg1_yes": hit.get("leg1_yes"),
            "leg2_gamma_market_id": hit.get("leg2_gamma_market_id"),
            "leg2_condition_id": hit.get("leg2_condition_id"),
            "leg2_name": hit.get("leg2_name"),
            "leg2_yes": hit.get("leg2_yes"),
            "top2_sum": hit.get("top2_sum"),
            "top2_signal_score": hit.get("top2_signal_score"),
            "event_market_ids": list(hit.get("event_market_ids") or []),
        },
        "snapshot_metrics": {
            "depth_a_is": str(hit.get("depth_a_is") or "leg1"),
            "depth_b_is": str(hit.get("depth_b_is") or "leg2"),
            "a_size": float(hit.get("a_filled") or 0.0),
            "b_size": float(hit.get("b_filled") or 0.0),
            "a_vwap": float(hit.get("a_vwap") or 0.0),
            "b_vwap": float(hit.get("b_vwap") or 0.0),
            "a_cost": float(hit.get("a_cost") or 0.0),
            "b_cost": float(hit.get("b_cost") or 0.0),
            "total_cost": float(hit.get("total_cost") or 0.0),
            "sum_vwap": float(hit.get("sum_vwap") or 0.0),
            "a_win_roi": float(hit.get("a_win_roi") or 0.0),
            "pnl_if_a_wins": float(hit.get("pnl_if_a_wins") or 0.0),
            "pnl_if_b_wins": float(hit.get("pnl_if_b_wins") or 0.0),
        },
        "latest_metrics": None,
        "selected_ts": None,
        "revalidated_ts": None,
        "approved_ts": None,
        "revalidate_due_ts": None,
        "expires_ts": None,
        "updated_ts": now,
    }

def _candidate_revalidation_metrics(candidate: Dict[str, Any], hit: Dict[str, Any]) -> Dict[str, Any]:
    snap = candidate.get("snapshot_metrics") or {}
    old_sum = float(snap.get("sum_vwap") or 0.0)
    old_a_vwap = float(snap.get("a_vwap") or 0.0)
    old_b_vwap = float(snap.get("b_vwap") or 0.0)
    old_a_size = float(snap.get("a_size") or 0.0)
    old_b_size = float(snap.get("b_size") or 0.0)
    new_a_size = float(hit.get("a_filled") or 0.0)
    new_b_size = float(hit.get("b_filled") or 0.0)
    a_fill_ratio = (new_a_size / old_a_size) if old_a_size > 0 else 0.0
    b_fill_ratio = (new_b_size / old_b_size) if old_b_size > 0 else 0.0
    return {
        "depth_a_is": str(hit.get("depth_a_is") or "leg1"),
        "depth_b_is": str(hit.get("depth_b_is") or "leg2"),
        "a_size": new_a_size,
        "b_size": new_b_size,
        "a_vwap": float(hit.get("a_vwap") or 0.0),
        "b_vwap": float(hit.get("b_vwap") or 0.0),
        "a_cost": float(hit.get("a_cost") or 0.0),
        "b_cost": float(hit.get("b_cost") or 0.0),
        "total_cost": float(hit.get("total_cost") or 0.0),
        "sum_vwap": float(hit.get("sum_vwap") or 0.0),
        "a_win_roi": float(hit.get("a_win_roi") or 0.0),
        "pnl_if_a_wins": float(hit.get("pnl_if_a_wins") or 0.0),
        "pnl_if_b_wins": float(hit.get("pnl_if_b_wins") or 0.0),
        "slippage_delta_sum_vwap": float(hit.get("sum_vwap") or 0.0) - old_sum,
        "price_drift_a_vwap": float(hit.get("a_vwap") or 0.0) - old_a_vwap,
        "price_drift_b_vwap": float(hit.get("b_vwap") or 0.0) - old_b_vwap,
        "fill_ratio_a": a_fill_ratio,
        "fill_ratio_b": b_fill_ratio,
        "fill_ratio_min": min(a_fill_ratio, b_fill_ratio),
        "edge_after_refresh": float(hit.get("a_win_roi") or 0.0),
    }

def _truncate_name(s: Any, max_len: int = 30) -> str:
    txt = str(s or "").strip()
    if not txt:
        return "unknown"
    if len(txt) <= max_len:
        return txt
    return txt[: max_len - 3].rstrip() + "..."

def _compact_leg_name(s: Any, max_words: int = 2, max_len: int = 30) -> str:
    txt = str(s or "").strip()
    if not txt:
        return "unknown"
    txt = re.sub(r"^\s*Will\s+", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\?\s*$", "", txt).strip()
    # For range/bin outcomes, keep the condition phrase so legs stay distinct.
    m_cond = re.search(
        r"\bbe\s+(less than|greater than|more than|under|over|between|at least|at most|exactly)\s+(.+)$",
        txt,
        flags=re.IGNORECASE,
    )
    if m_cond:
        phrase = f"{m_cond.group(1).lower()} {m_cond.group(2).strip()}"
        return _truncate_name(phrase, max_len=max_len)
    # Special-case "meet next" markets so both legs don't collapse to "Trump and".
    if re.search(r"\bnot meet\b", txt, flags=re.IGNORECASE):
        return _truncate_name("Not meet", max_len=max_len)
    m_meet = re.search(r"\bmeet next in\s+(.+)$", txt, flags=re.IGNORECASE)
    if m_meet:
        tail = m_meet.group(1).strip()
        return _truncate_name(tail, max_len=max_len)
    cut = re.split(r"\s+(win|be|have|get|become|make|take)\b", txt, maxsplit=1, flags=re.IGNORECASE)
    if cut and cut[0].strip():
        txt = cut[0].strip()
    words = txt.split()
    if max_words > 0 and len(words) > max_words:
        txt = " ".join(words[:max_words])
    return _truncate_name(txt, max_len=max_len)

def _leg_display_names(candidate: Dict[str, Any], metrics: Dict[str, Any]) -> Tuple[str, str]:
    a_side = str(metrics.get("depth_a_is") or "leg1")
    b_side = str(metrics.get("depth_b_is") or "leg2")
    leg1_name = str(candidate.get("leg1_name") or "")
    leg2_name = str(candidate.get("leg2_name") or "")
    a_name = leg1_name if a_side == "leg1" else leg2_name
    b_name = leg2_name if b_side == "leg2" else leg1_name
    return _compact_leg_name(a_name), _compact_leg_name(b_name)

def _expire_old_snapshot_candidates(state: Dict[str, Any], now: float) -> int:
    # Time-based status transitions are disabled for now.
    return 0

def _invalidate_conflicting_candidates(
    state: Dict[str, Any],
    new_snapshot_id: str,
    new_pair_ids: List[str],
    now: float,
) -> int:
    changed = 0
    pair_set = set(new_pair_ids)
    terminals = _snapshot_terminal_statuses()
    for sid, snap in _snapshot_bucket(state).items():
        if sid == new_snapshot_id:
            continue
        cands = snap.get("candidates", [])
        if not isinstance(cands, list):
            continue
        for c in cands:
            if not isinstance(c, dict):
                continue
            status = str(c.get("status") or "")
            if status in terminals:
                continue
            if str(c.get("pair_id") or "") not in pair_set:
                continue
            c["status"] = "invalidated"
            c["invalidated_reason"] = f"superseded_by:{new_snapshot_id}"
            c["updated_ts"] = now
            changed += 1
    return changed

# ---------------------------
# Enhanced Scan Logic
# ---------------------------

def scan_top2_per_event_signal(
    events: List[Dict[str, Any]],
    config: ScanConfig,
    now_ts: float,
) -> List[Dict[str, Any]]:
    """
    Scan for top2 pairs, using top2_sum as signal rather than binary filter
    """
    hits: List[Dict[str, Any]] = []

    for ev in events:
        if _event_is_blacklisted(ev, config):
            continue
        markets = ev.get("markets", []) or []
        if not isinstance(markets, list):
            continue

        approx_event_vol = 0.0
        approx_event_total_vol = 0.0
        groups: Dict[str, List[Tuple[float, Dict[str, Any]]]] = {}

        for m in markets:
            if not isinstance(m, dict):
                continue
            if not _is_open_market(m):
                continue
            if not _is_yes_no_market(m):
                continue

            yp = _yes_price(m)
            if yp is None or yp <= 0.0 or yp >= 1.0:
                continue

            approx_event_vol += _market_volume_24h(m)
            approx_event_total_vol += _market_volume_total(m)

            end_key = _end_date_key(m)
            if not end_key:
                continue
            if config.resolve_within_days is not None:
                end_ts = _parse_end_date_to_ts(end_key)
                if end_ts is None:
                    continue
                if end_ts < now_ts:
                    continue
                cutoff_ts = now_ts + float(config.resolve_within_days) * 86400.0
                if end_ts > cutoff_ts:
                    continue

            groups.setdefault(end_key, []).append((yp, m))

        if approx_event_vol < config.min_event_vol:
            continue
        if approx_event_total_vol < config.min_event_total_vol:
            continue
        if config.max_event_total_vol is not None and approx_event_total_vol > config.max_event_total_vol:
            continue

        for end_key, candidates in groups.items():
            if len(candidates) < max(2, config.min_candidates):
                continue

            event_market_ids = [_market_id(m) for _, m in candidates]

            # Sort by YES price descending
            candidates.sort(key=lambda x: x[0], reverse=True)
            
            # Tail dispersion check: absolute cap on third candidate
            if len(candidates) >= 3:
                p3 = candidates[2][0]
                if p3 > config.max_third_candidate:
                    continue  # Hidden third favorite
            
            # Consider top 3 for more flexibility
            top_n = min(3, len(candidates))
            
            for i in range(top_n - 1):
                for j in range(i + 1, top_n):
                    p1, m1 = candidates[i]
                    p2, m2 = candidates[j]
                    
                    # Apply leg filters
                    if p1 < config.min_leg_yes or p2 < config.min_leg_yes:
                        continue
                    
                    # FIX #1: Enforce A-leg cap (prevent 90% + 5% scenarios)
                    if max(p1, p2) > config.max_a_dominance:
                        continue
                    
                    top2_sum = p1 + p2
                    
                    # Core opportunity flag: top2_sum in realistic band
                    if not (config.min_top2_signal <= top2_sum <= config.max_top2_signal):
                        continue
                    
                    hits.append(
                        {
                            "event_id": ev.get("id"),
                            "event_title": _event_title(ev),
                            "event_volume_24h_approx": approx_event_vol,
                            "event_volume_total_approx": approx_event_total_vol,
                            "end_date": end_key,
                            "group_key": f"{ev.get('id')}::{end_key}",
                            "pair_rank": "top2" if (i == 0 and j == 1) else "other",
                            "leg1_gamma_market_id": _market_id(m1),
                            "leg1_condition_id": _condition_id(m1),
                            "leg1_name": _candidate_name(m1),
                            "leg1_yes": p1,
                            "leg2_gamma_market_id": _market_id(m2),
                            "leg2_condition_id": _condition_id(m2),
                            "leg2_name": _candidate_name(m2),
                            "leg2_yes": p2,
                            "top2_sum": top2_sum,
                            "top2_signal_score": top2_sum,  # For sorting by signal strength
                            "event_market_ids": event_market_ids,
                        }
                    )

    return hits

async def enrich_hits_with_depth_and_filter(
    session: aiohttp.ClientSession,
    hits: List[Dict[str, Any]],
    timeout_s: float,
    config: ScanConfig,
    reject_counts: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """
    Enrich hits with depth data and apply filtering logic
    Returns filtered and enriched hits
    """
    enriched_hits: List[Dict[str, Any]] = []
    
    async def enrich_pair(hit: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        def _reject(reason: str) -> None:
            if reject_counts is not None:
                reject_counts[reason] = reject_counts.get(reason, 0) + 1
        # Fetch market details and orderbooks for both legs
        try:
            m1_full = await fetch_market_by_gamma_id(session, hit["leg1_gamma_market_id"], timeout_s)
            m2_full = await fetch_market_by_gamma_id(session, hit["leg2_gamma_market_id"], timeout_s)
        except Exception as e:
            hit["depth_error"] = f"market_fetch_failed: {e}"
            _reject("market_fetch_failed")
            return None

        token1 = _extract_yes_token_id_from_market(m1_full)
        token2 = _extract_yes_token_id_from_market(m2_full)
        
        if not token1 or not token2:
            _reject("missing_token_id")
            return None

        try:
            book1 = await fetch_orderbook(session, token1, timeout_s)
            book2 = await fetch_orderbook(session, token2, timeout_s)
        except Exception as e:
            _reject("orderbook_fetch_failed")
            return None

        asks1 = _levels_from_book_asks(book1)
        asks2 = _levels_from_book_asks(book2)

        if not asks1 or not asks2:
            _reject("empty_orderbook")
            return None

        # ----------------------------
        # Phase 1: Define B as UNDERDOG (lower prob) AND THINNER (execution bottleneck)
        # ----------------------------

        # Step 1: pick B as lower-probability (lower YES price), using latest market data.
        leg1_yes_live = _to_float(_yes_price(m1_full), _to_float(hit.get("leg1_yes"), None))
        leg2_yes_live = _to_float(_yes_price(m2_full), _to_float(hit.get("leg2_yes"), None))
        if leg1_yes_live is None or leg2_yes_live is None:
            _reject("live_yes_price_missing")
            return None

        if leg1_yes_live <= leg2_yes_live:
            B_asks, B_token = asks1, token1
            A_asks, A_token = asks2, token2
            B_label, A_label = "leg1", "leg2"
            B_yes_price, A_yes_price = leg1_yes_live, leg2_yes_live
        else:
            B_asks, B_token = asks2, token2
            A_asks, A_token = asks1, token1
            B_label, A_label = "leg2", "leg1"
            B_yes_price, A_yes_price = leg2_yes_live, leg1_yes_live

        # Step 2: ensure B is actually thinner (fills fewer shares in a probe)
        probeB = vwap_fill_to_target(B_asks, config.b_target_shares, None, config.max_levels)
        probeA = vwap_fill_to_target(A_asks, config.b_target_shares, None, config.max_levels)

        b_probe_fill = float(probeB.get("filled") or 0.0)
        a_probe_fill = float(probeA.get("filled") or 0.0)

        # If underdog is not thinner, reject (matches your definition)
        if b_probe_fill >= a_probe_fill - 1e-9:
            _reject("b_not_thinner_than_a")
            return None

        # Phase 2/3: depth-aware sizing with adaptive downsize retries on live data.
        # If breakeven feasibility fails at full size, reduce B target and retry.
        base_target = max(float(config.min_b_fill), float(config.b_target_shares))
        retry_multipliers = [1.0, 0.8, 0.6, 0.45, 0.3, 0.2]
        chosen: Optional[Dict[str, Any]] = None

        for mult in retry_multipliers:
            target = max(float(config.min_b_fill), base_target * float(mult))
            B_fill = vwap_fill_to_target(B_asks, target, config.b_max_vwap, config.max_levels)
            b_filled_val = float(B_fill.get("filled") or 0.0)
            if b_filled_val < float(config.min_b_fill):
                continue

            b_vwap = B_fill.get("vwap")
            if b_vwap is None or float(b_vwap) >= 1.0:
                continue

            b_cost = float(B_fill.get("cost") or 0.0)
            b_shares = float(B_fill.get("filled") or 0.0)

            # Spend B-win profit on A so B-win remains breakeven.
            b_profit_if_wins = b_shares * (1.0 - float(b_vwap))
            a_budget = b_profit_if_wins
            A_fill = budget_fill(A_asks, a_budget, config.a_cap, config.max_levels)

            a_vwap = A_fill.get("vwap")
            if a_vwap is None or float(a_vwap) >= 1.0:
                continue

            a_cost = float(A_fill.get("cost") or 0.0)
            a_shares = float(A_fill.get("filled") or 0.0)
            total_cost = b_cost + a_cost
            pnl_if_b_wins = b_shares - total_cost
            pnl_if_a_wins = a_shares - total_cost
            sum_vwap = float(a_vwap) + float(b_vwap)

            if sum_vwap >= 1.0:
                continue
            if pnl_if_a_wins <= 0.0:
                continue

            tier = CostTier.get_tier(sum_vwap)
            if tier not in config.target_tiers:
                continue

            chosen = {
                "B_fill": B_fill,
                "A_fill": A_fill,
                "b_vwap": float(b_vwap),
                "a_vwap": float(a_vwap),
                "b_cost": b_cost,
                "a_cost": a_cost,
                "b_shares": b_shares,
                "a_shares": a_shares,
                "a_budget": float(a_budget),
                "sum_vwap": float(sum_vwap),
                "total_cost": float(total_cost),
                "pnl_if_b_wins": float(pnl_if_b_wins),
                "pnl_if_a_wins": float(pnl_if_a_wins),
                "tier": tier,
                "target_multiplier": float(mult),
            }
            break

        if chosen is None:
            _reject("breakeven_not_feasible_current_depth")
            return None

        B_fill = chosen["B_fill"]
        A_fill = chosen["A_fill"]
        b_vwap = chosen["b_vwap"]
        a_vwap = chosen["a_vwap"]
        b_cost = chosen["b_cost"]
        a_cost = chosen["a_cost"]
        b_shares = chosen["b_shares"]
        a_shares = chosen["a_shares"]
        a_budget = chosen["a_budget"]
        sum_vwap = chosen["sum_vwap"]
        total_cost = chosen["total_cost"]
        pnl_if_b_wins = chosen["pnl_if_b_wins"]
        pnl_if_a_wins = chosen["pnl_if_a_wins"]
        tier = chosen["tier"]

        # Save enrichment
        hit["depth_b_is"] = B_label
        hit["depth_a_is"] = A_label

        hit["b_token_id"] = B_token
        hit["b_filled"] = float(b_shares)
        hit["b_cost"] = float(b_cost)
        hit["b_vwap"] = float(b_vwap)
        hit["b_reason"] = B_fill["reason"]
        hit["b_levels"] = len(B_fill["used"])
        hit["b_yes_price"] = B_yes_price

        hit["a_token_id"] = A_token
        hit["a_budget"] = float(a_budget)
        hit["a_filled"] = float(a_shares)
        hit["a_cost"] = float(a_cost)
        hit["a_vwap"] = float(a_vwap)
        hit["a_reason"] = A_fill["reason"]
        hit["a_levels"] = len(A_fill["used"])
        hit["a_yes_price"] = A_yes_price

        hit["sum_vwap"] = sum_vwap
        hit["total_cost"] = float(total_cost)
        hit["cost_tier"] = tier.value
        hit["cost_tier_name"] = tier.name

        hit["pnl_if_b_wins"] = float(pnl_if_b_wins)
        hit["pnl_if_a_wins"] = float(pnl_if_a_wins)
        hit["a_win_roi"] = float(pnl_if_a_wins) / float(total_cost) if total_cost > 0 else 0.0
        hit["recalc_target_multiplier"] = float(chosen["target_multiplier"])

        return hit

    # Process hits concurrently
    tasks = [enrich_pair(hit) for hit in hits]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for result in results:
        if isinstance(result, dict):
            enriched_hits.append(result)

    # Require top-2 pair to pass; only show the top-2 pair for each group.
    allowed_groups = {
        h.get("group_key")
        for h in enriched_hits
        if h.get("pair_rank") == "top2"
    }
    if allowed_groups:
        enriched_hits = [
            h
            for h in enriched_hits
            if h.get("group_key") in allowed_groups and h.get("pair_rank") == "top2"
        ]
    else:
        enriched_hits = []

    return enriched_hits

# ---------------------------
# Trading Helpers (Dry-Run)
# ---------------------------

def _append_log(path: str, line: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {line}\n")

def _append_json_human_log(path: str, payload: Dict[str, Any], human_line: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = dict(payload)
    line.setdefault("logged_at", ts)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(line, sort_keys=True) + "\n")
        f.write(f"[{ts}] {human_line}\n")

async def fetch_yes_token_ids_for_markets(
    session: aiohttp.ClientSession,
    market_ids: List[str],
    timeout_s: float,
) -> Dict[str, str]:
    out: Dict[str, str] = {}
    async def _fetch(mid: str) -> None:
        try:
            m = await fetch_market_by_gamma_id(session, mid, timeout_s)
            tok = _extract_yes_token_id_from_market(m)
            if tok:
                out[mid] = tok
        except Exception:
            return
    tasks = [_fetch(mid) for mid in market_ids]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    return out

async def fetch_books_for_tokens(
    session: aiohttp.ClientSession,
    token_ids: List[str],
    timeout_s: float,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    async def _fetch(tok: str) -> None:
        try:
            out[tok] = await fetch_orderbook(session, tok, timeout_s)
        except Exception:
            return
    tasks = [_fetch(tok) for tok in token_ids]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    return out

def _best_price_for_sell(book: Dict[str, Any]) -> Optional[float]:
    best_bid, best_ask = _best_bid_ask(book)
    return best_bid if best_bid is not None else best_ask

def _hit_end_ts(hit: Dict[str, Any]) -> float:
    end_key = str(hit.get("end_date") or "")
    ts = _parse_end_date_to_ts(end_key)
    return float(ts) if ts is not None else 1e18

def _position_current_value(pos: Dict[str, Any], prices: Dict[str, float]) -> Optional[float]:
    a_tok = pos.get("a_token_id")
    b_tok = pos.get("b_token_id")
    a_size = float(pos.get("a_size") or 0.0)
    b_size = float(pos.get("b_size") or 0.0)
    if not a_tok or not b_tok:
        return None
    pa = prices.get(a_tok)
    pb = prices.get(b_tok)
    if pa is None or pb is None:
        return None
    return a_size * pa + b_size * pb

def _position_leg_values(pos: Dict[str, Any], prices: Dict[str, float]) -> Optional[Tuple[float, float, float]]:
    a_tok = pos.get("a_token_id")
    b_tok = pos.get("b_token_id")
    a_size = float(pos.get("a_size") or 0.0)
    b_size = float(pos.get("b_size") or 0.0)
    if not a_tok or not b_tok:
        return None
    pa = prices.get(a_tok)
    pb = prices.get(b_tok)
    if pa is None or pb is None:
        return None
    a_value = a_size * pa
    b_value = b_size * pb
    return a_value, b_value, a_value + b_value

async def _fetch_enriched_hits_for_config(
    session: aiohttp.ClientSession,
    config: ScanConfig,
    timeout_s: float,
    limit_events: int,
    max_pages: int,
    max_events: int,
    tag_id: Optional[int],
    now: float,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    page = 0
    total_cap = int(max_events or 0)
    end_date_min = None
    end_date_max = None
    if config.resolve_within_days is not None:
        now_dt = datetime.now(timezone.utc)
        end_date_min = now_dt.isoformat().replace("+00:00", "Z")
        end_date_max = (now_dt + timedelta(days=float(config.resolve_within_days))).isoformat().replace("+00:00", "Z")

    while True:
        if max_pages and page >= max_pages:
            break
        offset = page * limit_events
        batch = await fetch_events(
            session,
            limit_events,
            tag_id,
            timeout_s,
            offset=offset,
            end_date_min=end_date_min,
            end_date_max=end_date_max,
        )
        if not batch:
            break
        events.extend(batch)
        page += 1
        if total_cap and len(events) >= total_cap:
            events = events[:total_cap]
            break
        if len(batch) < limit_events:
            break

    hits = scan_top2_per_event_signal(events, config, now)
    enriched_hits = await enrich_hits_with_depth_and_filter(
        session=session,
        hits=hits,
        timeout_s=timeout_s,
        config=config,
        reject_counts=None,
    )
    enriched_hits.sort(key=lambda x: (_hit_end_ts(x), x.get("total_cost", 1e9)))
    return enriched_hits

def _print_snapshot(snapshot: Dict[str, Any]) -> None:
    sid = str(snapshot.get("snapshot_id") or "unknown")
    created_ts = snapshot.get("created_ts")
    created = datetime.fromtimestamp(float(created_ts), tz=timezone.utc).isoformat() if created_ts else "unknown"
    print(f"snapshot_id: {sid}")
    print(f"created_utc: {created}")
    print(f"status: {snapshot.get('status', 'open')}")
    cfg = snapshot.get("config") or {}
    print(f"candidate_limit: {snapshot.get('candidate_limit', 10)}")
    print(f"ttl_hours: {snapshot.get('ttl_hours', 2.0)}")
    print(f"tiers: {cfg.get('tiers', '60,70,80,90')}")
    cands = snapshot.get("candidates") or []
    for c in cands:
        idx = c.get("idx")
        title = c.get("event_title")
        status = c.get("status")
        snap = c.get("snapshot_metrics") or {}
        latest = c.get("latest_metrics") or {}
        metrics = latest if latest else snap
        total_cost = latest.get("total_cost", snap.get("total_cost", 0.0))
        roi = latest.get("a_win_roi", snap.get("a_win_roi", 0.0))
        a_vwap = latest.get("a_vwap", snap.get("a_vwap", 0.0))
        b_vwap = latest.get("b_vwap", snap.get("b_vwap", 0.0))
        a_size = latest.get("a_size", snap.get("a_size", 0.0))
        b_size = latest.get("b_size", snap.get("b_size", 0.0))
        a_name, b_name = _leg_display_names(c, metrics)
        print(
            f"  [{idx}] {status} | {title} | total_cost=${float(total_cost):,.2f} "
            f"A={a_name} {float(a_size):.2f}@{float(a_vwap):.4f} "
            f"B={b_name} {float(b_size):.2f}@{float(b_vwap):.4f} "
            f"edge={float(roi):.2%}"
        )

def _print_snapshot_filtered(snapshot: Dict[str, Any], include_statuses: set) -> None:
    sid = str(snapshot.get("snapshot_id") or "unknown")
    created_ts = snapshot.get("created_ts")
    created = datetime.fromtimestamp(float(created_ts), tz=timezone.utc).isoformat() if created_ts else "unknown"
    print(f"snapshot_id: {sid}")
    print(f"created_utc: {created}")
    print(f"status: {snapshot.get('status', 'open')}")
    cands = snapshot.get("candidates") or []
    for c in cands:
        status = str(c.get("status") or "")
        if status not in include_statuses:
            continue
        idx = c.get("idx")
        title = c.get("event_title")
        snap = c.get("snapshot_metrics") or {}
        latest = c.get("latest_metrics") or {}
        metrics = latest if latest else snap
        total_cost = latest.get("total_cost", snap.get("total_cost", 0.0))
        roi = latest.get("a_win_roi", snap.get("a_win_roi", 0.0))
        a_vwap = latest.get("a_vwap", snap.get("a_vwap", 0.0))
        b_vwap = latest.get("b_vwap", snap.get("b_vwap", 0.0))
        a_size = latest.get("a_size", snap.get("a_size", 0.0))
        b_size = latest.get("b_size", snap.get("b_size", 0.0))
        a_name, b_name = _leg_display_names(c, metrics)
        print(
            f"  [{idx}] {status} | {title} | total_cost=${float(total_cost):,.2f} "
            f"A={a_name} {float(a_size):.2f}@{float(a_vwap):.4f} "
            f"B={b_name} {float(b_size):.2f}@{float(b_vwap):.4f} "
            f"edge={float(roi):.2%}"
        )

async def _revalidate_candidate(
    session: aiohttp.ClientSession,
    candidate: Dict[str, Any],
    config: ScanConfig,
    timeout_s: float,
    ttl_sec: float,
    now: float,
) -> Tuple[bool, str]:
    base_hit = candidate.get("base_hit") or {}
    refreshed = await enrich_hits_with_depth_and_filter(
        session=session,
        hits=[dict(base_hit)],
        timeout_s=timeout_s,
        config=config,
        reject_counts=None,
    )
    if not refreshed:
        candidate["status"] = "expired"
        candidate["expiry_reason"] = "revalidate_failed_or_breakeven_broken"
        candidate["updated_ts"] = now
        return False, "revalidate_failed_or_breakeven_broken"

    hit = refreshed[0]
    candidate["latest_metrics"] = _candidate_revalidation_metrics(candidate, hit)
    candidate["status"] = "revalidated"
    candidate["revalidated_ts"] = now
    candidate["revalidate_due_ts"] = None
    candidate["expires_ts"] = None
    candidate["updated_ts"] = now
    return True, "ok"

# ---------------------------
# Enhanced Display Functions
# ---------------------------

def print_hits_by_tier(hits: List[Dict[str, Any]], top_n: int, show_depth: bool) -> None:
    if not hits:
        print("No flagged events found with current thresholds.")
        return

    # Group by tier
    hits_by_tier: Dict[float, List[Dict[str, Any]]] = {}
    for hit in hits:
        tier_val = hit.get("cost_tier", 1.0)
        hits_by_tier.setdefault(tier_val, []).append(hit)
    
    # Sort tiers
    sorted_tiers = sorted(hits_by_tier.keys())
    total_shown = 0
    
    for tier_val in sorted_tiers:
        tier_hits = hits_by_tier[tier_val]
        tier_name = tier_hits[0].get("cost_tier_name", f"TIER_{int(tier_val*100)}")
        full_count = len(tier_hits)

        print(f"\n{'='*70}")
        # Sort within tier by signal strength (top2_sum)
        tier_hits.sort(key=lambda x: x.get("top2_signal_score", 0), reverse=True)

        # Apply per-tier slicing if requested
        shown_hits = tier_hits[:top_n] if top_n > 0 else tier_hits
        shown_count = len(shown_hits)
        total_shown += shown_count

        header_note = f"showing {shown_count} of {full_count}" if top_n > 0 else f"{full_count} events"
        print(f" TIER: {tier_name} (unit price < ${tier_val:.2f}) - {header_note}")
        print(f"{'='*70}")

        for h in shown_hits:
            print(f"\nevent: {h['event_title']}")
            print(f"event_id: {h['event_id']}")
            print(f"approx_event_24h_volume: ${h['event_volume_24h_approx']:,.0f}")
            if "event_volume_total_approx" in h:
                print(f"approx_event_total_volume: ${h['event_volume_total_approx']:,.0f}")
            print(f"end_date: {h['end_date']}")
            
            # Show which leg is A and which is B
            b_label = h.get('depth_b_is', 'leg2')
            a_label = h.get('depth_a_is', 'leg1')
            a_leg_name = _compact_leg_name(h.get(f"{a_label}_name"))
            b_leg_name = _compact_leg_name(h.get(f"{b_label}_name"))
            
            print(f"{a_label.upper()} (A): YES {h[f'{a_label}_yes']:.4f}  {a_leg_name}")
            print(f"{b_label.upper()} (B): YES {h[f'{b_label}_yes']:.4f}  {b_leg_name}")
            
            if show_depth:
                print(f"\n[DEPTH SIZING] Total cost: ${h['total_cost']:,.2f} ({h['cost_tier_name']})")
                print(f"  B (thinner/underdog): {h['b_filled']:,.2f} shares @ ${h['b_vwap']:.4f} avg | cost: ${h['b_cost']:,.2f}")
                print(f"  A (stronger):        {h['a_filled']:,.2f} shares @ ${h['a_vwap']:.4f} avg | cost: ${h['a_cost']:,.2f}")
                total_cost = float(h.get("total_cost") or 0.0)
                if total_cost > 0:
                    a_pct = float(h.get("a_cost") or 0.0) / total_cost
                    b_pct = float(h.get("b_cost") or 0.0) / total_cost
                    print(f"  Capital ratio (A:B): {a_pct:.1%} : {b_pct:.1%}")
                print(f"  sum_vwap(A+B): {h.get('sum_vwap', 0.0):.4f} (must be < 1.0000)")
                print(f"  PnL if B wins (breakeven target): ${h.get('pnl_if_b_wins', 0.0):,.2f}")
                print(f"  PnL if A wins (must be > 0):      ${h.get('pnl_if_a_wins', 0.0):,.2f} | ROI {h.get('a_win_roi', 0.0):.2%}")
            
            print(f"top2_sum signal: {h['top2_sum']:.4f}")
            print("-" * 60)
    
    print(f"\n{'='*70}")
    if top_n > 0:
        print(f" Total candidates across all tiers: {len(hits)} | shown: {total_shown}")
    else:
        print(f" Total events across all tiers: {len(hits)}")
    print(f"{'='*70}\n")

# ---------------------------
# CLI with Enhanced Options
# ---------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="deapthbot.py", description="Polymarket top-2 candidates per event scanner + VWAP orderbook depth sizing.")
    sub = p.add_subparsers(dest="cmd", required=True)

    scan = sub.add_parser("scan", help="Scan and print flagged top-2 candidate events.")

    # Basic scan params
    scan.add_argument("--limit-events", type=int, default=200, help="How many events to fetch from Gamma.")
    scan.add_argument("--max-pages", type=int, default=10,
                     help="Max pages of events to fetch (pagination).")
    scan.add_argument("--max-events", type=int, default=0,
                     help="Optional cap on total events fetched (0 = no cap).")
    scan.add_argument("--tag-id", type=int, default=None, help="Optional Gamma tag_id filter.")
    
    # Signal-based filtering
    scan.add_argument("--min-top2-signal", type=float, default=0.52, 
                     help="Minimum top2_sum to consider (realistic lower bound).")
    scan.add_argument("--max-top2-signal", type=float, default=0.99, 
                     help="Maximum top2_sum to consider (before tier filtering).")
    scan.add_argument("--min-leg", type=float, default=0.00, 
                     help="Require EACH of top1/top2 >= this.")
    scan.add_argument("--min-candidates", type=int, default=2, 
                     help="Require at least this many candidate markets in the event.")
    scan.add_argument("--min-event-vol", type=float, default=0, 
                     help="Approx min 24h volume to consider.")
    scan.add_argument("--min-event-total-vol", type=float, default=0,
                     help="Approx min total volume to consider.")
    scan.add_argument("--max-event-total-vol", type=float, default=None,
                     help="Approx max total volume to consider.")

    # Blacklists
    scan.add_argument("--exclude-tags", type=str, default="1,101411,100240",
                     help="Comma-separated Gamma tag IDs to exclude (default: sports).")
    scan.add_argument("--exclude-keywords", type=str, default="sports,sport,nba,nfl,mlb,nhl,ncaa,ufc,mma,soccer,football,basketball,baseball,hockey,tennis,golf,boxing,cricket,rugby,f1,formula,nascar,motorsport,olympics,world cup,super bowl,world series,stanley cup,finals,playoffs",
                     help="Comma-separated keywords to exclude by event title (case-insensitive).")
    
    # VWAP constraints
    scan.add_argument("--a-cap", type=float, default=0.77,
                     help="Hard cap for A leg VWAP (e.g. 0.96).")
    scan.add_argument("--b-max-vwap", type=float, default=None,
                     help="Optional cap for B leg VWAP.")
    
    # Tier selection
    scan.add_argument("--tiers", type=str, default="60,70,80,90",
                     help="Comma-separated cost tiers to include (60,70,80,90).")
    
    # Orderbook params
    scan.add_argument("--b-target", type=float, default=1000.0,
                     help="Target shares for B leg.")
    scan.add_argument("--max-levels", type=int, default=10,
                     help="Max orderbook levels to consume.")
    scan.add_argument("--min-b-fill", type=float, default=100.0,
                     help="Skip if B fills less than this many shares.")
    scan.add_argument("--resolve-within-days", type=float, default=None,
                     help="Only include markets resolving within N days.")
    
    # Output control
    scan.add_argument("--top", type=int, default=0,
                     help="Print top N events per tier (0 = show all; default: all).")
    scan.add_argument("--timeout", type=float, default=55.0,
                     help="HTTP timeout seconds.")
    scan.add_argument("--watch", action="store_true",
                     help="Loop continuously.")
    scan.add_argument("--interval", type=float, default=3.0,
                     help="Seconds between scans in --watch mode.")
    
    scan.add_argument("--depth", action="store_true",
                     help="Enable orderbook depth sizing (multi-level VWAP).")
    scan.add_argument("--state-file", type=str, default="state.json",
                     help="Path to JSON state file for seen/traded pairs.")
    scan.add_argument("--dedupe-ttl-min", type=float, default=1.0,
                     help="Minutes to suppress already-seen pairs (0 disables).")
    scan.add_argument("--debug-rejects", action="store_true",
                     help="Print counts of depth filter rejection reasons.")

    trade = sub.add_parser("trade", help="Dry-run trading loop with trailing stop and kill-switch.")

    # Basic scan params
    trade.add_argument("--limit-events", type=int, default=200, help="How many events to fetch from Gamma.")
    trade.add_argument("--max-pages", type=int, default=10,
                     help="Max pages of events to fetch (pagination).")
    trade.add_argument("--max-events", type=int, default=0,
                     help="Optional cap on total events fetched (0 = no cap).")
    trade.add_argument("--tag-id", type=int, default=None, help="Optional Gamma tag_id filter.")

    # Signal-based filtering
    trade.add_argument("--min-top2-signal", type=float, default=0.52,
                     help="Minimum top2_sum to consider (realistic lower bound).")
    trade.add_argument("--max-top2-signal", type=float, default=0.99,
                     help="Maximum top2_sum to consider (before tier filtering).")
    trade.add_argument("--min-leg", type=float, default=0.00,
                     help="Require EACH of top1/top2 >= this.")
    trade.add_argument("--min-candidates", type=int, default=2,
                     help="Require at least this many candidate markets in the event.")
    trade.add_argument("--min-event-vol", type=float, default=0,
                     help="Approx min 24h volume to consider.")
    trade.add_argument("--min-event-total-vol", type=float, default=0,
                     help="Approx min total volume to consider.")
    trade.add_argument("--max-event-total-vol", type=float, default=None,
                     help="Approx max total volume to consider.")

    # Blacklists
    trade.add_argument("--exclude-tags", type=str, default="1,101411,100240",
                     help="Comma-separated Gamma tag IDs to exclude (default: sports).")
    trade.add_argument("--exclude-keywords", type=str, default="sports,sport,nba,nfl,mlb,nhl,ncaa,ufc,mma,soccer,football,basketball,baseball,hockey,tennis,golf,boxing,cricket,rugby,f1,formula,nascar,motorsport,olympics,world cup,super bowl,world series,stanley cup,finals,playoffs",
                     help="Comma-separated keywords to exclude by event title (case-insensitive).")

    # VWAP constraints
    trade.add_argument("--a-cap", type=float, default=0.77,
                     help="Hard cap for A leg VWAP (e.g. 0.96).")
    trade.add_argument("--b-max-vwap", type=float, default=None,
                     help="Optional cap for B leg VWAP.")

    # Tier selection
    trade.add_argument("--tiers", type=str, default="60,70,80,90",
                     help="Comma-separated cost tiers to include (60,70,80,90).")

    # Orderbook params
    trade.add_argument("--b-target", type=float, default=1000.0,
                     help="Target shares for B leg.")
    trade.add_argument("--max-levels", type=int, default=10,
                     help="Max orderbook levels to consume.")
    trade.add_argument("--min-b-fill", type=float, default=100.0,
                     help="Skip if B fills less than this many shares.")
    trade.add_argument("--resolve-within-days", type=float, default=None,
                     help="Only include markets resolving within N days.")

    # Runtime
    trade.add_argument("--timeout", type=float, default=55.0,
                     help="HTTP timeout seconds.")
    trade.add_argument("--interval", type=float, default=4.0,
                     help="Seconds between trading loops.")
    trade.add_argument("--once", action="store_true",
                     help="Run a single trading cycle and exit.")
    trade.add_argument("--state-file", type=str, default="state.json",
                     help="Path to JSON state file for positions and history.")
    trade.add_argument("--dedupe-ttl-min", type=float, default=0.0,
                     help="Minutes to suppress already-seen pairs (0 disables).")
    trade.add_argument("--dry-run-log", type=str, default="dryrun.log",
                     help="Path to dry-run log file.")

    snapshot = sub.add_parser("snapshot", help="Manual approval workflow over top candidates.")
    snapshot_sub = snapshot.add_subparsers(dest="snapshot_cmd", required=True)

    snap_create = snapshot_sub.add_parser("create", help="Create a top-N candidate snapshot.")
    snap_create.add_argument("--limit-events", type=int, default=200, help="How many events to fetch from Gamma.")
    snap_create.add_argument("--max-pages", type=int, default=10, help="Max pages of events to fetch.")
    snap_create.add_argument("--max-events", type=int, default=0, help="Optional cap on total events fetched (0 = no cap).")
    snap_create.add_argument("--tag-id", type=int, default=None, help="Optional Gamma tag_id filter.")
    snap_create.add_argument("--top-n", type=int, default=10, help="How many candidates to keep in snapshot.")
    snap_create.add_argument("--snapshot-ttl-hours", type=float, default=2.0, help="TTL hours per selected candidate before refresh required.")
    snap_create.add_argument("--min-top2-signal", type=float, default=0.52)
    snap_create.add_argument("--max-top2-signal", type=float, default=0.99)
    snap_create.add_argument("--min-leg", type=float, default=0.00)
    snap_create.add_argument("--min-candidates", type=int, default=2)
    snap_create.add_argument("--min-event-vol", type=float, default=0)
    snap_create.add_argument("--min-event-total-vol", type=float, default=0)
    snap_create.add_argument("--max-event-total-vol", type=float, default=None)
    snap_create.add_argument("--exclude-tags", type=str, default="1,101411,100240")
    snap_create.add_argument("--exclude-keywords", type=str, default="sports,sport,nba,nfl,mlb,nhl,ncaa,ufc,mma,soccer,football,basketball,baseball,hockey,tennis,golf,boxing,cricket,rugby,f1,formula,nascar,motorsport,olympics,world cup,super bowl,world series,stanley cup,finals,playoffs")
    snap_create.add_argument("--a-cap", type=float, default=0.77)
    snap_create.add_argument("--b-max-vwap", type=float, default=None)
    snap_create.add_argument("--tiers", type=str, default="60,70,80,90")
    snap_create.add_argument("--b-target", type=float, default=1000.0)
    snap_create.add_argument("--max-levels", type=int, default=10)
    snap_create.add_argument("--min-b-fill", type=float, default=100.0)
    snap_create.add_argument("--resolve-within-days", type=float, default=None)
    snap_create.add_argument("--timeout", type=float, default=55.0)
    snap_create.add_argument("--state-file", type=str, default="state.json")
    snap_create.add_argument("--manual-log", type=str, default="manual_execution.log")

    snap_show = snapshot_sub.add_parser("show", help="Show a snapshot.")
    snap_show.add_argument("snapshot_id", type=str)
    snap_show.add_argument("--state-file", type=str, default="state.json")

    snap_select = snapshot_sub.add_parser("select", help="Select candidate indices from a snapshot.")
    snap_select.add_argument("snapshot_id", type=str)
    snap_select.add_argument("indices", type=str, help="Comma-separated candidate indices, e.g. 1,2,5")
    snap_select.add_argument("--snapshot-ttl-hours", type=float, default=2.0)
    snap_select.add_argument("--state-file", type=str, default="state.json")
    snap_select.add_argument("--manual-log", type=str, default="manual_execution.log")

    snap_revalidate = snapshot_sub.add_parser("revalidate", help="Revalidate selected/approved candidates.")
    snap_revalidate.add_argument("snapshot_id", type=str)
    snap_revalidate.add_argument("--indices", type=str, default="", help="Optional comma-separated candidate indices.")
    snap_revalidate.add_argument("--snapshot-ttl-hours", type=float, default=2.0)
    snap_revalidate.add_argument("--timeout", type=float, default=55.0)
    snap_revalidate.add_argument("--state-file", type=str, default="state.json")
    snap_revalidate.add_argument("--manual-log", type=str, default="manual_execution.log")

    snap_approve = snapshot_sub.add_parser("approve", help="Approve candidates for manual execution.")
    snap_approve.add_argument("snapshot_id", type=str)
    snap_approve.add_argument("--indices", type=str, default="", help="Optional comma-separated candidate indices.")
    snap_approve.add_argument("--snapshot-ttl-hours", type=float, default=2.0)
    snap_approve.add_argument("--timeout", type=float, default=55.0)
    snap_approve.add_argument("--state-file", type=str, default="state.json")
    snap_approve.add_argument("--manual-log", type=str, default="manual_execution.log")

    snap_filled = snapshot_sub.add_parser("filled", help="Mark approved candidates as manually executed.")
    snap_filled.add_argument("snapshot_id", type=str)
    snap_filled.add_argument("indices", type=str, help="Comma-separated candidate indices.")
    snap_filled.add_argument("--state-file", type=str, default="state.json")
    snap_filled.add_argument("--manual-log", type=str, default="manual_execution.log")

    snap_skip = snapshot_sub.add_parser("skip", help="Mark a candidate as manually skipped.")
    snap_skip.add_argument("snapshot_id", type=str)
    snap_skip.add_argument("index", type=int)
    snap_skip.add_argument("reason", type=str)
    snap_skip.add_argument("--state-file", type=str, default="state.json")
    snap_skip.add_argument("--manual-log", type=str, default="manual_execution.log")

    return p

def parse_tier_arg(tier_str: str) -> List[CostTier]:
    """Parse comma-separated tier list"""
    tiers = []
    for t in tier_str.split(","):
        t = t.strip()
        if t == "60":
            tiers.append(CostTier.TIER_60)
        elif t == "70":
            tiers.append(CostTier.TIER_70)
        elif t == "80":
            tiers.append(CostTier.TIER_80)
        elif t == "90":
            tiers.append(CostTier.TIER_90)
    return tiers if tiers else [CostTier.TIER_60, CostTier.TIER_70, CostTier.TIER_80, CostTier.TIER_90]

def parse_csv_list(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]

async def run_scan(args: argparse.Namespace) -> None:
    print("Starting enhanced scan with tiered cost bands...")
    
    # Build configuration
    config = ScanConfig(
        min_top2_signal=args.min_top2_signal,
        max_top2_signal=args.max_top2_signal,
        min_leg_yes=args.min_leg,
        min_candidates=args.min_candidates,
        min_event_vol=args.min_event_vol,
        min_event_total_vol=args.min_event_total_vol,
        max_event_total_vol=args.max_event_total_vol,
        a_cap=args.a_cap,
        b_max_vwap=args.b_max_vwap,
        min_b_fill=args.min_b_fill,
        target_tiers=parse_tier_arg(args.tiers),
        b_target_shares=args.b_target,
        max_levels=args.max_levels,
        exclude_tag_ids=parse_csv_list(args.exclude_tags),
        exclude_keywords=parse_csv_list(args.exclude_keywords),
        resolve_within_days=args.resolve_within_days,
    )
    
    headers = {"User-Agent": "plybot/3.0", "Accept": "application/json"}
    async with aiohttp.ClientSession(headers=headers) as session:
        state = _load_state(args.state_file) if args.state_file else {"seen_pairs": {}, "traded_pairs": {}}
        while True:
            now = _now_ts()
            ttl_sec = max(0.0, float(args.dedupe_ttl_min) * 60.0)
            _prune_state(state, ttl_sec, now)
            print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] scanning with config:")
            print(f"  A-cap: {config.a_cap}")
            print(f"  Tiers: {[t.name for t in config.target_tiers]}")
            print(f"  Min top2 signal: {config.min_top2_signal}")
            if config.min_event_vol:
                print(f"  Min event 24h vol: {config.min_event_vol}")
            if config.min_event_total_vol:
                print(f"  Min event total vol: {config.min_event_total_vol}")
            if config.max_event_total_vol is not None:
                print(f"  Max event total vol: {config.max_event_total_vol}")
            if config.resolve_within_days is not None:
                print(f"  Resolve within days: {config.resolve_within_days}")
            sys.stdout.flush()
            
            try:
                events: List[Dict[str, Any]] = []
                page = 0
                total_cap = int(args.max_events or 0)
                end_date_min = None
                end_date_max = None
                if config.resolve_within_days is not None:
                    now_dt = datetime.now(timezone.utc)
                    end_date_min = now_dt.isoformat().replace("+00:00", "Z")
                    end_date_max = (now_dt + timedelta(days=float(config.resolve_within_days))).isoformat().replace("+00:00", "Z")
                while True:
                    if args.max_pages and page >= args.max_pages:
                        break
                    offset = page * args.limit_events
                    print(f"Fetching events: page {page + 1} (offset {offset}, limit {args.limit_events})")
                    batch = await fetch_events(
                        session,
                        args.limit_events,
                        args.tag_id,
                        args.timeout,
                        offset=offset,
                        end_date_min=end_date_min,
                        end_date_max=end_date_max,
                    )
                    if not batch:
                        break
                    events.extend(batch)
                    page += 1
                    if total_cap and len(events) >= total_cap:
                        events = events[:total_cap]
                        break
                    if len(batch) < args.limit_events:
                        break

                print(f"Fetched {len(events)} events from Gamma API ({page} page{'s' if page != 1 else ''})")
                
                # Get candidate hits using signal-based approach
                hits = scan_top2_per_event_signal(events, config, now)
                print(f"Found {len(hits)} potential pairs with top2_sum < 1.0")

                # Dedupe by persistent state
                if ttl_sec > 0:
                    for h in hits:
                        h["pair_id"] = _pair_id_from_hit(h)
                    before = len(hits)
                    hits = [h for h in hits if not _seen_recent(state, h["pair_id"], ttl_sec, now)]
                    print(f"After dedupe (ttl {args.dedupe_ttl_min}m): {len(hits)} (dropped {before - len(hits)})")

                if args.depth:
                    print(f"Enriching with depth data and applying filters...")
                    reject_counts: Optional[Dict[str, int]] = {} if args.debug_rejects else None
                    enriched_hits = await enrich_hits_with_depth_and_filter(
                        session=session,
                        hits=hits,
                        timeout_s=args.timeout,
                        config=config,
                        reject_counts=reject_counts,
                    )
                    print(f"After depth analysis: {len(enriched_hits)} events match constraints")
                    if args.debug_rejects and reject_counts:
                        print("Reject reason counts:")
                        for reason, count in sorted(reject_counts.items(), key=lambda x: (-x[1], x[0])):
                            print(f"  {reason}: {count}")
                    for h in enriched_hits:
                        if "pair_id" not in h:
                            h["pair_id"] = _pair_id_from_hit(h)
                        _mark_seen(state, h["pair_id"], now)
                    
                    # Sort by total cost (cheapest first)
                    enriched_hits.sort(key=lambda x: x.get("total_cost", 1e9))
                    
                    print_hits_by_tier(enriched_hits, args.top, show_depth=args.depth)
                else:
                    # Simple display without depth
                    hits.sort(key=lambda x: x["top2_sum"], reverse=True)
                    for h in hits:
                        if "pair_id" not in h:
                            h["pair_id"] = _pair_id_from_hit(h)
                        _mark_seen(state, h["pair_id"], now)
                    print_hits_by_tier(hits, args.top, show_depth=False)

            except Exception as e:
                print(f"ERROR: {e}", file=sys.stderr)

            if args.state_file:
                try:
                    _save_state(args.state_file, state)
                except Exception as e:
                    print(f"WARNING: failed to save state: {e}", file=sys.stderr)

            if not args.watch:
                break
            await asyncio.sleep(max(0.2, float(args.interval)))

async def run_trade(args: argparse.Namespace) -> None:
    print("Starting dry-run trading loop...")

    config = ScanConfig(
        min_top2_signal=args.min_top2_signal,
        max_top2_signal=args.max_top2_signal,
        min_leg_yes=args.min_leg,
        min_candidates=args.min_candidates,
        min_event_vol=args.min_event_vol,
        min_event_total_vol=args.min_event_total_vol,
        max_event_total_vol=args.max_event_total_vol,
        a_cap=args.a_cap,
        b_max_vwap=args.b_max_vwap,
        min_b_fill=args.min_b_fill,
        target_tiers=parse_tier_arg(args.tiers),
        b_target_shares=args.b_target,
        max_levels=args.max_levels,
        exclude_tag_ids=parse_csv_list(args.exclude_tags),
        exclude_keywords=parse_csv_list(args.exclude_keywords),
        resolve_within_days=args.resolve_within_days,
    )

    headers = {"User-Agent": "plybot/3.0", "Accept": "application/json"}
    async with aiohttp.ClientSession(headers=headers) as session:
        state = _load_state(args.state_file) if args.state_file else {"seen_pairs": {}, "traded_pairs": {}, "positions": {}}

        while True:
            now = _now_ts()
            ttl_sec = max(0.0, float(args.dedupe_ttl_min) * 60.0)
            _prune_state(state, ttl_sec, now)

            positions = state.get("positions", {})
            if not isinstance(positions, dict):
                positions = {}
                state["positions"] = positions

            # ----------------------------
            # Update open positions (PnL + trailing + kill switch)
            # ----------------------------
            if positions:
                held_tokens = set()
                all_event_tokens = set()
                for pos in positions.values():
                    a_tok = pos.get("a_token_id")
                    b_tok = pos.get("b_token_id")
                    if a_tok:
                        held_tokens.add(a_tok)
                    if b_tok:
                        held_tokens.add(b_tok)
                    for tok in pos.get("event_yes_tokens", []):
                        if tok:
                            all_event_tokens.add(tok)

                other_tokens = all_event_tokens - held_tokens
                token_ids = list(held_tokens | other_tokens)

                books = await fetch_books_for_tokens(session, token_ids, args.timeout)
                prices: Dict[str, float] = {}
                for tok, book in books.items():
                    px = _best_price_for_sell(book)
                    if px is not None:
                        prices[tok] = float(px)

                # Global kill switch: any non-held candidate >= 40% YES
                kill_token = None
                kill_price = None
                for tok in other_tokens:
                    px = prices.get(tok)
                    if px is not None and px >= 0.40:
                        kill_token = tok
                        kill_price = px
                        break

                if kill_token is not None:
                    _append_log(args.dry_run_log, f"KILL_SWITCH triggered by token {kill_token} at {kill_price:.4f}")
                    for pid, pos in list(positions.items()):
                        _append_log(
                            args.dry_run_log,
                            f"EXIT kill_switch pos={pid} event='{pos.get('event_title')}'"
                        )
                        positions.pop(pid, None)
                else:
                    for pid, pos in list(positions.items()):
                        leg_vals = _position_leg_values(pos, prices)
                        if leg_vals is None:
                            continue
                        a_value, b_value, value = leg_vals

                        a_size = float(pos.get("a_size") or 0.0)
                        b_size = float(pos.get("b_size") or 0.0)
                        a_cost = float(pos.get("a_cost") or 0.0)
                        b_cost = float(pos.get("b_cost") or 0.0)
                        if a_cost <= 0.0:
                            a_cost = a_size * float(pos.get("a_vwap") or 0.0)
                            pos["a_cost"] = float(a_cost)
                        if b_cost <= 0.0:
                            b_cost = b_size * float(pos.get("b_vwap") or 0.0)
                            pos["b_cost"] = float(b_cost)
                        total_cost = float(pos.get("total_cost") or 0.0)
                        if total_cost <= 0.0:
                            total_cost = a_cost + b_cost
                            pos["total_cost"] = float(total_cost)
                        if a_cost <= 0.0 or b_cost <= 0.0 or total_cost <= 0.0:
                            continue

                        roi = (value - total_cost) / total_cost
                        a_roi = (a_value - a_cost) / a_cost
                        b_roi = (b_value - b_cost) / b_cost
                        pos["last_value"] = float(value)
                        pos["last_roi"] = float(roi)
                        pos["a_last_value"] = float(a_value)
                        pos["b_last_value"] = float(b_value)
                        pos["a_last_roi"] = float(a_roi)
                        pos["b_last_roi"] = float(b_roi)
                        pos["last_update"] = now

                        peak = pos.get("peak_roi")
                        if peak is None or roi > float(peak):
                            pos["peak_roi"] = float(roi)

                        a_peak = pos.get("a_peak_roi")
                        if a_peak is None or a_roi > float(a_peak):
                            pos["a_peak_roi"] = float(a_roi)
                        b_peak = pos.get("b_peak_roi")
                        if b_peak is None or b_roi > float(b_peak):
                            pos["b_peak_roi"] = float(b_roi)

                        if not pos.get("a_trailing_active") and a_roi >= 0.10:
                            pos["a_trailing_active"] = True
                            pos["a_trailing_floor"] = 0.07
                            _append_log(
                                args.dry_run_log,
                                f"TRAILING_ACTIVE pos={pid} leg=A roi={a_roi:.2%} floor=7.00%"
                            )
                        if not pos.get("b_trailing_active") and b_roi >= 0.10:
                            pos["b_trailing_active"] = True
                            pos["b_trailing_floor"] = 0.07
                            _append_log(
                                args.dry_run_log,
                                f"TRAILING_ACTIVE pos={pid} leg=B roi={b_roi:.2%} floor=7.00%"
                            )

                        if pos.get("a_trailing_active") and a_roi <= float(pos.get("a_trailing_floor") or 0.07):
                            _append_log(
                                args.dry_run_log,
                                f"EXIT trailing_stop pos={pid} leg=A roi={a_roi:.2%} value=${a_value:,.2f}"
                            )
                            positions.pop(pid, None)
                            continue
                        if pos.get("b_trailing_active") and b_roi <= float(pos.get("b_trailing_floor") or 0.07):
                            _append_log(
                                args.dry_run_log,
                                f"EXIT trailing_stop pos={pid} leg=B roi={b_roi:.2%} value=${b_value:,.2f}"
                            )
                            positions.pop(pid, None)
                            continue

            # ----------------------------
            # Find new entries and open positions (dry-run)
            # ----------------------------
            try:
                events: List[Dict[str, Any]] = []
                page = 0
                total_cap = int(args.max_events or 0)
                end_date_min = None
                end_date_max = None
                if config.resolve_within_days is not None:
                    now_dt = datetime.now(timezone.utc)
                    end_date_min = now_dt.isoformat().replace("+00:00", "Z")
                    end_date_max = (now_dt + timedelta(days=float(config.resolve_within_days))).isoformat().replace("+00:00", "Z")
                while True:
                    if args.max_pages and page >= args.max_pages:
                        break
                    offset = page * args.limit_events
                    batch = await fetch_events(
                        session,
                        args.limit_events,
                        args.tag_id,
                        args.timeout,
                        offset=offset,
                        end_date_min=end_date_min,
                        end_date_max=end_date_max,
                    )
                    if not batch:
                        break
                    events.extend(batch)
                    page += 1
                    if total_cap and len(events) >= total_cap:
                        events = events[:total_cap]
                        break
                    if len(batch) < args.limit_events:
                        break

                hits = scan_top2_per_event_signal(events, config, now)
                enriched_hits = await enrich_hits_with_depth_and_filter(
                    session=session,
                    hits=hits,
                    timeout_s=args.timeout,
                    config=config,
                    reject_counts=None,
                )

                enriched_hits.sort(key=lambda x: (_hit_end_ts(x), x.get("total_cost", 1e9)))

                for h in enriched_hits:
                    pid = _pair_id_from_hit(h)
                    if pid in positions:
                        continue
                    if pid in state.get("traded_pairs", {}):
                        continue

                    market_ids = h.get("event_market_ids") or []
                    token_map = await fetch_yes_token_ids_for_markets(session, market_ids, args.timeout)
                    event_yes_tokens = list(token_map.values())

                    pos = {
                        "position_id": pid,
                        "event_id": h.get("event_id"),
                        "event_title": h.get("event_title"),
                        "end_date": h.get("end_date"),
                        "end_ts": _hit_end_ts(h),
                        "a_token_id": h.get("a_token_id"),
                        "b_token_id": h.get("b_token_id"),
                        "a_size": float(h.get("a_filled") or 0.0),
                        "b_size": float(h.get("b_filled") or 0.0),
                        "a_vwap": float(h.get("a_vwap") or 0.0),
                        "b_vwap": float(h.get("b_vwap") or 0.0),
                        "a_cost": float(h.get("a_cost") or 0.0),
                        "b_cost": float(h.get("b_cost") or 0.0),
                        "sum_vwap": float(h.get("sum_vwap") or 0.0),
                        "total_cost": float(h.get("total_cost") or 0.0),
                        "entry_ts": now,
                        "peak_roi": None,
                        "a_peak_roi": None,
                        "b_peak_roi": None,
                        "trailing_active": False,
                        "trailing_floor": None,
                        "a_trailing_active": False,
                        "b_trailing_active": False,
                        "a_trailing_floor": None,
                        "b_trailing_floor": None,
                        "event_yes_tokens": event_yes_tokens,
                    }
                    positions[pid] = pos
                    _mark_traded(state, pid, now)

                    _append_log(
                        args.dry_run_log,
                        f"ENTRY pos={pid} event='{h.get('event_title')}' total_cost=${pos['total_cost']:,.2f} "
                        f"A={pos['a_size']:.2f}@{pos['a_vwap']:.4f} B={pos['b_size']:.2f}@{pos['b_vwap']:.4f}"
                    )

            except Exception as e:
                _append_log(args.dry_run_log, f"ERROR trade_cycle: {e}")

            if args.state_file:
                try:
                    _save_state(args.state_file, state)
                except Exception:
                    pass

            if args.once:
                break
            await asyncio.sleep(max(0.2, float(args.interval)))

async def run_snapshot(args: argparse.Namespace) -> None:
    now = _now_ts()
    state = _load_state(args.state_file) if args.state_file else {"seen_pairs": {}, "traded_pairs": {}, "positions": {}, "snapshots": {}}
    _snapshot_bucket(state)
    _expire_old_snapshot_candidates(state, now)

    if args.snapshot_cmd == "create":
        cfg_dict = _snapshot_config_dict(args)
        config = _scan_config_from_snapshot_cfg(cfg_dict)
        headers = {"User-Agent": "plybot/3.0", "Accept": "application/json"}
        async with aiohttp.ClientSession(headers=headers) as session:
            enriched_hits = await _fetch_enriched_hits_for_config(
                session=session,
                config=config,
                timeout_s=args.timeout,
                limit_events=args.limit_events,
                max_pages=args.max_pages,
                max_events=args.max_events,
                tag_id=args.tag_id,
                now=now,
            )

        top_n = max(1, int(args.top_n))
        chosen = enriched_hits[:top_n]
        sid = _snapshot_id(now)
        candidates = [_candidate_from_hit(h, i + 1, now) for i, h in enumerate(chosen)]
        snapshot = {
            "snapshot_id": sid,
            "created_ts": now,
            "status": "open",
            "candidate_limit": top_n,
            "ttl_hours": float(args.snapshot_ttl_hours),
            "config": cfg_dict,
            "candidates": candidates,
        }
        _snapshot_bucket(state)[sid] = snapshot

        invalidated = _invalidate_conflicting_candidates(
            state=state,
            new_snapshot_id=sid,
            new_pair_ids=[str(c.get("pair_id") or "") for c in candidates],
            now=now,
        )
        _save_state(args.state_file, state)
        _append_json_human_log(
            args.manual_log,
            {
                "type": "snapshot_created",
                "snapshot_id": sid,
                "candidate_count": len(candidates),
                "invalidated_conflicts": invalidated,
            },
            f"SNAPSHOT_CREATE id={sid} candidates={len(candidates)} invalidated_conflicts={invalidated}",
        )
        print(f"snapshot_id: {sid}")
        print(f"candidates: {len(candidates)}")
        if invalidated:
            print(f"invalidated_conflicts: {invalidated}")
        _print_snapshot(snapshot)
        return

    snaps = _snapshot_bucket(state)
    snapshot = snaps.get(args.snapshot_id)
    if not isinstance(snapshot, dict):
        print(f"ERROR: snapshot not found: {args.snapshot_id}", file=sys.stderr)
        return
    cands = snapshot.get("candidates", [])
    if not isinstance(cands, list):
        cands = []
        snapshot["candidates"] = cands

    if args.snapshot_cmd == "show":
        _print_snapshot(snapshot)
        return

    ttl_hours = float(getattr(args, "snapshot_ttl_hours", snapshot.get("ttl_hours", 2.0)) or 2.0)
    ttl_sec = _snapshot_ttl_sec(ttl_hours)
    terminals = _snapshot_terminal_statuses()
    idx_filter = _parse_index_csv(getattr(args, "indices", "")) if hasattr(args, "indices") else []

    if args.snapshot_cmd == "select":
        target_idxs = _parse_index_csv(args.indices)
        changed = 0
        for c in cands:
            idx = int(c.get("idx") or 0)
            if idx not in target_idxs:
                continue
            if str(c.get("status") or "") in terminals:
                continue
            c["status"] = "selected"
            c["selected_ts"] = now
            c["revalidate_due_ts"] = now + ttl_sec
            c["expires_ts"] = None
            c["updated_ts"] = now
            changed += 1
            _append_json_human_log(
                args.manual_log,
                {
                    "type": "snapshot_selected",
                    "snapshot_id": args.snapshot_id,
                    "idx": idx,
                    "pair_id": c.get("pair_id"),
                    "revalidate_due_ts": c.get("revalidate_due_ts"),
                },
                f"SNAPSHOT_SELECT id={args.snapshot_id} idx={idx} revalidate_in_h={ttl_hours:.2f}",
            )
        _save_state(args.state_file, state)
        print(f"selected: {changed}")
        _print_snapshot_filtered(snapshot, {"selected"})
        return

    if args.snapshot_cmd in {"revalidate", "approve"}:
        target_statuses = {"selected", "needs_revalidation", "revalidated", "approved_manual"}
        if args.snapshot_cmd == "revalidate":
            target_statuses.add("expired")
        if idx_filter:
            target = [c for c in cands if int(c.get("idx") or 0) in idx_filter and str(c.get("status") or "") in target_statuses]
        else:
            target = [c for c in cands if str(c.get("status") or "") in target_statuses]

        if not target:
            print("No selected/revalidated candidates to process.")
            return

        cfg = _scan_config_from_snapshot_cfg(snapshot.get("config") or {})
        headers = {"User-Agent": "plybot/3.0", "Accept": "application/json"}
        async with aiohttp.ClientSession(headers=headers) as session:
            for c in target:
                idx = int(c.get("idx") or 0)
                status = str(c.get("status") or "")
                due_ts = c.get("revalidate_due_ts")
                needs_refresh = status == "needs_revalidation"
                if status == "selected" and due_ts is not None:
                    try:
                        needs_refresh = now > float(due_ts)
                    except Exception:
                        needs_refresh = True

                if args.snapshot_cmd == "approve" and (status == "selected" or needs_refresh):
                    ok, reason = await _revalidate_candidate(session, c, cfg, args.timeout, ttl_sec, now)
                    _append_json_human_log(
                        args.manual_log,
                        {
                            "type": "snapshot_auto_revalidate_for_approve",
                            "snapshot_id": args.snapshot_id,
                            "idx": idx,
                            "ok": ok,
                            "reason": reason,
                            "metrics": c.get("latest_metrics"),
                        },
                        f"SNAPSHOT_AUTO_REVALIDATE id={args.snapshot_id} idx={idx} ok={ok} reason={reason}",
                    )
                elif args.snapshot_cmd == "revalidate":
                    ok, reason = await _revalidate_candidate(session, c, cfg, args.timeout, ttl_sec, now)
                    _append_json_human_log(
                        args.manual_log,
                        {
                            "type": "snapshot_revalidated",
                            "snapshot_id": args.snapshot_id,
                            "idx": idx,
                            "ok": ok,
                            "reason": reason,
                            "metrics": c.get("latest_metrics"),
                        },
                        f"SNAPSHOT_REVALIDATE id={args.snapshot_id} idx={idx} ok={ok} reason={reason}",
                    )

        if args.snapshot_cmd == "approve":
            approved = 0
            for c in target:
                idx = int(c.get("idx") or 0)
                if str(c.get("status") or "") != "revalidated":
                    continue
                c["status"] = "approved_manual"
                c["approved_ts"] = now
                c["updated_ts"] = now
                approved += 1
                _append_json_human_log(
                    args.manual_log,
                    {
                        "type": "snapshot_approved_manual",
                        "snapshot_id": args.snapshot_id,
                        "idx": idx,
                        "pair_id": c.get("pair_id"),
                        "metrics": c.get("latest_metrics"),
                    },
                    f"SNAPSHOT_APPROVE id={args.snapshot_id} idx={idx} approved_for_manual_execution=1",
                )
            print(f"approved: {approved}")

        _save_state(args.state_file, state)
        if args.snapshot_cmd == "revalidate":
            _print_snapshot_filtered(snapshot, {"revalidated", "expired"})
        else:
            _print_snapshot(snapshot)
        return

    if args.snapshot_cmd == "filled":
        target_idxs = _parse_index_csv(args.indices)
        filled = 0
        for c in cands:
            idx = int(c.get("idx") or 0)
            if idx not in target_idxs:
                continue
            if str(c.get("status") or "") != "approved_manual":
                continue
            c["status"] = "filled_manual"
            c["filled_ts"] = now
            c["updated_ts"] = now
            filled += 1
            _append_json_human_log(
                args.manual_log,
                {
                    "type": "snapshot_filled_manual",
                    "snapshot_id": args.snapshot_id,
                    "idx": idx,
                    "pair_id": c.get("pair_id"),
                    "metrics": c.get("latest_metrics") or c.get("snapshot_metrics"),
                },
                f"SNAPSHOT_FILLED id={args.snapshot_id} idx={idx} manual_execution_confirmed=1",
            )
        _save_state(args.state_file, state)
        print(f"filled: {filled}")
        _print_snapshot(snapshot)
        return

    if args.snapshot_cmd == "skip":
        changed = 0
        for c in cands:
            if int(c.get("idx") or 0) != int(args.index):
                continue
            c["status"] = "skipped_manual"
            c["skip_reason"] = args.reason
            c["updated_ts"] = now
            changed = 1
            _append_json_human_log(
                args.manual_log,
                {
                    "type": "snapshot_skipped_manual",
                    "snapshot_id": args.snapshot_id,
                    "idx": int(args.index),
                    "reason": args.reason,
                },
                f"SNAPSHOT_SKIP id={args.snapshot_id} idx={int(args.index)} reason={args.reason}",
            )
            break
        _save_state(args.state_file, state)
        print(f"skipped: {changed}")
        _print_snapshot(snapshot)
        return

def main() -> None:
    args = build_parser().parse_args()
    if args.cmd == "scan":
        asyncio.run(run_scan(args))
    elif args.cmd == "trade":
        asyncio.run(run_trade(args))
    elif args.cmd == "snapshot":
        asyncio.run(run_snapshot(args))

if __name__ == "__main__":
    main()
