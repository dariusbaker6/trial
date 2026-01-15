#!/usr/bin/env python3
# Streamlit app: TrenchFeed - robust Top Coins link generation (Dexscreener, Solscan, Birdeye)
# Key fixes:
# 1) normalize token column names from views (token_address, base_token, token, token_addr, mint, mint_address)
# 2) backfill token_address from pairs table when only pair_address exists
# 3) dexscreener prefers pair_address if present, else falls back to token_address
# 4) LinkColumn config applied only if available
# 5) sanity caption shows row counts and valid token/pair counts
# 6) PREVIEW SESSION: Time-limited preview with per-user isolation via Supabase

import os
from typing import Dict, List, Optional, Iterable, Set, Tuple
import numpy as np
import pandas as pd
import requests
import streamlit as st
import json
import time
import uuid
import hashlib
from datetime import datetime, timezone

# Optional auto-refresh component.  Attempt import from the third‑party package; if unavailable, leave as None.
try:
    from streamlit_autorefresh import st_autorefresh  # type: ignore
except Exception:
    st_autorefresh = None  # fallback: no automatic refresh

# ============================= Config =============================
st.set_page_config(page_title="TrenchFeed - Early Leaders", page_icon="🚀", layout="wide")

# Preview duration configuration (default: 45 minutes = 2700 seconds)
PREVIEW_DURATION_SECONDS = int(os.environ.get("PREVIEW_DURATION_SECONDS", "2700"))
try:
    PREVIEW_DURATION_SECONDS = int(st.secrets.get("PREVIEW_DURATION_SECONDS", PREVIEW_DURATION_SECONDS))
except Exception:
    pass

# Redirect URL when preview expires
EXPIRED_REDIRECT_URL = "https://sold-1.onrender.com"

# Inject custom CSS for a modern, beautiful dashboard.
custom_css = """
/* Remove default Streamlit main menu and footer */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

/* General body styling with gradient background and subtle text coloring */
[data-testid="stAppViewContainer"] {
    background-color: #0F2027;
    background-image: linear-gradient(120deg, #0F2027 0%, #203A43 50%, #2C5364 100%);
    color: #E0E0E0;
}

/* Style the sidebar with a darker shade and consistent text color */
[data-testid="stSidebar"] {
    background-color: #1A2B3A;
    color: #E0E0E0;
}

/* Enhance appearance of sliders and toggles in sidebar */
[data-testid="stSidebar"] .stSlider > div, [data-testid="stSidebar"] .stToggle > label {
    color: #E0E0E0;
}
[data-testid="stSidebar"] .stSlider > div[data-baseweb="slider"] {
    background-color: rgba(255,255,255,0.05);
    border-radius: 4px;
    padding: 4px;
}

/* Headings styling with gradient text effect */
h1 {
    font-size: 2.5rem;
    font-weight: 700;
    background: linear-gradient(90deg, #00C9FF, #92FE9D);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
h2, h3, h4 {
    color: #E0E0E0;
}

/* Style hyperlinks to stand out on dark background */
a {
    color: #71C7EC;
}

/* Style for Streamlit dataframes (Ag-Grid) */
.ag-theme-streamlit {
    background-color: rgba(255,255,255,0.02) !important;
    color: #E0E0E0 !important;
    font-size: 14px;
}
.ag-theme-streamlit .ag-header, .ag-theme-streamlit .ag-header-cell {
    background-color: rgba(255,255,255,0.06) !important;
    color: #E0E0E0 !important;
    font-weight: bold;
}
.ag-theme-streamlit .ag-row-hover {
    background-color: rgba(255,255,255,0.05) !important;
}

/* Preview timer banner styling */
.preview-banner {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    z-index: 9999;
    background: linear-gradient(90deg, #1A2B3A 0%, #2C5364 100%);
    border-bottom: 2px solid rgba(0, 201, 255, 0.4);
    padding: 8px 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.9rem;
}
.preview-banner .timer {
    display: flex;
    align-items: center;
    gap: 8px;
    color: #92FE9D;
    font-weight: 600;
}
.preview-banner .timer.warning {
    color: #FFB900;
}
.preview-banner .timer.critical {
    color: #FF6B6B;
    animation: pulse-critical 1s ease-in-out infinite;
}
@keyframes pulse-critical {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.6; }
}
.preview-banner .cta {
    background: linear-gradient(90deg, #00C9FF, #92FE9D);
    color: #0F2027;
    padding: 6px 16px;
    border-radius: 6px;
    text-decoration: none;
    font-weight: 600;
    transition: transform 0.2s, box-shadow 0.2s;
}
.preview-banner .cta:hover {
    transform: translateY(-1px);
    box-shadow: 0 2px 10px rgba(0, 201, 255, 0.4);
}
/* Add top padding to main content to account for fixed banner */
[data-testid="stAppViewContainer"] > section > div {
    padding-top: 60px !important;
}

/* Expired overlay styling */
.expired-overlay {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(15, 32, 39, 0.98);
    z-index: 99999;
    display: flex;
    justify-content: center;
    align-items: center;
}
.expired-container {
    max-width: 500px;
    padding: 40px;
    background: rgba(255, 255, 255, 0.05);
    border-radius: 16px;
    border: 1px solid rgba(255, 255, 255, 0.1);
    text-align: center;
}
.expired-title {
    font-size: 2rem;
    margin-bottom: 1rem;
    background: linear-gradient(90deg, #00C9FF, #92FE9D);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.expired-subtitle {
    color: #A0A0A0;
    margin-bottom: 2rem;
    line-height: 1.6;
}
.subscribe-btn {
    display: inline-block;
    padding: 14px 36px;
    background: linear-gradient(90deg, #00C9FF, #92FE9D);
    color: #0F2027 !important;
    text-decoration: none;
    border-radius: 8px;
    font-weight: bold;
    font-size: 1.1rem;
    transition: transform 0.2s, box-shadow 0.2s;
}
.subscribe-btn:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 20px rgba(0, 201, 255, 0.3);
}

/* Stream status indicator styling */
.stream-status {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 6px 14px;
    border-radius: 20px;
    font-size: 0.85rem;
    font-weight: 600;
    letter-spacing: 0.5px;
}
.stream-status.live {
    background: linear-gradient(135deg, rgba(0, 201, 255, 0.15) 0%, rgba(146, 254, 157, 0.15) 100%);
    border: 1px solid rgba(0, 201, 255, 0.4);
    color: #92FE9D;
}
.stream-status.paused {
    background: rgba(255, 185, 0, 0.12);
    border: 1px solid rgba(255, 185, 0, 0.4);
    color: #FFB900;
}
.stream-status .pulse {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #92FE9D;
    animation: pulse-glow 1.5s ease-in-out infinite;
}
.stream-status.paused .pulse {
    background: #FFB900;
    animation: none;
}
@keyframes pulse-glow {
    0%, 100% { opacity: 1; box-shadow: 0 0 4px rgba(146, 254, 157, 0.6); }
    50% { opacity: 0.5; box-shadow: 0 0 12px rgba(146, 254, 157, 0.9); }
}
"""
st.markdown(f"<style>{custom_css}</style>", unsafe_allow_html=True)

def cfg(key: str, default: str = "") -> str:
    v = os.environ.get(key, default)
    try:
        v = st.secrets.get(key, v)  # type: ignore[attr-defined]
    except Exception:
        pass
    return str(v).strip()

SB_URL    = cfg("SUPABASE_URL").rstrip("/")
SB_KEY    = cfg("SUPABASE_SERVICE_ROLE")
SB_SCHEMA = cfg("SUPABASE_SCHEMA", "public")

if not SB_URL or not SB_KEY:
    st.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE")
    st.stop()

SESSION = requests.Session()
SESSION.headers.update({
    "apikey": SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type": "application/json",
    "Accept-Profile": SB_SCHEMA,
    "Content-Profile": SB_SCHEMA,
})

# ============================= PREVIEW SESSION MANAGEMENT =============================
STRIPE_PURCHASE_URL = "https://buy.stripe.com/dRm7sLdUF0DR2m7ga07IY08"

def get_or_create_anon_id() -> str:
    if "preview_anon_id" in st.session_state and st.session_state.preview_anon_id:
        return st.session_state.preview_anon_id
    query_params = st.query_params
    if "aid" in query_params:
        anon_id = query_params["aid"]
        st.session_state.preview_anon_id = anon_id
        return anon_id
    anon_id = f"anon_{uuid.uuid4().hex[:16]}"
    st.session_state.preview_anon_id = anon_id
    st.query_params["aid"] = anon_id
    return anon_id


def get_preview_session(anon_id: str) -> Optional[Dict]:
    try:
        url = f"{SB_URL}/rest/v1/trial_sessions"
        params = {
            "select": "anon_id,started_at,last_seen",
            "anon_id": f"eq.{anon_id}",
            "limit": "1"
        }
        response = SESSION.get(url, params=params, timeout=10)
        if response.status_code in (200, 206):
            data = response.json()
            if data and len(data) > 0:
                return data[0]
        return None
    except Exception:
        return None


def create_preview_session(anon_id: str) -> Optional[Dict]:
    try:
        url = f"{SB_URL}/rest/v1/trial_sessions"
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            "anon_id": anon_id,
            "started_at": now,
            "last_seen": now
        }
        headers = SESSION.headers.copy()
        headers["Prefer"] = "return=representation"
        response = SESSION.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code in (200, 201):
            data = response.json()
            if data and len(data) > 0:
                return data[0]
            return payload
        return None
    except Exception:
        return {
            "anon_id": anon_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "last_seen": datetime.now(timezone.utc).isoformat()
        }


def update_last_seen(anon_id: str) -> None:
    try:
        url = f"{SB_URL}/rest/v1/trial_sessions"
        params = {"anon_id": f"eq.{anon_id}"}
        payload = {"last_seen": datetime.now(timezone.utc).isoformat()}
        SESSION.patch(url, params=params, json=payload, timeout=5)
    except Exception:
        pass


def calculate_remaining_time(started_at_str: str) -> int:
    try:
        started_at = pd.to_datetime(started_at_str, utc=True)
        now = pd.Timestamp.now(tz="UTC")
        elapsed_seconds = (now - started_at).total_seconds()
        remaining = PREVIEW_DURATION_SECONDS - elapsed_seconds
        return max(0, int(remaining))
    except Exception:
        return PREVIEW_DURATION_SECONDS


def format_time_remaining(seconds: int) -> str:
    if seconds <= 0:
        return "00:00"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def show_preview_banner(remaining_seconds: int) -> None:
    time_str = format_time_remaining(remaining_seconds)
    if remaining_seconds <= 60:
        timer_class = "critical"
    elif remaining_seconds <= 300:
        timer_class = "warning"
    else:
        timer_class = ""
    banner_html = f"""
    <div class="preview-banner">
        <div class="timer {timer_class}">
            ⏱️ Preview time remaining: <strong>{time_str}</strong>
        </div>
        <a href="{STRIPE_PURCHASE_URL}" target="_blank" class="cta">
            Subscribe for Full Access →
        </a>
    </div>
    """
    st.markdown(banner_html, unsafe_allow_html=True)


def show_expired_redirect() -> None:
    expired_html = f"""
    <div class="expired-overlay">
        <div class="expired-container">
            <div class="expired-title">⏰ Preview Expired</div>
            <div class="expired-subtitle">
                Your free preview has ended. Subscribe now to get full access 
                to TrenchFeed's powerful trading intelligence platform, including 
                a 5-day free trial!
            </div>
            <a href="{EXPIRED_REDIRECT_URL}" class="subscribe-btn">
                Get Full Access →
            </a>
        </div>
    </div>
    <script>
        setTimeout(function() {{
            window.location.href = "{EXPIRED_REDIRECT_URL}";
        }}, 3000);
    </script>
    """
    st.markdown(expired_html, unsafe_allow_html=True)
    st.stop()


def check_preview_session() -> int:
    anon_id = get_or_create_anon_id()
    session = get_preview_session(anon_id)
    if session is None:
        session = create_preview_session(anon_id)
        if session is None:
            return PREVIEW_DURATION_SECONDS
    started_at = session.get("started_at")
    if not started_at:
        return PREVIEW_DURATION_SECONDS
    remaining = calculate_remaining_time(started_at)
    if remaining <= 0:
        show_expired_redirect()
    update_last_seen(anon_id)
    return remaining


# ============================= CHECK PREVIEW SESSION =============================
remaining_preview_seconds = check_preview_session()
show_preview_banner(remaining_preview_seconds)

if st_autorefresh is not None:
    refresh_interval = 10000 if remaining_preview_seconds <= 60 else 30000
    st_autorefresh(interval=refresh_interval, key="preview_refresh")

# ============================= AUTHENTICATED USER CONTENT BELOW =============================

# ============================= Helpers =============================
def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")

def iso(ts: pd.Timestamp) -> str:
    if ts is None or pd.isna(ts):
        return ""
    try:
        utc_ts = pd.Timestamp(ts).tz_convert("UTC")
    except Exception:
        utc_ts = pd.Timestamp(ts).tz_localize("UTC")
    utc_ts = utc_ts.floor("s")
    return utc_ts.strftime("%Y-%m-%dT%H:%M:%SZ")

def iso_hours_ago(hours: int) -> str:
    ts = now_utc() - pd.Timedelta(hours=hours)
    ts = ts.floor("s")
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")

def to_dt(x):
    return pd.to_datetime(x, utc=True, errors="coerce")

def numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# =====================================================================
# Streaming helpers
ENTERPRISE_URL: str = "https://api.trenchfeed.cc/stream/enterprise"
ENTERPRISE_TOKEN: str = "tk_live_henCCxnVFK4E1CaGlNRO7aH8PA11cxtf"

def stream_enterprise(api_key: str = ENTERPRISE_TOKEN, *, max_events: int = 200) -> Iterable[Dict]:
    url = ENTERPRISE_URL
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "text/event-stream"
    }
    try:
        with requests.get(url, headers=headers, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            buffer = ""
            event_count = 0
            for chunk in resp.iter_content(chunk_size=1024):
                try:
                    buffer += chunk.decode("utf-8", errors="ignore")
                except Exception:
                    continue
                while "\n\n" in buffer:
                    frame, buffer = buffer.split("\n\n", 1)
                    for line in frame.split("\n"):
                        if line.startswith("data: "):
                            json_text = line[6:]
                            try:
                                payload = json.loads(json_text)
                                yield payload
                                event_count += 1
                                if event_count >= max_events:
                                    return
                            except Exception:
                                continue
    except Exception:
        return

# ============================= UI helpers =============================
DISCLAIMER_TEXT: str = (
    "Token names and symbols displayed here are user‑generated by token creators in the market "
    "and do not reflect the views of TrenchFeed."
    " Investments made using information displayed here are purely at your own risk. TrenchFeed does not provide financial investment advice."
)

def display_disclaimer() -> None:
    st.markdown(
        f"<div style='background: rgba(255, 255, 255, 0.1); padding: 0.5rem 0.75rem; "
        f"border-left: 4px solid #FFB900; margin-bottom: 1rem;'>"
        f"<strong>Disclaimer:</strong> {DISCLAIMER_TEXT}</div>",
        unsafe_allow_html=True,
    )

# REST base
def rest_get(table: str, params: Dict[str, str], start: int = 0, stop: int = 9999, timeout: int = 30) -> List[Dict]:
    url = f"{SB_URL}/rest/v1/{table}"
    headers = SESSION.headers.copy()
    headers["Range-Unit"] = "items"
    headers["Range"] = f"{start}-{stop}"
    r = SESSION.get(url, params=params, headers=headers, timeout=timeout)
    if r.status_code in (200, 206):
        try:
            return r.json()
        except Exception:
            return []
    if r.status_code == 500:
        try:
            st.info(f"The '{table}' view timed out. Please reduce the date range or lower the row limit and try again.")
        except Exception:
            pass
        return []
    if r.status_code == 404:
        body_lower = (r.text or "").lower()
        if any(tok in body_lower for tok in ["public.listings", "public.risk_flags", "could not find the table"]):
            return []
    st.warning(f"Fetch error for {table}: HTTP {r.status_code}: {r.text[:240]}")
    return []

def fetch_table(
    table: str,
    select: str = "*",
    where: Optional[Dict[str, str]] = None,
    order: Optional[str] = None,
    limit: int = 1000,
    start: int = 0,
) -> pd.DataFrame:
    params: Dict[str, str] = {"select": select, "limit": str(limit)}
    if order:
        params["order"] = order
    if where:
        params.update(where)
    rows = rest_get(table, params=params, start=start, stop=start + limit - 1)
    return pd.DataFrame(rows or [])

def fetch_view(view_name: str, limit: int) -> pd.DataFrame:
    df = fetch_table(view_name, select="*", limit=limit)
    if df.empty:
        return df
    for col in ["snapshot_ts","pair_created_at","last_seen","last_window","created_at","start_ts","end_ts","ts"]:
        if col in df.columns:
            df[col] = to_dt(df[col])
    return df

# Chunk helper
def _chunk(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

# Token metadata join
def fetch_tokens_for_addresses(addresses: Iterable[str]) -> pd.DataFrame:
    addrs = sorted({a for a in addresses if isinstance(a, str) and a})
    if not addrs:
        return pd.DataFrame(columns=["token_address","name","symbol"])
    CHUNK = 150
    parts: List[pd.DataFrame] = []
    for batch in _chunk(addrs, CHUNK):
        where = {"token_address": "in.(" + ",".join(batch) + ")"}
        parts.append(fetch_table("tokens", select="token_address,name,symbol", where=where, limit=len(batch)))
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["token_address","name","symbol"])
    return out.drop_duplicates("token_address", keep="last")

# Pair mapping from base_token
def latest_pair_map_for_tokens(token_addrs: Iterable[str]) -> Dict[str, str]:
    tlist = sorted({a for a in token_addrs if isinstance(a, str) and a})
    if not tlist:
        return {}
    CHUNK = 120
    maps: Dict[str, str] = {}
    for batch in _chunk(tlist, CHUNK):
        where = {"base_token": "in.(" + ",".join(batch) + ")"}
        cols = "pair_address,base_token,snapshot_ts,pair_created_at"
        pairs = fetch_table("pairs", select=cols, where=where, order="snapshot_ts.desc.nullslast", limit=len(batch)*6)
        if pairs.empty:
            continue
        for c in ["snapshot_ts","pair_created_at"]:
            pairs[c] = to_dt(pairs[c])
        pairs = pairs.sort_values(["base_token","snapshot_ts","pair_created_at"], ascending=[True, False, False])
        latest = pairs.drop_duplicates("base_token", keep="first")
        for _, row in latest.iterrows():
            b = row.get("base_token")
            p = row.get("pair_address")
            if isinstance(b, str) and isinstance(p, str) and b and p:
                maps[b] = p
    return maps

# Backfill token from pairs when only pair is present
def base_token_map_for_pairs(pair_addrs: Iterable[str]) -> Dict[str, str]:
    plist = sorted({p for p in pair_addrs if isinstance(p, str) and p})
    if not plist:
        return {}
    CHUNK = 150
    maps: Dict[str, str] = {}
    for batch in _chunk(plist, CHUNK):
        where = {"pair_address": "in.(" + ",".join(batch) + ")"}
        cols = "pair_address,base_token"
        pairs = fetch_table("pairs", select=cols, where=where, limit=len(batch))
        if pairs.empty:
            continue
        for _, row in pairs.iterrows():
            pa = row.get("pair_address")
            bt = row.get("base_token")
            if isinstance(pa, str) and isinstance(bt, str) and pa and bt:
                maps[pa] = bt
    return maps

# Normalize token column name from a variety of possibilities
def normalize_token_col(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    out = df.copy()
    cols = {c.lower(): c for c in out.columns}
    candidates = ["token_address","base_token","token","token_addr","mint","mint_address"]
    found_src = None
    for cand in candidates:
        if cand in cols:
            found_src = cols[cand]
            break
    if found_src and found_src != "token_address":
        out = out.rename(columns={found_src: "token_address"})
    if "token_address" not in out.columns:
        out["token_address"] = ""
    return out, "token_address"

# Ensure pair links exist, by mapping from token_address when needed
def ensure_pair_links(df: pd.DataFrame, token_col: str = "token_address") -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "pair_address" not in out.columns or out["pair_address"].fillna("").eq("").all():
        tokens = [t for t in out.get(token_col, pd.Series(dtype=str)).astype(str).tolist() if t]
        pmap = latest_pair_map_for_tokens(tokens)
        out["pair_address"] = out.get("pair_address", pd.Series([""]*len(out)))
        out.loc[:, "pair_address"] = out[token_col].map(lambda t: pmap.get(str(t), ""))
    return out

# If token is missing but pair exists, backfill token from pairs.base_token
def ensure_token_from_pairs(df: pd.DataFrame, token_col: str = "token_address") -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    need = (out.get(token_col, pd.Series(dtype=str)).fillna("") == "")
    if "pair_address" in out.columns and need.any():
        plist = out.loc[need, "pair_address"].dropna().astype(str).tolist()
        if plist:
            bmap = base_token_map_for_pairs(plist)
            out.loc[need, token_col] = out.loc[need, "pair_address"].map(lambda p: bmap.get(str(p), ""))
    return out

# Join token names and symbols
def attach_token_names(df: pd.DataFrame, token_col: str = "token_address") -> pd.DataFrame:
    out = df.copy()
    if out.empty or token_col not in out.columns:
        for c in ["name","symbol"]:
            if c not in out.columns:
                out[c] = ""
        return out
    toks = out[token_col].dropna().astype(str).unique().tolist()
    meta = fetch_tokens_for_addresses(toks)
    if meta.empty:
        for c in ["name","symbol"]:
            if c not in out.columns:
                out[c] = ""
        return out
    out = out.drop(columns=[c for c in ["name","symbol"] if c in out.columns])
    out = out.merge(meta.rename(columns={"token_address": token_col}), on=token_col, how="left")
    out[["name","symbol"]] = out[["name","symbol"]].fillna("")
    return out

# Link builder prefers pair link first, then token link
def add_links(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "pair_address" in out.columns:
        out["dexscreener"] = out["pair_address"].apply(
            lambda p: f"https://dexscreener.com/solana/{p}" if isinstance(p, str) and p else ""
        )
    else:
        out["dexscreener"] = ""
    if "token_address" in out.columns:
        out.loc[out["dexscreener"] == "", "dexscreener"] = out.loc[out["dexscreener"] == "", "token_address"].apply(
            lambda t: f"https://dexscreener.com/solana/{t}" if isinstance(t, str) and t else ""
        )
        out["solscan"] = out["token_address"].apply(
            lambda m: f"https://solscan.io/token/{m}" if isinstance(m, str) and m else ""
        )
        out["birdeye"] = out["token_address"].apply(
            lambda m: f"https://birdeye.so/token/{m}?chain=solana" if isinstance(m, str) and m else ""
        )
    else:
        out["solscan"] = ""
        out["birdeye"] = ""
    return out

def link_config(cols: List[str]):
    cfg_map = {}
    colmod = getattr(st, "column_config", None)
    if not colmod or not hasattr(colmod, "LinkColumn"):
        return cfg_map
    if "dexscreener" in cols:
        cfg_map["dexscreener"] = colmod.LinkColumn("Dexscreener", display_text="Open")
    if "solscan" in cols:
        cfg_map["solscan"] = colmod.LinkColumn("Solscan", display_text="Scan")
    if "birdeye" in cols:
        cfg_map["birdeye"] = colmod.LinkColumn("Birdeye", display_text="Bird")
    return cfg_map

# ============================= Data Fetching Functions =============================
def fetch_recent_pairs(max_pairs: int, recency_hours: int, max_age_minutes: int, *, use_snapshot_fallback: bool = True) -> pd.DataFrame:
    since_iso = iso_hours_ago(recency_hours)
    pairs = fetch_table(
        "pairs",
        select=("pair_address,base_token,quote_token,price_usd,fdv_usd,market_cap_usd,"
                "pair_created_at,snapshot_ts,base_token_name,base_token_symbol,quote_token_name,quote_token_symbol"),
        where={"snapshot_ts": f"gte.{since_iso}"},
        order="snapshot_ts.desc.nullslast",
        limit=max_pairs,
    )
    if pairs.empty:
        return pairs
    pairs = numeric(pairs, ["price_usd","fdv_usd","market_cap_usd"])
    for tcol in ["pair_created_at","snapshot_ts"]:
        if tcol in pairs.columns:
            pairs[tcol] = to_dt(pairs[tcol])
    eff = pairs["pair_created_at"].copy()
    if use_snapshot_fallback:
        eff = eff.where(eff.notna(), pairs["snapshot_ts"])
    min_ts = now_utc() - pd.Timedelta(minutes=max_age_minutes)
    pairs = pairs.loc[eff >= min_ts].copy()
    pairs["effective_created_at"] = eff
    pairs = pairs.rename(columns={"base_token": "token_address"})
    return pairs

def fetch_swaps_for_pairs(pair_addrs: List[str], since_iso: str, limit_per_batch: int = 10000) -> pd.DataFrame:
    if not pair_addrs:
        return pd.DataFrame(columns=["pair_address","ts","trader_wallet","side","amount_in","amount_out","amount_usd","price_usd"])
    CHUNK = 120
    parts: List[pd.DataFrame] = []
    for batch in _chunk(pair_addrs, CHUNK):
        where = {"pair_address": "in.(" + ",".join(batch) + ")", "ts": f"gte.{since_iso}"}
        cols = "pair_address,ts,trader_wallet,side,amount_in,amount_out,amount_usd,price_usd"
        sw = fetch_table("swaps", select=cols, where=where, order="ts.asc.nullslast", limit=limit_per_batch)
        if not sw.empty:
            sw["ts"] = to_dt(sw["ts"])
            parts.append(sw)
    if parts:
        out = pd.concat(parts, ignore_index=True)
        return numeric(out, ["amount_in","amount_out","amount_usd","price_usd"])
    return pd.DataFrame(columns=["pair_address","ts","trader_wallet","side","amount_in","amount_out","amount_usd","price_usd"])

def fetch_pwm_for_pairs(pair_addrs: List[str], since_iso: str) -> pd.DataFrame:
    if not pair_addrs:
        return pd.DataFrame(columns=["pair_address","window_code","price_change_pct","buys","sells","volume_usd","snapshot_ts"])
    CHUNK = 200
    parts: List[pd.DataFrame] = []
    for batch in _chunk(pair_addrs, CHUNK):
        where = {"pair_address": "in.(" + ",".join(batch) + ")", "snapshot_ts": f"gte.{since_iso}", "window_code": "eq.m5"}
        cols = "pair_address,window_code,price_change_pct,buys,sells,volume_usd,snapshot_ts"
        pm = fetch_table("pair_window_metrics", select=cols, where=where, order="snapshot_ts.asc.nullslast", limit=5000)
        if not pm.empty:
            pm["snapshot_ts"] = to_dt(pm["snapshot_ts"])
            parts.append(pm)
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["pair_address","window_code","price_change_pct","buys","sells","volume_usd","snapshot_ts"])
    return numeric(out, ["price_change_pct","buys","sells","volume_usd"])

def fetch_lp_events_for_pairs(pair_addrs: List[str], since_iso: str) -> pd.DataFrame:
    if not pair_addrs:
        return pd.DataFrame(columns=["pair_address","ts","action","value_usd"])
    CHUNK = 200
    parts: List[pd.DataFrame] = []
    for batch in _chunk(pair_addrs, CHUNK):
        where = {"pair_address": "in.(" + ",".join(batch) + ")", "ts": f"gte.{since_iso}"}
        cols = "pair_address,ts,action,value_usd"
        le = fetch_table("liquidity_events", select=cols, where=where, order="ts.asc.nullslast", limit=20000)
        if not le.empty:
            le["ts"] = to_dt(le["ts"])
            parts.append(le)
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["pair_address","ts","action","value_usd"])
    return numeric(out, ["value_usd"])

# ============================= Early Leader Metrics =============================
def _first_trade_ts(sw: pd.DataFrame) -> Optional[pd.Timestamp]:
    if sw.empty:
        return None
    ts = sw["ts"].dropna()
    return ts.min() if not ts.empty else None

def _buy_mask(sw: pd.DataFrame) -> pd.Series:
    if "side" in sw.columns and not sw["side"].isna().all():
        return sw["side"].astype(str).str.lower().eq("buy")
    return pd.Series([i % 2 == 0 for i in range(len(sw))], index=sw.index)

def _amount_for_concentration(sw_buys: pd.DataFrame) -> pd.Series:
    if "amount_usd" in sw_buys.columns and not sw_buys["amount_usd"].isna().all():
        return pd.to_numeric(sw_buys["amount_usd"], errors="coerce").fillna(0.0)
    if "amount_in" in sw_buys.columns and not sw_buys["amount_in"].isna().all():
        return pd.to_numeric(sw_buys["amount_in"], errors="coerce").fillna(0.0)
    if "amount_out" in sw_buys.columns and not sw_buys["amount_out"].isna().all():
        return pd.to_numeric(sw_buys["amount_out"], errors="coerce").fillna(0.0)
    return pd.Series(1.0, index=sw_buys.index)

def compute_early_metrics(
    pairs: pd.DataFrame,
    swaps: pd.DataFrame,
    pwm: pd.DataFrame,
    lpev: pd.DataFrame,
    *,
    burst_window_s: int = 120,
    uniques_window_m: int = 10,
    pwm_window_total_m: int = 15,
) -> pd.DataFrame:
    if pairs.empty:
        return pairs.copy()
    out = pairs.copy()
    for c in ["time_to_first_trade_s","swaps_in_burst","swaps_per_min_burst","uniq_traders_10m","buy_ratio_15m","top5_concentration","first_trade_ts","lp_add_usd_15","lp_remove_usd_15"]:
        out[c] = np.nan

    sw_by_pair: Dict[str, pd.DataFrame] = {}
    if not swaps.empty and "pair_address" in swaps.columns:
        for p, g in swaps.groupby("pair_address"):
            sw_by_pair[str(p)] = g.copy()

    pwm_by_pair: Dict[str, pd.DataFrame] = {}
    if not pwm.empty and "pair_address" in pwm.columns:
        for p, g in pwm.groupby("pair_address"):
            pwm_by_pair[str(p)] = g.copy()

    le_by_pair: Dict[str, pd.DataFrame] = {}
    if not lpev.empty and "pair_address" in lpev.columns:
        for p, g in lpev.groupby("pair_address"):
            le_by_pair[str(p)] = g.copy()

    for i, row in out.iterrows():
        p = str(row.get("pair_address",""))
        created = row.get("effective_created_at")
        if not p or pd.isna(created):
            continue
        sw = sw_by_pair.get(p, pd.DataFrame())
        if not sw.empty and "ts" in sw.columns:
            sw = sw[sw["ts"].fillna(pd.Timestamp.max.tz_localize("UTC")) >= created]

        first_ts = _first_trade_ts(sw)
        if pd.notna(first_ts):
            try:
                out.at[i, "time_to_first_trade_s"] = float(max(0.0, (first_ts - created).total_seconds()))
            except Exception:
                out.at[i, "time_to_first_trade_s"] = np.inf
        else:
            out.at[i, "time_to_first_trade_s"] = np.inf

        if pd.notna(first_ts):
            try:
                cast_ts = pd.to_datetime(first_ts)
                if cast_ts.tzinfo is not None:
                    cast_ts = cast_ts.tz_convert(None)
            except Exception:
                cast_ts = pd.NaT
        else:
            cast_ts = pd.NaT
        out.at[i, "first_trade_ts"] = cast_ts

        if pd.notna(first_ts):
            burst_end = first_ts + pd.Timedelta(seconds=burst_window_s)
            burst = sw[(sw["ts"] >= first_ts) & (sw["ts"] <= burst_end)]
            swaps_in_burst = float(len(burst))
            swaps_per_min = swaps_in_burst / max(burst_window_s / 60.0, 1e-9)
            out.at[i, "swaps_in_burst"] = swaps_in_burst
            out.at[i, "swaps_per_min_burst"] = float(swaps_per_min)

            uniq_end = first_ts + pd.Timedelta(minutes=uniques_window_m)
            w = sw[(sw["ts"] >= first_ts) & (sw["ts"] <= uniq_end)] if not sw.empty else sw
            uniq_cnt = float(w["trader_wallet"].dropna().astype(str).nunique()) if ("trader_wallet" in w.columns and not w.empty) else 0.0
            out.at[i, "uniq_traders_10m"] = uniq_cnt

            sw_15 = sw[(sw["ts"] >= first_ts) & (sw["ts"] <= first_ts + pd.Timedelta(minutes=pwm_window_total_m))]
            if not sw_15.empty and "side" in sw_15.columns:
                is_buy = _buy_mask(sw_15)
                buys = float(is_buy.sum())
                sells = float((~is_buy).sum())
                out.at[i, "buy_ratio_15m"] = buys / max(buys + sells, 1.0)
            else:
                pwm_df = pwm_by_pair.get(p, pd.DataFrame())
                if not pwm_df.empty:
                    pwm_early = pwm_df[(pwm_df["snapshot_ts"] >= created) & (pwm_df["snapshot_ts"] <= created + pd.Timedelta(minutes=pwm_window_total_m))]
                    if not pwm_early.empty and {"buys","sells"}.issubset(pwm_early.columns):
                        buys = float(pd.to_numeric(pwm_early["buys"], errors="coerce").fillna(0).sum())
                        sells = float(pd.to_numeric(pwm_early["sells"], errors="coerce").fillna(0).sum())
                        out.at[i, "buy_ratio_15m"] = buys / max(buys + sells, 1.0)

            conc = np.nan
            if not sw_15.empty and "trader_wallet" in sw_15.columns:
                buys_df = sw_15[_buy_mask(sw_15)].copy()
                if not buys_df.empty:
                    amt = _amount_for_concentration(buys_df)
                    g = buys_df.assign(_amt=amt).groupby("trader_wallet")["_amt"].sum().sort_values(ascending=False)
                    total = float(g.sum())
                    top5 = float(g.head(5).sum())
                    conc = (top5 / total) if total > 0 else np.nan
            out.at[i, "top5_concentration"] = conc

        le_df = le_by_pair.get(p, pd.DataFrame())
        if not le_df.empty and pd.notna(created):
            span_end = created + pd.Timedelta(minutes=15)
            win = le_df[(le_df["ts"] >= created) & (le_df["ts"] <= span_end)]
            if not win.empty and {"action","value_usd"}.issubset(win.columns):
                add_usd = pd.to_numeric(win.loc[win["action"].astype(str).str.lower().eq("add"), "value_usd"], errors="coerce").fillna(0).sum()
                rem_usd = pd.to_numeric(win.loc[win["action"].astype(str).str.lower().eq("remove"), "value_usd"], errors="coerce").fillna(0).sum()
                out.at[i, "lp_add_usd_15"] = float(add_usd)
                out.at[i, "lp_remove_usd_15"] = float(rem_usd)

    return out

def score_and_classify(
    df: pd.DataFrame,
    *,
    ttf_ceil_s: int = 600,
    min_swaps_per_min: float = 20,
    min_uniques_10m: int = 50,
    buy_ratio_center: float = 0.55,
    buy_ratio_tol: float = 0.25,
    max_concentration: float = 0.70,
    leader_score_min: float = 60.0,
) -> pd.DataFrame:
    if df.empty: return df.copy()
    out = df.copy()
    vel   = out["swaps_per_min_burst"].astype(float).fillna(0.0)
    uniq  = out["uniq_traders_10m"].astype(float).fillna(0.0)
    br    = out["buy_ratio_15m"].astype(float)
    conc  = out["top5_concentration"].astype(float)
    lprem = out["lp_remove_usd_15"].astype(float).fillna(0.0)

    s_vel  = (vel / max(min_swaps_per_min, 1e-9)).clip(0, 1)
    s_uniq = (uniq / max(min_uniques_10m, 1e-9)).clip(0, 1)
    s_br   = 1.0 - (br.sub(buy_ratio_center).abs() / max(buy_ratio_tol, 1e-9))
    s_br   = s_br.clip(0, 1).fillna(0.5)
    s_conc = 1.0 - ((conc - 0.50) / 0.50)
    s_conc = s_conc.clip(0, 1).fillna(0.5)
    lp_pen = (lprem > 0.0).astype(float)

    gate_tradeable = out["time_to_first_trade_s"].astype(float).fillna(np.inf) <= float(ttf_ceil_s)
    gate_velocity  = vel  >= float(min_swaps_per_min)
    gate_uniques   = uniq >= float(min_uniques_10m)
    gate_br        = br.between(buy_ratio_center - buy_ratio_tol, buy_ratio_center + buy_ratio_tol, inclusive="both").fillna(True)
    gate_conc      = conc.fillna(0.50) <= float(max_concentration)
    gate_lp_ok     = ~(lprem > 0.0)

    score_01 = 0.35*s_vel + 0.35*s_uniq + 0.10*s_br + 0.20*s_conc
    score_01 = score_01 * (1.0 - 0.9 * lp_pen)
    out["early_score"] = (score_01.clip(0, 1) * 100.0).round(1)

    labels, reasons = [], []
    early_scores = out["early_score"].astype(float).fillna(0.0)
    for trd, vel_ok, unq_ok, br_ok, conc_ok, lp_ok, sc in zip(
        gate_tradeable, gate_velocity, gate_uniques, gate_br, gate_conc, gate_lp_ok, early_scores
    ):
        if (vel_ok and unq_ok and br_ok and conc_ok and sc >= leader_score_min):
            labels.append("Early Leader")
            reasons.append("Velocity+Uniques+Balance+Dispersion")
            continue
        if sc >= 35.0 or (vel_ok and (unq_ok or br_ok)):
            labels.append("Hype / Risky")
            missing = []
            if not conc_ok:
                missing.append("Concentration")
            if not unq_ok:
                missing.append("Uniques")
            if not br_ok:
                missing.append("BuyRatio")
            reasons.append(" & ".join(missing) if missing else "Borderline")
            continue
        if not trd:
            labels.append("Loser (no early trades)")
            reasons.append("No trade <=10m")
            continue
        if not lp_ok:
            labels.append("Loser (early LP remove)")
            reasons.append("LP removed <=15m")
            continue
        labels.append("Loser")
        why = []
        if not vel_ok:
            why.append("Low velocity")
        if not unq_ok:
            why.append("Low uniques")
        if not br_ok:
            why.append("Unbalanced flow")
        reasons.append("Weak" if not why else " & ".join(why))
    out["classification"] = labels
    out["reason"] = reasons
    return out

# ============================= Sidebar =============================
with st.sidebar:
    st.title("🚀 TrenchFeed")
    st.caption("Early Leader Intelligence")
    
    # Preview info
    st.markdown("---")
    time_remaining_str = format_time_remaining(remaining_preview_seconds)
    st.markdown(f"**⏱️ Preview:** {time_remaining_str} remaining")
    st.markdown(f"[Subscribe for Full Access →]({STRIPE_PURCHASE_URL})")
    # Add a login link for returning users beneath the subscription CTA.
    # This points to the main paid dashboard login page.
    st.markdown("[Login →](https://sold-1.onrender.com)")
    st.markdown("---")
    
    # Encapsulate all controls in an expander for a cleaner look
    with st.expander("🛠️ Controls & Settings", expanded=True):
        # Manual refresh button
        if st.button("🔄 Manual Refresh"):
            st.rerun()
        
        # Settings for scanning tokens and windows
        max_pairs = st.slider("Max pairs to scan", 200, 10000, 2000, 100)
        recency_hours = st.slider("Only tokens newer than (hours)", 1, 72, 2, 1)
        max_age_minutes = st.slider("Max token age for candidates (minutes)", 1, 240, 30, 1)
        use_snapshot_fallback = st.toggle("Use snapshot_ts when pair_created_at is NULL", value=True)
        
        st.markdown("---")
        st.markdown("**Early Leader thresholds**")
        min_swaps_per_min = st.slider("Min swaps/min in 2m burst", 1, 80, 20, 1)
        min_uniques_10m = st.slider("Min unique traders in first 10m", 5, 200, 50, 5)
        buy_center = st.slider("Buy ratio center", 0.40, 0.70, 0.55, 0.01)
        buy_tol = st.slider("Buy ratio tolerance (±)", 0.05, 0.35, 0.25, 0.01)
        max_conc = st.slider("Max Top-5 concentration", 0.50, 0.95, 0.70, 0.01)
        leader_score_min = st.slider("Min Early Leader score", 0, 100, 60, 1)
        
        st.markdown("---")
        radar_window_m = st.slider("Launch Radar: lookback minutes", 30, 240, 120, 15)
        radar_max = st.slider("Launch Radar: max rows", 20, 400, 120, 20)
        # New slider controlling how many pairs are scanned for the Launch Radar.
        # Scanning fewer pairs significantly reduces load time while still
        # surfacing the most interesting launches.
        radar_candidates = st.slider(
            "Launch Radar: pairs to scan",
            min_value=300,
            max_value=500,
            value=400,
            step=50,
        )
        # Removed Token Detail slider as Token Detail tab has been removed.

# ============================= Tabs =============================
tab_all, tab_leaders, tab_top, tab_radar = st.tabs([
    "📋 All Candidates",
    "🏆 Early Leaders",
    "🔴 LIVE",
    "🚀 Launch Radar"
])

# ============================= Early Leaders =============================
with tab_leaders:
    st.subheader("Early Leaders")
    display_disclaimer()
    pairs = fetch_recent_pairs(max_pairs, recency_hours, max_age_minutes, use_snapshot_fallback=use_snapshot_fallback)
    if pairs.empty:
        st.info("No recent pairs in the selected window.")
    else:
        earliest_eff = pairs["effective_created_at"].min()
        since_iso = iso(earliest_eff - pd.Timedelta(minutes=1)) if pd.notna(earliest_eff) else iso_hours_ago(recency_hours)

        pair_ids = pairs["pair_address"].dropna().astype(str).unique().tolist()
        swaps = fetch_swaps_for_pairs(pair_ids, since_iso)
        pwm   = fetch_pwm_for_pairs(pair_ids, since_iso)
        lpev  = fetch_lp_events_for_pairs(pair_ids, since_iso)

        metrics = compute_early_metrics(pairs, swaps, pwm, lpev)
        ranked  = score_and_classify(metrics,
                                     ttf_ceil_s=600,
                                     min_swaps_per_min=float(min_swaps_per_min),
                                     min_uniques_10m=int(min_uniques_10m),
                                     buy_ratio_center=float(buy_center),
                                     buy_ratio_tol=float(buy_tol),
                                     max_concentration=float(max_conc),
                                     leader_score_min=float(leader_score_min))

        ranked = ensure_pair_links(ranked, token_col="token_address")
        ranked = attach_token_names(ranked, token_col="token_address")
        ranked = add_links(ranked)
        if not ranked.empty and "early_score" in ranked.columns:
            ranked = ranked.sort_values(["early_score"], ascending=[False]).reset_index(drop=True)

        cols = [
            "early_score","classification","reason",
            "swaps_per_min_burst","uniq_traders_10m","buy_ratio_15m","top5_concentration",
            "token_address","name","symbol",
            "dexscreener","solscan","birdeye",
            "pair_address","effective_created_at","first_trade_ts","time_to_first_trade_s",
        ]
        shown = [c for c in cols if c in ranked.columns]
        if not ranked.empty:
            leader_count = int((ranked.get("classification") == "Early Leader").sum()) if "classification" in ranked.columns else 0
            hype_count   = int((ranked.get("classification") == "Hype / Risky").sum()) if "classification" in ranked.columns else 0
            loser_count  = int(ranked.get("classification").astype(str).str.contains("Loser").sum()) if "classification" in ranked.columns else 0
            avg_score    = float(ranked.get("early_score").mean()) if "early_score" in ranked.columns else float('nan')
            max_score    = float(ranked.get("early_score").max()) if "early_score" in ranked.columns else float('nan')
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Leaders", f"{leader_count}")
            c2.metric("Hype", f"{hype_count}")
            c3.metric("Losers", f"{loser_count}")
            c4.metric("Avg Score", f"{avg_score:.1f}" if not pd.isna(avg_score) else "N/A")
            c5.metric("Top Score", f"{max_score:.1f}" if not pd.isna(max_score) else "N/A")
        st.dataframe(ranked[shown].reset_index(drop=True), use_container_width=True, height=520, column_config=link_config(shown))

# ============================= All Candidates =============================
with tab_all:
    st.subheader("All Candidates")
    display_disclaimer()
    since_iso = iso_hours_ago(recency_hours)
    pairs = fetch_table(
        "pairs",
        select=("pair_address,base_token,quote_token,price_usd,fdv_usd,market_cap_usd,"
                "pair_created_at,snapshot_ts,base_token_name,base_token_symbol,quote_token_name,quote_token_symbol"),
        where={"snapshot_ts": f"gte.{since_iso}"},
        order="snapshot_ts.desc.nullslast",
        limit=max_pairs,
    )
    if pairs.empty:
        st.info("No recent pairs.")
    else:
        pairs = numeric(pairs, ["price_usd","fdv_usd","market_cap_usd"])
        for tcol in ["pair_created_at","snapshot_ts"]:
            pairs[tcol] = to_dt(pairs[tcol])
        eff = pairs["pair_created_at"].where(pairs["pair_created_at"].notna(), pairs["snapshot_ts"])
        min_ts = now_utc() - pd.Timedelta(minutes=max_age_minutes)
        pairs = pairs.loc[eff >= min_ts].copy()
        pairs["effective_created_at"] = eff
        pairs = pairs.rename(columns={"base_token": "token_address"})
        pairs = ensure_pair_links(pairs, token_col="token_address")
        pairs = attach_token_names(pairs, token_col="token_address")
        pairs = add_links(pairs)

        cols = [
            "price_usd","fdv_usd","market_cap_usd","effective_created_at","snapshot_ts",
            "token_address","name","symbol",
            "dexscreener","solscan","birdeye",
            "pair_address","quote_token_symbol","quote_token_name",
        ]
        shown = [c for c in cols if c in pairs.columns]
        st.dataframe(pairs[shown].reset_index(drop=True), use_container_width=True, height=620, column_config=link_config(shown))

# The Token Detail tab has been removed.

# ============================= Top Coins (LIVE) =============================
with tab_top:
    st.subheader("Live Market Data Stream")
    display_disclaimer()
    st.caption("Real‑time streaming of market events from TrenchFeed Enterprise API. \n"
               "Click the button below to re‑establish the feed at any time.")
    st.markdown(
        "<div style='margin: 0.5rem 0;'>"
        "For continuous, high‑fidelity data streams, head over to "
        "<a href='https://trenchfeed.cc' target='_blank' style='font-weight:bold; color:#FFB900;'>"
        "trenchfeed.cc</a> and grab a subscription.</div>",
        unsafe_allow_html=True,
    )
    
    # Initialize stream control state
    if "stream_paused" not in st.session_state:
        st.session_state.stream_paused = False
    if "stream_data" not in st.session_state:
        st.session_state.stream_data = []
    if "stream_initialized" not in st.session_state:
        st.session_state.stream_initialized = False
    
    # Create button row with Refresh and Pause/Resume side by side
    btn_col1, btn_col2, status_col = st.columns([1, 1, 2])
    
    with btn_col1:
        refresh_feed = st.button("🔄 Refresh Feed", key="refresh_feed", use_container_width=True)
    
    with btn_col2:
        is_paused = st.session_state.stream_paused
        pause_label = "▶️ Resume" if is_paused else "⏸️ Pause"
        if st.button(pause_label, key="pause_resume", use_container_width=True):
            st.session_state.stream_paused = not st.session_state.stream_paused
            st.rerun()
    
    with status_col:
        if st.session_state.stream_paused:
            st.markdown(
                '<div class="stream-status paused">'
                '<span class="pulse"></span>'
                'PAUSED'
                '</div>',
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                '<div class="stream-status live">'
                '<span class="pulse"></span>'
                'STREAMING'
                '</div>',
                unsafe_allow_html=True
            )
    
    if refresh_feed:
        st.session_state.stream_data = []
        st.session_state.stream_initialized = False
        st.session_state.stream_paused = False
        st.rerun()
    
    live_placeholder = st.empty()
    
    _live_rename_map = {
        "payload.token.name": "Token Name",
        "payload.token.symbol": "Ticker",
        "payload.pair.base_token": "Base Token",
        "payload.pair.dex": "DEX",
        "payload.pair.price_usd": "Price USD",
        "payload.current_swap.side": "Side",
        "payload.current_swap.price_usd": "Swap Price USD",
        "payload.current_swap.usd_value": "Swap Value USD",
        "payload.current_swap.sol_amount": "SOL Amount",
        "payload.current_swap.token_amount": "Token Amount",
        "payload.current_swap.trader_wallet": "Trader Wallet",
        "payload.timestamp": "Event Timestamp",
    }
    
    def render_stream_table():
        if not st.session_state.stream_data:
            live_placeholder.info("No data yet. Waiting for stream events...")
            return
        
        try:
            df_stream = pd.json_normalize(st.session_state.stream_data)
        except Exception:
            df_stream = pd.DataFrame(st.session_state.stream_data)
        
        if "payload.recent_swaps" in df_stream.columns:
            df_stream = df_stream.drop(columns=["payload.recent_swaps"])
        
        rename_map = {k: v for k, v in _live_rename_map.items() if k in df_stream.columns}
        df_stream = df_stream.rename(columns=rename_map)
        
        primary_raw = ["payload.token.name", "payload.token.symbol", "payload.pair.base_token"]
        primary_cols = [rename_map.get(col, _live_rename_map.get(col, col)) for col in primary_raw if (rename_map.get(col, _live_rename_map.get(col, col)) in df_stream.columns)]
        
        other_cols = [c for c in df_stream.columns if c not in primary_cols]
        ordered_cols = primary_cols + other_cols
        
        df_show = df_stream[ordered_cols].tail(50).iloc[::-1].copy()
        
        n_rows = len(df_show)
        df_show.index = range(n_rows, 0, -1)
        
        live_placeholder.dataframe(
            df_show,
            use_container_width=True,
            height=620,
        )
    
    def run_live_stream():
        for event in stream_enterprise(max_events=200):
            if st.session_state.stream_paused:
                break
            st.session_state.stream_data.append(event)
            render_stream_table()
    
    if st.session_state.stream_paused:
        render_stream_table()
    elif not st.session_state.get("stream_initialized"):
        st.session_state.stream_initialized = True
        run_live_stream()
    else:
        run_live_stream()

# ============================= Launch Radar =============================
with tab_radar:
    st.subheader("Launch Radar")
    display_disclaimer()
    # Progress bar to indicate Launch Radar status
    progress_bar = st.progress(0.0)
    lookback_iso = iso(now_utc() - pd.Timedelta(minutes=radar_window_m))
    recent_pairs = fetch_table(
        "pairs",
        select="pair_address,token_address:base_token,base_token_name,base_token_symbol,pair_created_at,snapshot_ts,price_usd,fdv_usd,market_cap_usd",
        where={"pair_created_at": f"gte.{lookback_iso}"},
        order="pair_created_at.desc.nullslast",
        limit=int(radar_candidates)
    )
    progress_bar.progress(0.1)
    if recent_pairs.empty or recent_pairs["pair_created_at"].isna().all():
        extra = fetch_table(
            "pairs",
            select="pair_address,token_address:base_token,base_token_name,base_token_symbol,pair_created_at,snapshot_ts,price_usd,fdv_usd,market_cap_usd",
            where={"snapshot_ts": f"gte.{lookback_iso}"},
            order="snapshot_ts.desc.nullslast",
            limit=int(radar_candidates)
        )
        # advance progress after fallback query
        progress_bar.progress(0.2)
        if not extra.empty:
            recent_pairs = extra

    if recent_pairs.empty:
        st.info("No very recent launches.")
        progress_bar.progress(1.0)
    else:
        recent_pairs["pair_created_at"] = to_dt(recent_pairs["pair_created_at"])
        recent_pairs["snapshot_ts"]     = to_dt(recent_pairs["snapshot_ts"])
        eff = recent_pairs["pair_created_at"].where(recent_pairs["pair_created_at"].notna(), recent_pairs["snapshot_ts"])
        recent_pairs["effective_created_at"] = eff
        progress_bar.progress(0.3)

        pair_ids = recent_pairs["pair_address"].dropna().astype(str).unique().tolist()
        swaps = fetch_swaps_for_pairs(pair_ids, lookback_iso)
        progress_bar.progress(0.5)
        pwm   = fetch_pwm_for_pairs(pair_ids, lookback_iso)
        progress_bar.progress(0.6)
        lpev  = fetch_lp_events_for_pairs(pair_ids, lookback_iso)
        progress_bar.progress(0.8)

        metrics = compute_early_metrics(recent_pairs, swaps, pwm, lpev)
        ranked  = score_and_classify(
            metrics,
            ttf_ceil_s=600,
            min_swaps_per_min=float(min_swaps_per_min),
            min_uniques_10m=int(min_uniques_10m),
            buy_ratio_center=float(buy_center),
            buy_ratio_tol=float(buy_tol),
            max_concentration=float(max_conc),
            leader_score_min=float(leader_score_min),
        )

        ranked = ensure_pair_links(ranked, token_col="token_address")
        ranked = attach_token_names(ranked, token_col="token_address")
        ranked = add_links(ranked)

        if "market_cap_usd" in ranked.columns:
            ranked = numeric(ranked, ["market_cap_usd"])
            ranked = ranked[ranked["market_cap_usd"].fillna(0) >= 30000]

        cols = [
            "effective_created_at",
            "token_address","name","symbol",
            "dexscreener","solscan","birdeye",
            "early_score","classification","reason",
            "swaps_per_min_burst","uniq_traders_10m","buy_ratio_15m","top5_concentration",
            "pair_address",
        ]
        shown = [c for c in cols if c in ranked.columns]
        ranked = ranked.sort_values(["early_score"], ascending=[False]).head(radar_max).reset_index(drop=True)
        progress_bar.progress(1.0)
        total_pairs    = len(ranked)
        lr_leaders     = int((ranked.get("classification") == "Early Leader").sum()) if "classification" in ranked.columns else 0
        lr_hype        = int((ranked.get("classification") == "Hype / Risky").sum()) if "classification" in ranked.columns else 0
        lr_avg_score   = float(ranked.get("early_score").mean()) if "early_score" in ranked.columns else float('nan')
        col_a, col_b, col_c, col_d = st.columns(4)
        col_a.metric("New Pairs", f"{total_pairs}")
        col_b.metric("Leaders", f"{lr_leaders}")
        col_c.metric("Hype", f"{lr_hype}")
        col_d.metric("Avg Score", f"{lr_avg_score:.1f}" if not pd.isna(lr_avg_score) else "N/A")
        st.dataframe(ranked[shown], use_container_width=True, height=640, column_config=link_config(shown))
