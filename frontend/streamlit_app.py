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

# Inject custom CSS for a modern, beautiful dashboard.  This styling enhances the
# overall look and feel without altering the underlying functionality.  The
# gradient background, dark sidebar, styled headings, and improved table colors
# provide a cohesive visual experience.
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
    """
    Get or create a stable anonymous ID for the current visitor.
    Uses st.session_state as the primary storage mechanism.
    The ID is a UUID that persists across page refreshes within the same browser session.
    """
    # Check if we already have an anon_id in session state
    if "preview_anon_id" in st.session_state and st.session_state.preview_anon_id:
        return st.session_state.preview_anon_id
    
    # Try to get from query params (for cookie-like persistence via URL)
    query_params = st.query_params
    if "aid" in query_params:
        anon_id = query_params["aid"]
        st.session_state.preview_anon_id = anon_id
        return anon_id
    
    # Generate a new anonymous ID
    anon_id = f"anon_{uuid.uuid4().hex[:16]}"
    st.session_state.preview_anon_id = anon_id
    
    # Store in query params for persistence across page loads
    # This acts as a pseudo-cookie mechanism
    st.query_params["aid"] = anon_id
    
    return anon_id


def get_preview_session(anon_id: str) -> Optional[Dict]:
    """
    Fetch the preview session from Supabase for the given anonymous ID.
    Returns None if no session exists.
    """
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
    except Exception as e:
        # Log error but don't block the user
        return None


def create_preview_session(anon_id: str) -> Optional[Dict]:
    """
    Create a new preview session in Supabase for the given anonymous ID.
    Sets started_at to the current time (immutable after creation).
    """
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
            # Return the payload if no data returned
            return payload
        return None
    except Exception as e:
        # If we can't create a session, return a fallback that allows access
        # This ensures network issues don't block legitimate users
        return {
            "anon_id": anon_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "last_seen": datetime.now(timezone.utc).isoformat()
        }


def update_last_seen(anon_id: str) -> None:
    """
    Opportunistically update the last_seen timestamp for analytics.
    Failures are silently ignored to avoid blocking the user.
    """
    try:
        url = f"{SB_URL}/rest/v1/trial_sessions"
        params = {"anon_id": f"eq.{anon_id}"}
        payload = {"last_seen": datetime.now(timezone.utc).isoformat()}
        SESSION.patch(url, params=params, json=payload, timeout=5)
    except Exception:
        pass  # Silent failure - this is non-critical


def calculate_remaining_time(started_at_str: str) -> int:
    """
    Calculate remaining preview time in seconds.
    Returns 0 if time has expired.
    """
    try:
        # Parse the started_at timestamp
        started_at = pd.to_datetime(started_at_str, utc=True)
        now = pd.Timestamp.now(tz="UTC")
        
        elapsed_seconds = (now - started_at).total_seconds()
        remaining = PREVIEW_DURATION_SECONDS - elapsed_seconds
        
        return max(0, int(remaining))
    except Exception:
        # If we can't parse, assume full time remaining to be generous
        return PREVIEW_DURATION_SECONDS


def format_time_remaining(seconds: int) -> str:
    """Format seconds as MM:SS or HH:MM:SS."""
    if seconds <= 0:
        return "00:00"
    
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def show_preview_banner(remaining_seconds: int) -> None:
    """
    Display the preview countdown banner at the top of the page.
    Changes color based on remaining time.
    """
    time_str = format_time_remaining(remaining_seconds)
    
    # Determine timer class based on remaining time
    if remaining_seconds <= 60:  # Last minute
        timer_class = "critical"
    elif remaining_seconds <= 300:  # Last 5 minutes
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
    """
    Show the expired overlay and trigger a redirect to the paid dashboard.
    """
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
        // Auto-redirect after 3 seconds
        setTimeout(function() {{
            window.location.href = "{EXPIRED_REDIRECT_URL}";
        }}, 3000);
    </script>
    """
    st.markdown(expired_html, unsafe_allow_html=True)
    st.stop()


def check_preview_session() -> int:
    """
    Main preview session check. Called on every page load.
    
    Returns:
        Remaining seconds if preview is valid, or calls st.stop() if expired.
    """
    # Get or create the anonymous ID for this visitor
    anon_id = get_or_create_anon_id()
    
    # Try to fetch existing session from Supabase
    session = get_preview_session(anon_id)
    
    if session is None:
        # New visitor - create their preview session
        session = create_preview_session(anon_id)
        if session is None:
            # Fallback: allow access with full duration if DB is unreachable
            return PREVIEW_DURATION_SECONDS
    
    # Calculate remaining time based on their started_at
    started_at = session.get("started_at")
    if not started_at:
        # If no started_at, treat as new session
        return PREVIEW_DURATION_SECONDS
    
    remaining = calculate_remaining_time(started_at)
    
    if remaining <= 0:
        # Preview expired - show redirect
        show_expired_redirect()
        # show_expired_redirect calls st.stop(), so we won't reach here
    
    # Update last_seen opportunistically (non-blocking)
    update_last_seen(anon_id)
    
    return remaining


# ============================= CHECK PREVIEW SESSION =============================
# This runs on every page load to enforce the preview time limit
remaining_preview_seconds = check_preview_session()

# Display the countdown banner
show_preview_banner(remaining_preview_seconds)

# Set up auto-refresh to update the countdown (every 30 seconds)
if st_autorefresh is not None:
    # Refresh more frequently as time runs low
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
#
# The enterprise API streams new‑line delimited JSON (NDJSON) events via
# Server‑Sent Events (SSE).  Each event is framed by a blank line and
# contains one or more lines beginning with "data: ".  The following
# helper decodes these frames and yields a parsed JSON object per event.
#
ENTERPRISE_URL: str = "https://api.trenchfeed.cc/stream/enterprise"
# NOTE: The provided token is a read‑only demonstration token suitable
# only for experimentation.  In production you should inject the token
# via environment variables or Streamlit secrets.
ENTERPRISE_TOKEN: str = "tk_live_henCCxnVFK4E1CaGlNRO7aH8PA11cxtf"

def stream_enterprise(api_key: str = ENTERPRISE_TOKEN, *, max_events: int = 200) -> Iterable[Dict]:
    """Yield parsed JSON objects from the enterprise streaming endpoint.

    Parameters
    ----------
    api_key: str
        The bearer token used for authentication.
    max_events: int
        Maximum number of events to yield before returning.  Limiting
        prevents runaway loops during interactive sessions.

    Yields
    ------
    dict
        Parsed JSON payload for each SSE `data:` event.  If a line
        cannot be decoded as JSON it is skipped.
    """
    url = ENTERPRISE_URL
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "text/event-stream"
    }
    # Open a streaming HTTP connection.  The timeout is set to 60 seconds
    # per read to allow for network delays without freezing the UI.
    try:
        with requests.get(url, headers=headers, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            buffer = ""
            event_count = 0
            for chunk in resp.iter_content(chunk_size=1024):
                # Append decoded text to our buffer
                try:
                    buffer += chunk.decode("utf-8", errors="ignore")
                except Exception:
                    continue
                # Process complete frames separated by two newlines
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
                                # Skip malformed JSON lines
                                continue
    except Exception:
        # On any failure (network or authentication), simply return
        return

# ============================= UI helpers =============================
DISCLAIMER_TEXT: str = (
    "Token names and symbols displayed here are user‑generated by token creators in the market "
    "and do not reflect the views of TrenchFeed."
    " Investments made using information displayed here are purely at your own risk. TrenchFeed does not provide financial investment advice."
)

def display_disclaimer() -> None:
    """Render a consistent disclaimer across all tabs.

    Uses HTML to draw attention to the message without clashing with
    existing dark styling.  The box uses a semi‑transparent background
    so as not to distract from the content but to remain visible.
    """
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
    # Successful responses include 200 and 206 (partial).  Return parsed JSON.
    if r.status_code in (200, 206):
        try:
            return r.json()
        except Exception:
            return []
    # Handle database timeouts gracefully: Supabase returns HTTP 500 with a cancellation code.
    if r.status_code == 500:
        # Display a user-friendly message rather than a raw exception.  Note: st may not be available in
        # all contexts, but this ensures that the UI remains clean for top_coins views.
        try:
            st.info(f"The '{table}' view timed out. Please reduce the date range or lower the row limit and try again.")
        except Exception:
            pass
        return []
    # For other errors, suppress noisy 404 messages for optional tables.  Some
    # views (e.g. 'listings' and 'risk_flags') may not exist in all schemas and
    # return a 404 with a descriptive body.  We silently return an empty list
    # for those cases so the UI remains clean.
    if r.status_code == 404:
        body_lower = (r.text or "").lower()
        if any(tok in body_lower for tok in ["public.listings", "public.risk_flags", "could not find the table"]):
            return []
    # For other errors, log a warning with truncated details
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

# ============================= Linking & enrichment =============================
TOKEN_COL_ALIASES: Tuple[str, ...] = (
    "token_address", "base_token", "token", "token_addr", "mint", "mint_address"
)

def normalize_token_column(df: pd.DataFrame) -> pd.DataFrame:
    if "token_address" in df.columns:
        return df
    for alias in TOKEN_COL_ALIASES:
        if alias in df.columns:
            df = df.rename(columns={alias: "token_address"})
            break
    return df

def backfill_token_from_pairs(df: pd.DataFrame, token_col: str = "token_address") -> pd.DataFrame:
    if token_col in df.columns and df[token_col].notna().all():
        return df
    if "pair_address" not in df.columns or df["pair_address"].isna().all():
        return df
    missing = df[token_col].isna() if token_col in df.columns else pd.Series(True, index=df.index)
    pair_ids = df.loc[missing, "pair_address"].dropna().astype(str).unique().tolist()
    if not pair_ids:
        return df
    pairs = fetch_table(
        "pairs",
        select="pair_address,base_token",
        where={"pair_address": f"in.({','.join(pair_ids)})"},
        limit=len(pair_ids),
    )
    if pairs.empty:
        return df
    lookup = dict(zip(pairs["pair_address"].astype(str), pairs["base_token"].astype(str)))
    if token_col not in df.columns:
        df[token_col] = None
    filled = df.loc[missing, "pair_address"].map(lookup)
    df.loc[missing, token_col] = filled
    return df

def ensure_pair_links(df: pd.DataFrame, token_col: str = "token_address") -> pd.DataFrame:
    df = normalize_token_column(df)
    df = backfill_token_from_pairs(df, token_col)
    return df

def attach_token_names(df: pd.DataFrame, token_col: str = "token_address") -> pd.DataFrame:
    if "name" in df.columns and "symbol" in df.columns:
        return df
    if token_col not in df.columns or df[token_col].isna().all():
        return df
    token_ids = df[token_col].dropna().astype(str).unique().tolist()
    if not token_ids:
        return df
    tokens = fetch_table(
        "tokens",
        select="token_address,name,symbol",
        where={"token_address": f"in.({','.join(token_ids)})"},
        limit=len(token_ids),
    )
    if tokens.empty:
        return df
    lookup_name = dict(zip(tokens["token_address"].astype(str), tokens["name"]))
    lookup_sym  = dict(zip(tokens["token_address"].astype(str), tokens["symbol"]))
    if "name" not in df.columns:
        df["name"] = df[token_col].map(lookup_name)
    if "symbol" not in df.columns:
        df["symbol"] = df[token_col].map(lookup_sym)
    return df

def add_links(df: pd.DataFrame) -> pd.DataFrame:
    def dex_link(row):
        if pd.notna(row.get("pair_address")):
            return f"https://dexscreener.com/solana/{row['pair_address']}"
        if pd.notna(row.get("token_address")):
            return f"https://dexscreener.com/solana/{row['token_address']}"
        return None
    def solscan_link(row):
        if pd.notna(row.get("token_address")):
            return f"https://solscan.io/token/{row['token_address']}"
        return None
    def birdeye_link(row):
        if pd.notna(row.get("token_address")):
            return f"https://birdeye.so/token/{row['token_address']}?chain=solana"
        return None

    df["dexscreener"] = df.apply(dex_link, axis=1)
    df["solscan"]     = df.apply(solscan_link, axis=1)
    df["birdeye"]     = df.apply(birdeye_link, axis=1)
    return df

def link_config(columns: List[str]) -> Dict:
    cfg: Dict = {}
    for col in ("dexscreener", "solscan", "birdeye"):
        if col in columns:
            cfg[col] = st.column_config.LinkColumn(col, display_text="🔗")
    return cfg

# ============================= Analysis helpers =============================
def fetch_swaps_for_pairs(pair_ids: List[str], since_iso: str) -> pd.DataFrame:
    if not pair_ids:
        return pd.DataFrame()
    swaps = fetch_table(
        "swaps",
        select="pair_address,trader_address,swap_ts,side,usd_value",
        where={"pair_address": f"in.({','.join(pair_ids)})", "swap_ts": f"gte.{since_iso}"},
        order="swap_ts.desc",
        limit=50000,
    )
    if not swaps.empty and "swap_ts" in swaps.columns:
        swaps["swap_ts"] = to_dt(swaps["swap_ts"])
    return swaps

def fetch_pwm_for_pairs(pair_ids: List[str], since_iso: str) -> pd.DataFrame:
    if not pair_ids:
        return pd.DataFrame()
    pwm = fetch_table(
        "pair_window_metrics",
        select="pair_address,window_start,buys,sells,unique_buyers,unique_sellers,volume_usd,trades",
        where={"pair_address": f"in.({','.join(pair_ids)})", "window_start": f"gte.{since_iso}"},
        order="window_start.desc",
        limit=50000,
    )
    if not pwm.empty and "window_start" in pwm.columns:
        pwm["window_start"] = to_dt(pwm["window_start"])
    return pwm

def fetch_lp_events_for_pairs(pair_ids: List[str], since_iso: str) -> pd.DataFrame:
    if not pair_ids:
        return pd.DataFrame()
    lp = fetch_table(
        "lp_events",
        select="pair_address,event_type,provider,usd_value,ts",
        where={"pair_address": f"in.({','.join(pair_ids)})", "ts": f"gte.{since_iso}"},
        order="ts.desc",
        limit=20000,
    )
    if not lp.empty and "ts" in lp.columns:
        lp["ts"] = to_dt(lp["ts"])
    return lp

def compute_early_metrics(pairs: pd.DataFrame, swaps: pd.DataFrame, pwm: pd.DataFrame, lpev: pd.DataFrame) -> pd.DataFrame:
    if pairs.empty:
        return pairs.copy()
    metrics = pairs.copy()

    # First trade timestamp
    if not swaps.empty:
        first = swaps.groupby("pair_address")["swap_ts"].min().reset_index(name="first_trade_ts")
        metrics = metrics.merge(first, on="pair_address", how="left")
    else:
        metrics["first_trade_ts"] = pd.NaT

    # Time to first trade
    metrics["time_to_first_trade_s"] = (
        (metrics["first_trade_ts"] - metrics["effective_created_at"]).dt.total_seconds()
    )

    # Burst metrics (first 5 minutes)
    if not swaps.empty:
        swaps = swaps.merge(metrics[["pair_address","effective_created_at"]], on="pair_address", how="left")
        swaps["since_creation"] = (swaps["swap_ts"] - swaps["effective_created_at"]).dt.total_seconds()
        burst = swaps[swaps["since_creation"].between(0, 300)]
        burst_agg = burst.groupby("pair_address").agg(
            burst_swaps=("swap_ts", "count"),
            burst_traders=("trader_address", "nunique"),
        ).reset_index()
        burst_agg["swaps_per_min_burst"] = burst_agg["burst_swaps"] / 5
        metrics = metrics.merge(burst_agg[["pair_address","swaps_per_min_burst","burst_traders"]], on="pair_address", how="left")
    else:
        metrics["swaps_per_min_burst"] = 0.0
        metrics["burst_traders"] = 0

    # 10 minute unique traders
    if not swaps.empty:
        ten_min = swaps[swaps["since_creation"].between(0, 600)]
        uniq10 = ten_min.groupby("pair_address")["trader_address"].nunique().reset_index(name="uniq_traders_10m")
        metrics = metrics.merge(uniq10, on="pair_address", how="left")
    else:
        metrics["uniq_traders_10m"] = 0

    # 15 minute buy ratio
    if not pwm.empty:
        pwm = pwm.merge(metrics[["pair_address","effective_created_at"]], on="pair_address", how="left")
        pwm["since_creation"] = (pwm["window_start"] - pwm["effective_created_at"]).dt.total_seconds()
        win15 = pwm[pwm["since_creation"].between(0, 900)]
        agg15 = win15.groupby("pair_address").agg(total_buys=("buys","sum"), total_sells=("sells","sum")).reset_index()
        agg15["buy_ratio_15m"] = agg15["total_buys"] / (agg15["total_buys"] + agg15["total_sells"]).replace(0, np.nan)
        metrics = metrics.merge(agg15[["pair_address","buy_ratio_15m"]], on="pair_address", how="left")
    else:
        metrics["buy_ratio_15m"] = np.nan

    # Top 5 concentration
    if not swaps.empty:
        vol_by_trader = swaps.groupby(["pair_address","trader_address"])["usd_value"].sum().reset_index()
        total_vol = vol_by_trader.groupby("pair_address")["usd_value"].sum().reset_index(name="total_vol")
        vol_by_trader = vol_by_trader.merge(total_vol, on="pair_address", how="left")
        vol_by_trader["share"] = vol_by_trader["usd_value"] / vol_by_trader["total_vol"].replace(0, np.nan)
        top5 = vol_by_trader.sort_values(["pair_address","share"], ascending=[True, False]).groupby("pair_address").head(5)
        conc = top5.groupby("pair_address")["share"].sum().reset_index(name="top5_concentration")
        metrics = metrics.merge(conc, on="pair_address", how="left")
    else:
        metrics["top5_concentration"] = np.nan

    # LP add/remove ratio
    if not lpev.empty:
        adds = lpev[lpev["event_type"].str.lower().str.contains("add", na=False)].groupby("pair_address")["usd_value"].sum().reset_index(name="lp_adds")
        removes = lpev[lpev["event_type"].str.lower().str.contains("remove", na=False)].groupby("pair_address")["usd_value"].sum().reset_index(name="lp_removes")
        metrics = metrics.merge(adds, on="pair_address", how="left")
        metrics = metrics.merge(removes, on="pair_address", how="left")
        metrics["lp_add_remove_ratio"] = metrics["lp_adds"].fillna(0) / metrics["lp_removes"].replace(0, np.nan).fillna(1)
    else:
        metrics["lp_adds"] = 0.0
        metrics["lp_removes"] = 0.0
        metrics["lp_add_remove_ratio"] = np.nan

    return metrics

def score_and_classify(
    df: pd.DataFrame,
    ttf_ceil_s: float = 600,
    min_swaps_per_min: float = 2.0,
    min_uniques_10m: int = 8,
    buy_ratio_center: float = 0.55,
    buy_ratio_tol: float = 0.15,
    max_concentration: float = 0.6,
    leader_score_min: float = 50,
) -> pd.DataFrame:
    df = df.copy()

    # Time to first trade score (0-20)
    ttf = df["time_to_first_trade_s"].fillna(ttf_ceil_s).clip(upper=ttf_ceil_s)
    df["ttf_score"] = 20 * (1 - ttf / ttf_ceil_s)

    # Swaps per minute score (0-25)
    spm = df["swaps_per_min_burst"].fillna(0)
    df["spm_score"] = (25 * (spm / 10)).clip(upper=25)

    # Unique traders score (0-20)
    uniq = df["uniq_traders_10m"].fillna(0)
    df["uniq_score"] = (20 * (uniq / 30)).clip(upper=20)

    # Buy ratio score (0-20)
    br = df["buy_ratio_15m"].fillna(0.5)
    deviation = (br - buy_ratio_center).abs()
    df["br_score"] = (20 * (1 - deviation / buy_ratio_tol)).clip(lower=0, upper=20)

    # Concentration score (0-15)
    conc = df["top5_concentration"].fillna(1)
    df["conc_score"] = (15 * (1 - conc / max_concentration)).clip(lower=0, upper=15)

    # Total score
    df["early_score"] = df["ttf_score"] + df["spm_score"] + df["uniq_score"] + df["br_score"] + df["conc_score"]

    # Classification
    def classify(row):
        score = row.get("early_score", 0) or 0
        spm_val = row.get("swaps_per_min_burst", 0) or 0
        uniq_val = row.get("uniq_traders_10m", 0) or 0
        br_val = row.get("buy_ratio_15m", 0.5) or 0.5
        conc_val = row.get("top5_concentration", 1) or 1

        reasons = []
        if score >= leader_score_min and spm_val >= min_swaps_per_min and uniq_val >= min_uniques_10m:
            if conc_val <= max_concentration and abs(br_val - buy_ratio_center) <= buy_ratio_tol:
                return "Early Leader", "High score, healthy metrics"
            reasons.append("conc or buy_ratio out of range")
        if score >= leader_score_min * 0.6:
            if spm_val >= min_swaps_per_min * 0.5:
                return "Hype / Risky", "Moderate score but risky metrics"
        return "Loser / Skip", "; ".join(reasons) if reasons else "Low overall score"

    classified = df.apply(classify, axis=1, result_type="expand")
    df["classification"] = classified[0]
    df["reason"] = classified[1]

    return df

# ============================= Sidebar =============================
with st.sidebar:
    st.title("🚀 TrenchFeed")
    st.caption("Early Leader Intelligence")
    
    # Preview info
    st.markdown("---")
    time_remaining_str = format_time_remaining(remaining_preview_seconds)
    st.markdown(f"**⏱️ Preview:** {time_remaining_str} remaining")
    st.markdown(f"[Subscribe for Full Access →]({STRIPE_PURCHASE_URL})")
    st.markdown("---")
    
    st.subheader("Filters")
    recency_hours = st.slider("Recency (hours)", 1, 72, 6, help="How far back to look for new pairs.")
    max_pairs = st.slider("Max pairs", 100, 5000, 1000, help="Maximum pairs to fetch.")
    max_age_minutes = st.slider("Max age (min)", 5, 1440, 120, help="Only show pairs created within this window.")
    radar_window_m = st.slider("Radar window (min)", 5, 120, 30, help="Lookback window for Launch Radar.")
    radar_max = st.slider("Radar max rows", 10, 500, 100, help="Max rows in Launch Radar.")
    detail_limit = st.slider("Detail limit", 10, 500, 100, help="Max rows for detail views.")

    st.subheader("Scoring Thresholds")
    min_swaps_per_min = st.number_input("Min swaps/min (burst)", 0.5, 20.0, 2.0, 0.5)
    min_uniques_10m = st.number_input("Min unique traders (10m)", 1, 100, 8, 1)
    buy_center = st.number_input("Buy ratio center", 0.3, 0.7, 0.55, 0.05)
    buy_tol = st.number_input("Buy ratio tolerance", 0.05, 0.3, 0.15, 0.05)
    max_conc = st.number_input("Max top5 concentration", 0.3, 1.0, 0.6, 0.05)
    leader_score_min = st.number_input("Leader score min", 20, 100, 50, 5)

# ============================= Tabs =============================
tab_live, tab_leaders, tab_all, tab_radar, tab_detail, tab_top = st.tabs([
    "Live Feed", "Early Leaders", "All Candidates", "Launch Radar", "Token Detail", "Top Coins"
])

# ============================= Live Feed =============================
with tab_live:
    st.subheader("Live Feed")
    display_disclaimer()
    
    # Initialize session state for streaming
    if "stream_data" not in st.session_state:
        st.session_state.stream_data = []
    if "stream_initialized" not in st.session_state:
        st.session_state.stream_initialized = False
    if "stream_paused" not in st.session_state:
        st.session_state.stream_paused = False
    
    # Stream control buttons
    btn_col1, btn_col2, status_col = st.columns([1, 1, 2])
    
    with btn_col1:
        refresh_feed = st.button("🔄 Refresh", key="refresh_feed", use_container_width=True)
    
    with btn_col2:
        # Toggle pause/resume state
        is_paused = st.session_state.stream_paused
        pause_label = "▶️ Resume" if is_paused else "⏸️ Pause"
        if st.button(pause_label, key="pause_resume", use_container_width=True):
            st.session_state.stream_paused = not st.session_state.stream_paused
            st.rerun()
    
    with status_col:
        # Display current stream status with visual indicator
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
    
    # Handle refresh - reset data and re-initialize
    if refresh_feed:
        st.session_state.stream_data = []
        st.session_state.stream_initialized = False
        st.session_state.stream_paused = False
        st.rerun()
    
    # Placeholder for the live table
    live_placeholder = st.empty()
    
    # Define a mapping from raw column names to degen‑friendly jargon.  These
    # names resonate with crypto traders while remaining self‑explanatory.
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
        """Render the current stream data as a table."""
        if not st.session_state.stream_data:
            live_placeholder.info("No data yet. Waiting for stream events...")
            return
        
        try:
            df_stream = pd.json_normalize(st.session_state.stream_data)
        except Exception:
            df_stream = pd.DataFrame(st.session_state.stream_data)
        
        # Drop the recent_swaps column if present, as it contains opaque objects
        if "payload.recent_swaps" in df_stream.columns:
            df_stream = df_stream.drop(columns=["payload.recent_swaps"])
        
        # Rename columns using the friendly mapping where available
        rename_map = {k: v for k, v in _live_rename_map.items() if k in df_stream.columns}
        df_stream = df_stream.rename(columns=rename_map)
        
        # Determine primary columns (after rename, if rename occurred)
        primary_raw = ["payload.token.name", "payload.token.symbol", "payload.pair.base_token"]
        primary_cols = [rename_map.get(col, _live_rename_map.get(col, col)) for col in primary_raw if (rename_map.get(col, _live_rename_map.get(col, col)) in df_stream.columns)]
        
        # Build an ordered list of columns: primary first, then the rest
        other_cols = [c for c in df_stream.columns if c not in primary_cols]
        ordered_cols = primary_cols + other_cols
        
        # Take the last 50 events and reverse order so newest events appear first
        df_show = df_stream[ordered_cols].tail(50).iloc[::-1].copy()
        
        # Assign descending row numbers (highest at top) as the index
        n_rows = len(df_show)
        df_show.index = range(n_rows, 0, -1)
        
        # Display the table
        live_placeholder.dataframe(
            df_show,
            use_container_width=True,
            height=620,
        )
    
    def run_live_stream():
        """Internal helper to consume streaming data and update the table."""
        # Collect a bounded number of events to avoid infinite loops
        for event in stream_enterprise(max_events=200):
            # Check if stream is paused - if so, stop consuming new events
            # Note: Due to Streamlit's execution model, this check happens
            # between events. The pause takes effect after the current event.
            if st.session_state.stream_paused:
                break
            
            # Append the raw event to our session state
            st.session_state.stream_data.append(event)
            
            # Render the updated table
            render_stream_table()
    
    # If paused, just render the existing data
    if st.session_state.stream_paused:
        render_stream_table()
    # Start the stream when the component first renders or upon refresh
    elif not st.session_state.get("stream_initialized"):
        st.session_state.stream_initialized = True
        run_live_stream()
    else:
        # Already initialized and not paused - continue streaming
        run_live_stream()

# ============================= Early Leaders =============================
with tab_leaders:
    st.subheader("Early Leaders")
    display_disclaimer()
    since_iso = iso_hours_ago(recency_hours)
    pairs = fetch_table(
        "pairs",
        select="pair_address,token_address:base_token,base_token_name,base_token_symbol,pair_created_at,snapshot_ts,price_usd,fdv_usd,market_cap_usd",
        where={"snapshot_ts": f"gte.{since_iso}"},
        order="snapshot_ts.desc.nullslast",
        limit=max_pairs,
    )
    if pairs.empty:
        st.info("No recent pairs found.")
    else:
        pairs = numeric(pairs, ["price_usd","fdv_usd","market_cap_usd"])
        for tcol in ["pair_created_at","snapshot_ts"]:
            pairs[tcol] = to_dt(pairs[tcol])
        eff = pairs["pair_created_at"].where(pairs["pair_created_at"].notna(), pairs["snapshot_ts"])
        min_ts = now_utc() - pd.Timedelta(minutes=max_age_minutes)
        pairs = pairs.loc[eff >= min_ts].copy()
        pairs["effective_created_at"] = eff

        if pairs.empty:
            st.info("No pairs within the age filter.")
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
            # Sort the Early Leaders view by descending early_score to surface the highest
            # scoring tokens at the top.  If early_score is missing the sort has no effect.
            if not ranked.empty and "early_score" in ranked.columns:
                ranked = ranked.sort_values(["early_score"], ascending=[False]).reset_index(drop=True)

            cols = [
                "early_score","classification","reason",
                "swaps_per_min_burst","uniq_traders_10m","buy_ratio_15m","top5_concentration",
                # LP columns are removed from the display in Early Leaders; metrics still include them
                "token_address","name","symbol",
                "dexscreener","solscan","birdeye",
                "pair_address","effective_created_at","first_trade_ts","time_to_first_trade_s",
            ]
            shown = [c for c in cols if c in ranked.columns]
            # Display summary metrics for Early Leaders.  These indicators give a quick glance at
            # how many tokens fall into each classification and how strong they are on average.
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

# ============================= Launch Radar =============================
with tab_radar:
    st.subheader("Launch Radar")
    display_disclaimer()
    lookback_iso = iso(now_utc() - pd.Timedelta(minutes=radar_window_m))
    recent_pairs = fetch_table(
        "pairs",
        select="pair_address,token_address:base_token,base_token_name,base_token_symbol,pair_created_at,snapshot_ts,price_usd,fdv_usd,market_cap_usd",
        where={"pair_created_at": f"gte.{lookback_iso}"},
        order="pair_created_at.desc.nullslast",
        limit=2000
    )
    if recent_pairs.empty or recent_pairs["pair_created_at"].isna().all():
        extra = fetch_table(
            "pairs",
            select="pair_address,token_address:base_token,base_token_name,base_token_symbol,pair_created_at,snapshot_ts,price_usd,fdv_usd,market_cap_usd",
            where={"snapshot_ts": f"gte.{lookback_iso}"},
            order="snapshot_ts.desc.nullslast",
            limit=2000
        )
        if not extra.empty:
            recent_pairs = extra

    if recent_pairs.empty:
        st.info("No very recent launches.")
    else:
        recent_pairs["pair_created_at"] = to_dt(recent_pairs["pair_created_at"])
        recent_pairs["snapshot_ts"]     = to_dt(recent_pairs["snapshot_ts"])
        eff = recent_pairs["pair_created_at"].where(recent_pairs["pair_created_at"].notna(), recent_pairs["snapshot_ts"])
        recent_pairs["effective_created_at"] = eff

        pair_ids = recent_pairs["pair_address"].dropna().astype(str).unique().tolist()
        swaps = fetch_swaps_for_pairs(pair_ids, lookback_iso)
        pwm   = fetch_pwm_for_pairs(pair_ids, lookback_iso)
        lpev  = fetch_lp_events_for_pairs(pair_ids, lookback_iso)

        metrics = compute_early_metrics(recent_pairs, swaps, pwm, lpev)
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

        if "market_cap_usd" in ranked.columns:
            ranked = numeric(ranked, ["market_cap_usd"])
            ranked = ranked[ranked["market_cap_usd"].fillna(0) >= 30000]

        cols = [
            "effective_created_at",
            "token_address","name","symbol",
            "dexscreener","solscan","birdeye",
            "early_score","classification","reason",
            "swaps_per_min_burst","uniq_traders_10m","buy_ratio_15m","top5_concentration",
            # LP columns are intentionally excluded from Launch Radar display
            "pair_address",
        ]
        shown = [c for c in cols if c in ranked.columns]
        # Sort Launch Radar strictly by highest early_score first.  This ensures that top-scoring
        # tokens (actual winners) appear first instead of being grouped by classification or
        # alphabetical order.  We drop secondary sorting by classification/effective_created_at.
        ranked = ranked.sort_values(["early_score"], ascending=[False]).head(radar_max).reset_index(drop=True)
        # Display summary metrics for Launch Radar.  These metrics provide a quick overview
        # of the newly launched pairs and how many qualify as leaders or hype tokens.
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

# ============================= Token Detail =============================
with tab_detail:
    st.subheader("Token Detail")
    display_disclaimer()
    q_token = st.text_input("Token address", help="Paste token address")
    if q_token:
        if st.button("Fetch latest Data"):
            import subprocess
            helius_path = "/opt/sol/etl/hel.py"
            try:
                subprocess.run(["python3", helius_path, q_token], check=False)
                st.success("Refreshed from Helius. You can now view updated tables.")
            except Exception as e:
                st.warning(f"Helius refresh failed: {e}")
        tok = fetch_table("tokens", select="token_address,chain_id,name,symbol,updated_at", where={"token_address": f"eq.{q_token}"}, limit=1)
        if not tok.empty: tok["updated_at"] = to_dt(tok.get("updated_at"))
        st.write("Tokens")
        st.dataframe(tok.reset_index(drop=True), use_container_width=True, height=120)

        tstate = fetch_table("token_state", select="*", where={"token_address": f"eq.{q_token}"}, limit=1)
        if not tstate.empty and "last_window" in tstate.columns:
            tstate["last_window"] = to_dt(tstate["last_window"])
        st.write("Token State")
        st.dataframe(tstate.reset_index(drop=True), use_container_width=True, height=150)

        creators = fetch_table("creators", select="*", where={"token_address": f"eq.{q_token}"}, limit=detail_limit)
        if not creators.empty and "created_at" in creators.columns:
            creators["created_at"] = to_dt(creators["created_at"])
        st.write("Creators")
        st.dataframe(creators.reset_index(drop=True), use_container_width=True, height=150)

        holders = fetch_table("holder_snapshots", select="*", where={"token_address": f"eq.{q_token}"}, order="snapshot_ts.desc", limit=detail_limit)
        if not holders.empty: holders["snapshot_ts"] = to_dt(holders.get("snapshot_ts"))
        st.write("Holder Snapshots")
        st.dataframe(holders.reset_index(drop=True), use_container_width=True, height=220)

        rflags = fetch_table("risk_flags", select="*", where={"token_address": f"eq.{q_token}"}, order="ts.desc", limit=detail_limit)
        if not rflags.empty: rflags["ts"] = to_dt(rflags.get("ts"))
        st.write("Risk Flags")
        st.dataframe(rflags.reset_index(drop=True), use_container_width=True, height=180)

        pairs_q = fetch_table(
            "pairs",
            select=("pair_address,chain_id,dex_id,base_token,quote_token,price_usd,fdv_usd,market_cap_usd,"
                    "pair_created_at,snapshot_ts,base_token_name,base_token_symbol,quote_token_name,quote_token_symbol"),
            where={"or": f"(base_token.eq.{q_token},quote_token.eq.{q_token})"},
            order="snapshot_ts.desc.nullslast",
            limit=detail_limit,
        )
        if not pairs_q.empty:
            pairs_q = numeric(pairs_q, ["price_usd","fdv_usd","market_cap_usd"])
            for tcol in ["pair_created_at","snapshot_ts"]:
                pairs_q[tcol] = to_dt(pairs_q[tcol])
            pairs_q = pairs_q.rename(columns={"base_token":"token_address"})
            pairs_q["effective_created_at"] = pairs_q["pair_created_at"].where(pairs_q["pair_created_at"].notna(), pairs_q["snapshot_ts"])
            pairs_q = ensure_pair_links(pairs_q, token_col="token_address")
            pairs_q = attach_token_names(pairs_q, token_col="token_address")
            pairs_q = add_links(pairs_q)
        st.write("Pairs")
        st.dataframe(pairs_q.reset_index(drop=True), use_container_width=True, height=250,
                     column_config=link_config(list(pairs_q.columns) if not pairs_q.empty else []))

        pair_ids: List[str] = list(pairs_q["pair_address"].dropna().unique()) if not pairs_q.empty else []
        if pair_ids:
            swaps_q = fetch_table(
                "swaps",
                select="pair_address,trader_address,swap_ts,side,usd_value,sol_amount,token_amount",
                where={"pair_address": f"in.({','.join(pair_ids)})"},
                order="swap_ts.desc",
                limit=detail_limit,
            )
            if not swaps_q.empty: swaps_q["swap_ts"] = to_dt(swaps_q.get("swap_ts"))
            st.write("Recent Swaps")
            st.dataframe(swaps_q.reset_index(drop=True), use_container_width=True, height=250)

# ============================= Top Coins =============================
with tab_top:
    st.subheader("Top Coins (Database Views)")
    display_disclaimer()
    available_views = [
        "top_coins_24h",
        "top_coins_1h",
        "hot_pairs",
        "whale_activity",
        "smart_money_flow",
    ]
    view_choice = st.selectbox("Select view", available_views)
    view_limit = st.slider("Rows", 10, 500, 100, key="top_view_limit")
    if st.button("Load View", key="load_top_view"):
        df_view = fetch_view(view_choice, view_limit)
        if df_view.empty:
            st.info(f"No data from {view_choice}.")
        else:
            df_view = ensure_pair_links(df_view)
            df_view = attach_token_names(df_view)
            df_view = add_links(df_view)
            shown = list(df_view.columns)
            st.dataframe(df_view.reset_index(drop=True), use_container_width=True, height=620, column_config=link_config(shown))
