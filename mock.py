import csv
import json
import math
import os
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Set, Callable, Optional, Tuple

from dotenv import load_dotenv

# ================== LOAD .ENV ==================
load_dotenv()

WEARERS_JSON = os.getenv("WEARERS_JSON")

# ---- Helper to read an env var with fallbacks ----
def env_any(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None

# ================== CHART DIRS (supports multiple env names) ==================
CHART_DIRS: Dict[str, Optional[str]] = {
    # earlier 6
    "activity": env_any("ACTIVITY_DIR"),
    "calmness": env_any("CALMNESS_DIR"),
    "mobility": env_any("MOBILITY_DIR"),
    "cadence": env_any("CADENCE_DIR"),
    "prv": env_any("PRV_DIR"),                      # PulseRateVariability
    "skin": env_any("SKIN_DIR"),                    # SkinConductance

    # new set
    "bouts": env_any("BOUTS_DIR"),
    "longest_bout": env_any("LONGEST_BOUTS_DIR"),
    "pulse_rate": env_any("PULSE_RATE_DIR"),
    "skin_temp": env_any("SKIN_TEMPERATURE_DIR"),   # SkinTemperature
    "steps": env_any("STEPS_DIR"),
    "step_time_var": env_any("STEP_TIME_VARIATION_DIR", "STEP_TIMES_VARIATION_DIR"),
    "symmetry": env_any("SYMMETRY_DIR"),
    "turns": env_any("TURNS_DIR", "TURNS"),         # supports TURNS or TURNS_DIR
    "walking": env_any("WALKING_DIR", "WALKING__DIR", "WALKING"),  # supports WALKING__DIR
}

# deletion controls
DELETE_ON_REMOVAL = (os.getenv("DELETE_ON_REMOVAL", "false").strip().lower() == "true")
DRY_RUN = (os.getenv("DRY_RUN", "false").strip().lower() == "true")

# JSON poll
WEARERS_POLL_SEC = int(os.getenv("WEARERS_POLL_SEC", "15"))

# ====== BACKFILL CONFIG ======
# How many days to ensure exist historically (default 60 days ≈ two months)
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "60"))
# Backfill step: "second" or "minute" (minute is MUCH smaller/faster)
BACKFILL_STEP = os.getenv("BACKFILL_STEP", "minute").strip().lower()
# Validate/normalize
if BACKFILL_STEP not in ("second", "minute"):
    BACKFILL_STEP = "minute"
BACKFILL_STEP_SECONDS = 1 if BACKFILL_STEP == "second" else 60

# ================== GENERATORS (per-second) ==================
def _id_num(wearer_id: str) -> int:
    try:
        return int(wearer_id)
    except Exception:
        return abs(hash(wearer_id)) % 10_000_000

def clamp(v, lo, hi): return lo if v < lo else hi if v > hi else v

# --- originals (normalized 0..1 or defined ranges) ---
def gen_activity_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour*3600 + now.minute*60 + now.second
    u = _id_num(wearer_id)
    wave = 0.25 * math.sin(2*math.pi*(mid + (u % 86400))/86400)
    base = 0.60 + ((u % 7) - 3)*0.005
    noise = random.uniform(-0.05, 0.05)
    return clamp(base + wave + noise, 0.0, 1.0)

def gen_calmness_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour*3600 + now.minute*60 + now.second
    u = _id_num(wearer_id)
    wave = 0.30 * math.cos(2*math.pi*(mid + (u % 86400))/86400)
    base = 0.50 + ((u % 5) - 2)*0.004
    noise = random.uniform(-0.05, 0.05)
    return clamp(base + wave + noise, 0.0, 1.0)

def gen_mobility_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour*3600 + now.minute*60 + now.second
    u = _id_num(wearer_id)
    wave = 0.25 * math.sin(2*math.pi*(mid + (u % 5400))/5400)   # ~90 min cycle
    base = 0.30 + ((u % 9) - 4)*0.003
    noise = random.uniform(-0.04, 0.04)
    return clamp(base + wave + noise, 0.0, 1.0)

def gen_cadence_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour*3600 + now.minute*60 + now.second
    u = _id_num(wearer_id)
    wave = 7.0 * math.sin(2*math.pi*(mid + (u % 86400))/86400)
    base = 52.0 + ((u % 5) - 2)*0.3
    noise = random.uniform(-2.0, 2.0)
    return clamp(base + wave + noise, 35.0, 80.0)

def gen_prv_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour*3600 + now.minute*60 + now.second
    u = _id_num(wearer_id)
    wave = 18.0 * math.cos(2*math.pi*(mid + (u % 21600))/21600) # ~6h
    base = 40.0 + ((u % 7) - 3)*0.5
    noise = random.uniform(-6.0, 6.0)
    return clamp(base + wave + noise, 5.0, 100.0)

def gen_skin_value(now: datetime, wearer_id: str) -> float:
    u = _id_num(wearer_id)
    base = 0.02 + ((u % 9) - 4)*0.0008
    phasic = 0.015 * (1 + math.sin(2*math.pi*(now.second + (u % 120))/45.0))
    spike = random.uniform(0.02, 0.05) if random.random() < 0.07 else 0.0
    if random.random() < 0.20:  # sparse zeros
        return 0.0
    noise = random.uniform(-0.004, 0.004)
    return clamp(base + phasic + spike + noise, 0.0, 0.08)

# --- new group ---
def gen_bouts_value(now: datetime, wearer_id: str) -> float:
    r = random.random()
    if r < 0.75: v = random.randint(0, 1)
    elif r < 0.93: v = random.randint(2, 3)
    else: v = random.randint(4, 6)
    return float(v)

def gen_longest_bout_value(now: datetime, wearer_id: str) -> float:
    u = _id_num(wearer_id)
    mid = now.hour*3600 + now.minute*60 + now.second
    wave = 60.0 * (1 + math.sin(2*math.pi*(mid + (u % 5000))/5000.0))  # 0..120
    base = 30.0 + ((u % 7) - 3)*2.0
    noise = random.uniform(-10, 10)
    return clamp(base + wave + noise, 2.0, 300.0)

def gen_pulse_rate_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour*3600 + now.minute*60 + now.second
    u = _id_num(wearer_id)
    wave = 12.0 * math.sin(2*math.pi*(mid + (u % 86400))/86400)
    base = 75.0 + ((u % 9) - 4)*0.6
    noise = random.uniform(-6.0, 6.0)
    return clamp(base + wave + noise, 50.0, 120.0)

def gen_skin_temperature_value(now: datetime, wearer_id: str) -> float:
    u = _id_num(wearer_id)
    mid = now.hour*3600 + now.minute*60 + now.second
    wave = 0.010 * math.sin(2*math.pi*(mid + (u % 4000))/4000.0)   # ±0.01
    base = 32.170 + ((u % 5) - 2)*0.001
    noise = random.uniform(-0.006, 0.006)
    return clamp(base + wave + noise, 32.120, 32.220)

def gen_steps_value(now: datetime, wearer_id: str) -> float:
    r = random.random()
    if r < 0.80: return 0.0
    elif r < 0.95: return float(random.randint(1, 2))
    else: return float(random.randint(3, 4))

def gen_step_time_variation_value(now: datetime, wearer_id: str) -> float:
    u = _id_num(wearer_id)
    mid = now.hour*3600 + now.minute*60 + now.second
    wave = 0.06 * math.sin(2*math.pi*(mid + (u % 7200))/7200.0)  # ±0.06
    base = 0.56 + ((u % 7) - 3)*0.002
    noise = random.uniform(-0.04, 0.04)
    return clamp(base + wave + noise, 0.40, 0.80)

def gen_symmetry_value(now: datetime, wearer_id: str) -> float:
    u = _id_num(wearer_id)
    mid = now.hour*3600 + now.minute*60 + now.second
    wave = 0.003 * math.sin(2*math.pi*(mid + (u % 600))/600.0)   # ±0.003
    base = 0.900 + ((u % 5) - 2)*0.0005
    noise = random.uniform(-0.0025, 0.0025)
    return clamp(base + wave + noise, 0.885, 0.915)

def gen_turns_value(now: datetime, wearer_id: str) -> float:
    u = _id_num(wearer_id)
    mid = now.hour*3600 + now.minute*60 + now.second
    wave = 0.18 * math.sin(2*math.pi*(mid + (u % 1800))/1800.0)  # ±0.18
    base = 1.80 + ((u % 7) - 3)*0.01
    noise = random.uniform(-0.08, 0.08)
    return clamp(base + wave + noise, 1.40, 2.20)

def gen_walking_value(now: datetime, wearer_id: str) -> float:
    u = _id_num(wearer_id)
    mid = now.hour*3600 + now.minute*60 + now.second
    wave = 0.25 * math.sin(2*math.pi*(mid + (u % 2700))/2700.0)  # ±0.25
    base = 1.20 + ((u % 5) - 2)*0.01
    noise = random.uniform(-0.12, 0.12)
    return clamp(base + wave + noise, 0.80, 1.80)

GEN_MAP: Dict[str, Callable[[datetime, str], float]] = {
    "activity": gen_activity_value,
    "calmness": gen_calmness_value,
    "mobility": gen_mobility_value,
    "cadence": gen_cadence_value,
    "prv": gen_prv_value,
    "skin": gen_skin_value,
    "bouts": gen_bouts_value,
    "longest_bout": gen_longest_bout_value,
    "pulse_rate": gen_pulse_rate_value,
    "skin_temp": gen_skin_temperature_value,
    "steps": gen_steps_value,
    "step_time_var": gen_step_time_variation_value,
    "symmetry": gen_symmetry_value,
    "turns": gen_turns_value,
    "walking": gen_walking_value,
}

# ================== FORMATTERS (match your samples) ==================
def fmt_8(v: float) -> str: return f"{v:.8f}"
def fmt_3(v: float) -> str: return f"{v:.3f}"
def fmt_2(v: float) -> str: return f"{v:.2f}"
def fmt_int(v: float) -> str: return str(int(round(v)))

def fmt_trim_to_4(v: float) -> str:
    s = f"{v:.4f}".rstrip("0").rstrip(".")
    if "." not in s: s += ".0"
    return s

def fmt_skin_var(v: float) -> str:
    s = f"{v:.8f}".rstrip("0").rstrip(".")
    if "." not in s: s += ".0"
    return s

FORMAT_MAP: Dict[str, Callable[[float], str]] = {
    "activity": fmt_8,       # 0..1 with 8 dp
    "calmness": fmt_8,
    "mobility": fmt_8,
    "cadence": fmt_3,        # 60.565
    "prv": fmt_3,            # 56.711
    "skin": fmt_skin_var,    # 0.01766667, 0.06, 0.0
    "bouts": fmt_int,        # integer
    "longest_bout": fmt_int, # integer seconds
    "pulse_rate": fmt_int,   # integer BPM
    "skin_temp": fmt_3,      # 32.168
    "steps": fmt_int,        # integer
    "step_time_var": fmt_trim_to_4,  # 0.4955, 0.63
    "symmetry": fmt_3,       # 0.896
    "turns": fmt_2,          # 1.58
    "walking": fmt_3,        # 1.327
}

# ================== HELPERS ==================
def tz_offset_colon() -> str:
    z = datetime.now().astimezone().strftime("%z")
    return f"{z[:-2]}:{z[-2:]}" if len(z) == 5 else z

def ensure_file_with_header(dir_path: str, wearer_id: str) -> str:
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"{wearer_id}.csv")
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["x", "y", "time", "date"])
        print(f"✅ header created: {file_path}")
    return file_path

def append_row(file_path: str, y_str: str, ts: datetime, tz: str):
    x_str = f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{tz}"
    time_str = ts.strftime("%H:%M:%S")
    date_str = ts.strftime("%Y-%m-%d")
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([x_str, y_str, time_str, date_str])

def delete_csvs_for_wearer(wearer_id: str, configured: Dict[str, str]):
    for _, d in configured.items():
        path = os.path.join(d, f"{wearer_id}.csv")
        if os.path.exists(path):
            if DRY_RUN:
                print(f"🧪 DRY_RUN: would delete {path}")
            elif DELETE_ON_REMOVAL:
                try:
                    os.remove(path)
                    print(f"🗑️ deleted: {path}")
                except PermissionError:
                    print(f"⚠️ cannot delete (in use/locked): {path}")
                except Exception as e:
                    print(f"⚠️ delete failed for {path}: {e}")

def load_wearer_ids(json_path: str) -> List[str]:
    if not json_path or not os.path.exists(json_path):
        return []
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        wearers = (data or {}).get("Wearers", {})
        ids: List[str] = []
        if isinstance(wearers, dict):
            for _, wearer in wearers.items():
                wid = str(wearer.get("id", "")).strip()
                if wid: ids.append(wid)
        elif isinstance(wearers, list):
            for wearer in wearers:
                wid = str((wearer or {}).get("id", "")).strip()
                if wid: ids.append(wid)
        return ids
    except Exception:
        return []

# --------- CSV scan helpers (to decide backfill need) ---------
def parse_first_last_ts(file_path: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Returns (first_ts, last_ts) from CSV (based on column x => 'YYYY-mm-dd HH:MM:SS+TZ').
    If file has only header or is missing, returns (None, None).
    """
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return (None, None)

    first_ts = None
    last_ts = None
    try:
        # First ts
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or (row[0].lower() == "x"):
                    continue
                x = row[0]
                x_no_tz = x.split("+")[0] if "+" in x else x
                first_ts = datetime.strptime(x_no_tz, "%Y-%m-%d %H:%M:%S")
                break
        # Last ts (tail scan)
        with open(file_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            size = 4096
            chunk = b""
            pos = end
            while pos > 0:
                read_size = size if pos - size > 0 else pos
                pos -= read_size
                f.seek(pos)
                chunk = f.read(read_size) + chunk
                if chunk.count(b"\n") > 5:
                    break
            lines = [ln.decode("utf-8").strip() for ln in chunk.splitlines() if ln.strip()]
            for line in reversed(lines):
                if line.lower().startswith("x,"):
                    continue
                parts = line.split(",")
                if len(parts) >= 1:
                    x = parts[0]
                    x_no_tz = x.split("+")[0] if "+" in x else x
                    last_ts = datetime.strptime(x_no_tz, "%Y-%m-%d %H:%M:%S")
                    break
    except Exception:
        pass
    return (first_ts, last_ts)

def needs_backfill(first_ts: Optional[datetime], required_start: datetime) -> bool:
    """
    We need backfill if we have no data or our earliest data is AFTER the required start.
    """
    if first_ts is None:
        return True
    return first_ts > required_start

def backfill_one_file(chart: str, dir_path: str, wearer_id: str, tz: str, required_start: datetime):
    """
    Backfill a single CSV from required_start up to (first_existing_ts - step),
    or up to 'now - step' if the file has no data at all.
    """
    file_path = os.path.join(dir_path, f"{wearer_id}.csv")
    ensure_file_with_header(dir_path, wearer_id)
    first_ts, last_ts = parse_first_last_ts(file_path)

    if not needs_backfill(first_ts, required_start):
        return  # already has at least two months historical coverage

    # Determine backfill end (exclusive)
    if first_ts is not None:
        backfill_end_exclusive = first_ts  # stop right before first existing row
    else:
        backfill_end_exclusive = datetime.now()

    # Safety: if required_start >= end, nothing to do
    if required_start >= backfill_end_exclusive:
        return

    gen_fn = GEN_MAP[chart]
    fmt_fn = FORMAT_MAP[chart]

    # Align start to step boundary (drop seconds if minute mode)
    if BACKFILL_STEP_SECONDS == 60:
        start_ts = required_start.replace(second=0, microsecond=0)
    else:
        start_ts = required_start.replace(microsecond=0)

    # Iterate and append
    ts = start_ts
    step = timedelta(seconds=BACKFILL_STEP_SECONDS)
    # Write in chunks to avoid keeping file open too long
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        while ts < backfill_end_exclusive:
            val = gen_fn(ts, wearer_id)
            y_str = fmt_fn(val)
            x_str = f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{tz}"
            writer.writerow([x_str, y_str, ts.strftime("%H:%M:%S"), ts.strftime("%Y-%m-%d")])
            ts += step

    print(f"📦 backfilled {chart}/{wearer_id}.csv from {start_ts} to {backfill_end_exclusive} ({BACKFILL_STEP} steps)")

def backfill_all(configured: Dict[str, str], wearer_ids: Set[str], tz: str):
    """
    For every chart+wearer, ensure we have at least BACKFILL_DAYS of history.
    """
    required_start = (datetime.now() - timedelta(days=BACKFILL_DAYS))
    for wid in wearer_ids:
        for chart, dir_path in configured.items():
            backfill_one_file(chart, dir_path, wid, tz, required_start)

# ================== MAIN (per-second cadence + backfill) ==================
if __name__ == "__main__":
    configured = {k: v for k, v in CHART_DIRS.items() if v}
    if not WEARERS_JSON or not configured:
        missing = [k for k, v in CHART_DIRS.items() if not v]
        raise SystemExit(
            "❌ Missing .env values. Need WEARERS_JSON and at least one *_DIR.\n"
            f"Missing dirs: {', '.join(missing)}"
        )

    print("🚀 Smart Socks CSV generator (multi-user, SECOND cadence, live JSON discovery + startup backfill)")
    print(f"🧹 DELETE_ON_REMOVAL={DELETE_ON_REMOVAL} | DRY_RUN={DRY_RUN}")
    print(f"🗓️ Backfill: last {BACKFILL_DAYS} days at {BACKFILL_STEP} resolution")

    # Prepare known wearers & files
    known_ids: Set[str] = set()
    current_ids = set(load_wearer_ids(WEARERS_JSON))
    for wid in current_ids:
        for _, d in configured.items():
            ensure_file_with_header(d, wid)
    known_ids |= current_ids

    tz = tz_offset_colon()

    # ===== BACKFILL on startup =====
    if BACKFILL_DAYS > 0:
        backfill_all(configured, known_ids, tz)

    # ===== Live loop (per-second appends) =====
    last_json_check = time.monotonic()

    while True:
        now = datetime.now()

        # JSON re-scan (adds/removals)
        if time.monotonic() - last_json_check >= WEARERS_POLL_SEC:
            last_json_check = time.monotonic()
            ids = set(load_wearer_ids(WEARERS_JSON))

            removed = known_ids - ids
            if removed:
                print(f"🧹 wearers removed: {sorted(removed)}")
                for wid in removed:
                    delete_csvs_for_wearer(wid, configured)

            added = ids - known_ids
            if added:
                print(f"🔎 new wearers: {sorted(added)}")
                for wid in added:
                    for _, d in configured.items():
                        ensure_file_with_header(d, wid)
                # Backfill for newly discovered users too
                backfill_all(configured, added, tz)

            known_ids = ids

        # Append one row per second per wearer per configured chart
        for wid in list(known_ids):
            for chart, d in configured.items():
                file_path = os.path.join(d, f"{wid}.csv")
                if not os.path.exists(file_path):
                    ensure_file_with_header(d, wid)
                val = GEN_MAP[chart](now, wid)
                y_str = FORMAT_MAP[chart](val)
                append_row(file_path, y_str, now, tz)

        time.sleep(1)
