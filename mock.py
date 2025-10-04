import csv
import json
import math
import os
import time
import random
from datetime import datetime
from typing import Dict, List, Set, Callable

from dotenv import load_dotenv

# ================== LOAD .ENV ==================
load_dotenv()

WEARERS_JSON = os.getenv("WEARERS_JSON")

CHART_DIRS: Dict[str, str] = {
    "activity": os.getenv("ACTIVITY_DIR"),
    "calmness": os.getenv("CALMNESS_DIR"),
    "mobility": os.getenv("MOBILITY_DIR"),
    "cadence": os.getenv("CADENCE_DIR"),
    "prv": os.getenv("PRV_DIR"),
    "skin": os.getenv("SKIN_DIR"),
}

# deletion controls
DELETE_ON_REMOVAL = os.getenv("DELETE_ON_REMOVAL", "false").strip().lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() == "true"

# Poll JSON every 15s
WEARERS_POLL_SEC = 15

# ================== GENERATORS ==================
def _id_num(wearer_id: str) -> int:
    try:
        return int(wearer_id)
    except Exception:
        return abs(hash(wearer_id)) % 10_000_000

def clamp(v, lo, hi):
    return lo if v < lo else hi if v > hi else v

def gen_activity_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour * 3600 + now.minute * 60 + now.second
    u = _id_num(wearer_id)
    wave = 0.25 * math.sin(2 * math.pi * (mid + (u % 86400)) / 86400)
    base = 0.60 + ((u % 7) - 3) * 0.005
    noise = random.uniform(-0.05, 0.05)
    return clamp(base + wave + noise, 0.0, 1.0)

def gen_calmness_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour * 3600 + now.minute * 60 + now.second
    u = _id_num(wearer_id)
    wave = 0.30 * math.cos(2 * math.pi * (mid + (u % 86400)) / 86400)
    base = 0.50 + ((u % 5) - 2) * 0.004
    noise = random.uniform(-0.05, 0.05)
    return clamp(base + wave + noise, 0.0, 1.0)

def gen_mobility_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour * 3600 + now.minute * 60 + now.second
    u = _id_num(wearer_id)
    wave = 0.25 * math.sin(2 * math.pi * (mid + (u % 5400)) / 5400)
    base = 0.30 + ((u % 9) - 4) * 0.003
    noise = random.uniform(-0.04, 0.04)
    return clamp(base + wave + noise, 0.0, 1.0)

def gen_cadence_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour * 3600 + now.minute * 60 + now.second
    u = _id_num(wearer_id)
    wave = 7.0 * math.sin(2 * math.pi * (mid + (u % 86400)) / 86400)
    base = 52.0 + ((u % 5) - 2) * 0.3
    noise = random.uniform(-2.0, 2.0)
    return clamp(base + wave + noise, 35.0, 80.0)

def gen_prv_value(now: datetime, wearer_id: str) -> float:
    mid = now.hour * 3600 + now.minute * 60 + now.second
    u = _id_num(wearer_id)
    wave = 18.0 * math.cos(2 * math.pi * (mid + (u % 21600)) / 21600)  # ~6h cycle
    base = 40.0 + ((u % 7) - 3) * 0.5
    noise = random.uniform(-6.0, 6.0)
    return clamp(base + wave + noise, 5.0, 100.0)

def gen_skin_value(now: datetime, wearer_id: str) -> float:
    u = _id_num(wearer_id)
    base = 0.02 + ((u % 9) - 4) * 0.0008
    phasic = 0.015 * (1 + math.sin(2 * math.pi * (now.second + (u % 120)) / 45.0))
    spike = 0.0
    if random.random() < 0.07:
        spike = random.uniform(0.02, 0.05)
    if random.random() < 0.20:
        return 0.0
    noise = random.uniform(-0.004, 0.004)
    return clamp(base + phasic + spike + noise, 0.0, 0.08)

GEN_MAP: Dict[str, Callable[[datetime, str], float]] = {
    "activity": gen_activity_value,
    "calmness": gen_calmness_value,
    "mobility": gen_mobility_value,
    "cadence": gen_cadence_value,
    "prv": gen_prv_value,
    "skin": gen_skin_value,
}

# formatters
def fmt_fixed_3(v: float) -> str: return f"{v:.3f}"
def fmt_skin(v: float) -> str:
    s = f"{v:.8f}".rstrip("0").rstrip(".")
    if "." not in s: s += ".0"
    return s

FORMAT_MAP: Dict[str, Callable[[float], str]] = {
    "activity": lambda v: f"{v:.8f}",
    "calmness": lambda v: f"{v:.8f}",
    "mobility": lambda v: f"{v:.8f}",
    "cadence": fmt_fixed_3,
    "prv": fmt_fixed_3,
    "skin": fmt_skin,
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

def append_row(file_path: str, y_str: str, now: datetime, tz: str):
    x_str = f"{now.strftime('%Y-%m-%d %H:%M:%S')}{tz}"
    time_str = now.strftime("%H:%M:%S")
    date_str = now.strftime("%Y-%m-%d")
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([x_str, y_str, time_str, date_str])

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

# ================== MAIN ==================
if __name__ == "__main__":
    missing = [k for k, v in CHART_DIRS.items() if not v]
    if not WEARERS_JSON or missing:
        raise SystemExit(f"❌ Missing .env values. Need WEARERS_JSON and all CHART_DIRS.")

    print("🚀 Smart Socks CSV generator (multi-user, SECOND cadence, live JSON discovery)")

    known_ids: Set[str] = set()
    current_ids = set(load_wearer_ids(WEARERS_JSON))
    for wid in current_ids:
        for _, d in CHART_DIRS.items():
            ensure_file_with_header(d, wid)
    known_ids |= current_ids

    tz = tz_offset_colon()
    last_json_check = time.monotonic()

    while True:
        now = datetime.now()

        # rescan JSON
        if time.monotonic() - last_json_check >= WEARERS_POLL_SEC:
            last_json_check = time.monotonic()
            ids = set(load_wearer_ids(WEARERS_JSON))
            new_ids = ids - known_ids
            if new_ids:
                print(f"🔎 new wearers: {sorted(new_ids)}")
                for wid in new_ids:
                    for _, d in CHART_DIRS.items():
                        ensure_file_with_header(d, wid)
            known_ids = ids

        for wid in list(known_ids):
            for chart, d in CHART_DIRS.items():
                file_path = os.path.join(d, f"{wid}.csv")
                if not os.path.exists(file_path):
                    ensure_file_with_header(d, wid)
                val = GEN_MAP[chart](now, wid)
                y_str = FORMAT_MAP[chart](val)
                append_row(file_path, y_str, now, tz)
                print(f"📝 {chart}/{wid}.csv -> {now.strftime('%H:%M:%S')} = {y_str}")

        time.sleep(1)
