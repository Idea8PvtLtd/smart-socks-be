import csv
import json
import math
import os
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Set

from dotenv import load_dotenv

# ================== LOAD .ENV ==================
load_dotenv()

WEARERS_JSON = os.getenv("WEARERS_JSON")

CHART_DIRS = {
    "activity": os.getenv("ACTIVITY_DIR"),
    "calmness": os.getenv("CALMNESS_DIR"),
    "mobility": os.getenv("MOBILITY_DIR"),
}

# NEW: deletion controls
DELETE_ON_REMOVAL = os.getenv("DELETE_ON_REMOVAL", "false").strip().lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() == "true"

# Poll the JSON for new/removed users every N seconds
WEARERS_POLL_SEC = 15

# ================== GENERATORS (distinct per chart & user) ==================
def _id_num(wearer_id: str) -> int:
    try:
        return int(wearer_id)
    except Exception:
        return abs(hash(wearer_id)) % 10_000_000

def gen_activity_value(now_floor: datetime, wearer_id: str) -> float:
    mid = now_floor.hour * 60 + now_floor.minute
    u = _id_num(wearer_id)
    phase = (u % 1440)
    wave = 2.6 * math.sin(2 * math.pi * (mid + phase) / (24 * 60))
    base = 26.5 + ((u % 7) - 3) * 0.15
    noise = random.uniform(-1.2, 1.2)
    return round(max(23.0, min(31.0, base + wave + noise)), 2)

def gen_calmness_value(now_floor: datetime, wearer_id: str) -> float:
    mid = now_floor.hour * 60 + now_floor.minute
    u = _id_num(wearer_id)
    phase = (u * 3) % 1440
    wave = 4.2 * math.cos(2 * math.pi * (mid + 120 + phase) / (24 * 60))
    base = 50.0 + ((u % 5) - 2) * 0.2
    noise = random.uniform(-1.0, 1.0)
    return round(max(40.0, min(60.0, base + wave + noise)), 2)

def gen_mobility_value(now_floor: datetime, wearer_id: str) -> float:
    mid = now_floor.hour * 60 + now_floor.minute
    u = _id_num(wearer_id)
    wave = 3.0 * math.sin(2 * math.pi * (mid + (u % 90)) / 90.0)
    base = 15.0 + ((u % 9) - 4) * 0.1
    noise = random.uniform(-0.8, 0.8)
    return round(max(10.0, min(20.0, base + wave + noise)), 2)

GEN_MAP = {
    "activity": gen_activity_value,
    "calmness": gen_calmness_value,
    "mobility": gen_mobility_value,
}

# ================== FILE I/O HELPERS ==================
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

def read_last_minute(file_path: str):
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return None
    last_dt = None
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            for line in reversed(lines):
                line = line.strip()
                if not line or line.lower().startswith("x,"):
                    continue
                parts = line.split(",")
                if len(parts) >= 4 and parts[0]:
                    x_no_tz = parts[0].split("+")[0]
                    last_dt = datetime.strptime(x_no_tz, "%Y-%m-%d %H:%M:%S")
                    break
    except Exception:
        pass
    return last_dt

def append_row(file_path: str, y_value: float, now_floor: datetime, tz: str):
    x_str = f"{now_floor.strftime('%Y-%m-%d %H:%M:%S')}{tz}"
    time_str = now_floor.strftime("%H:%M:%S")
    date_str = now_floor.strftime("%Y-%m-%d")
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([x_str, y_value, time_str, date_str])

# NEW: deletion utility
def delete_csvs_for_wearer(wearer_id: str):
    """Delete {id}.csv from each chart dir (with DRY_RUN/DELETE_ON_REMOVAL controls)."""
    for chart, d in CHART_DIRS.items():
        if not d:
            continue
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

# ================== WEARERS LOADING ==================
def load_wearer_ids(json_path: str) -> List[str]:
    """
    Parse Wearers.json and return list of wearer IDs as strings.
    Accepts:
    {
      "Wearers": {
        "someKey": {"id":"1", ...},
        "another": {"id": 2, ...}
      }
    }
    or a flat list: { "Wearers": [ {"id":"1"}, {"id":"2"} ] }
    """
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
                if wid:
                    ids.append(wid)
        elif isinstance(wearers, list):
            for wearer in wearers:
                wid = str((wearer or {}).get("id", "")).strip()
                if wid:
                    ids.append(wid)
        return ids
    except Exception:
        return []

# ================== MAIN LOOP ==================
if __name__ == "__main__":
    missing = [k for k, v in CHART_DIRS.items() if not v]
    if not WEARERS_JSON or missing:
        raise SystemExit(
            f"❌ Missing .env values. Need WEARERS_JSON and all CHART_DIRS. "
            f"Missing: {', '.join(['WEARERS_JSON' if not WEARERS_JSON else ''] + missing)}"
        )

    print("🚀 Smart Socks CSV generator (multi-user, minute cadence, live JSON discovery)")
    print(f"🧹 DELETE_ON_REMOVAL={DELETE_ON_REMOVAL} | DRY_RUN={DRY_RUN}")

    known_ids: Set[str] = set()

    # Initial scan & prep
    current_ids = set(load_wearer_ids(WEARERS_JSON))
    for wid in current_ids:
        for _, d in CHART_DIRS.items():
            ensure_file_with_header(d, wid)
    known_ids |= current_ids

    # Align to next :00
    now = datetime.now()
    next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    time.sleep((next_minute - now).total_seconds())

    tz = tz_offset_colon()
    last_json_check = time.monotonic()

    while True:
        now_floor = datetime.now().replace(second=0, microsecond=0)

        # Re-scan JSON (new or removed IDs)
        if time.monotonic() - last_json_check >= WEARERS_POLL_SEC:
            last_json_check = time.monotonic()
            ids = set(load_wearer_ids(WEARERS_JSON))

            # NEW: handle removed wearers
            removed_ids = known_ids - ids
            if removed_ids:
                print(f"🧹 wearers removed from JSON: {sorted(removed_ids)}")
                for wid in removed_ids:
                    delete_csvs_for_wearer(wid)

            # Handle newly added
            new_ids = ids - known_ids
            if new_ids:
                print(f"🔎 discovered new wearers: {sorted(new_ids)}")
                for wid in new_ids:
                    for _, d in CHART_DIRS.items():
                        ensure_file_with_header(d, wid)

            # Update known set to current JSON view
            known_ids = ids

        # For each wearer & chart: generate if not already written for this minute
        for wid in list(known_ids):
            for chart, d in CHART_DIRS.items():
                file_path = os.path.join(d, f"{wid}.csv")
                if not os.path.exists(file_path):
                    ensure_file_with_header(d, wid)

                last_dt = read_last_minute(file_path)
                if last_dt is not None and last_dt == now_floor:
                    continue

                val = GEN_MAP[chart](now_floor, wid)
                append_row(file_path, val, now_floor, tz)
                print(f"📝 {chart}/{wid}.csv -> {now_floor.strftime('%H:%M:%S')} = {val}")

        time.sleep(60)
