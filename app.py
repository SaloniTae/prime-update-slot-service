import os
import requests
from datetime import datetime, timedelta
import pytz

from flask import Flask, request, jsonify

app = Flask(__name__)

# --------------------------------------------------------------------
# Environment variables set in your hosting (Render, etc.)
# e.g. REAL_DB_URL="https://mydb.firebaseio.com/"
#      PROXY_SECRET="some_very_secret_token"
# --------------------------------------------------------------------
REAL_DB_URL  = os.getenv("REAL_DB_URL", "")
PROXY_SECRET = os.getenv("PROXY_SECRET", "")

ist = pytz.timezone("Asia/Kolkata")

def parse_ist(dt_str: str):
    """Parse a string 'YYYY-MM-DD HH:MM:SS' as IST-aware datetime."""
    naive = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return ist.localize(naive)

def format_ist(dt_aware: datetime) -> str:
    """Format an IST-aware datetime as 'YYYY-MM-DD HH:MM:SS' string."""
    return dt_aware.strftime("%Y-%m-%d %H:%M:%S")

def is_credential(node):
    """
    Return True if 'node' is shaped like a credential, i.e.:
      {
        "email": "...",
        "password": "...",
        "expiry_date": "...",
        "locked": 0 or 1 or 2,
        "usage_count": N,
        "max_usage": M,
        "belongs_to_slot": "slot_1" or "slot_2" or "slot_3", etc.
      }
    """
    if not isinstance(node, dict):
        return False
    required = [
        "email","password","expiry_date",
        "locked","usage_count","max_usage",
        "belongs_to_slot"
    ]
    return all(r in node for r in required)

# --------------------------------------------------------------------
# Update all slots' start/end if 24h have passed since last_update:
#  - If frequency="daily", shift +1 day
#  - If frequency="3day", shift +3 days
#  - Then lock credentials if desired.
# --------------------------------------------------------------------
def update_slot_times_multi():
    now_ist = datetime.now(ist)

    # 1) Read settings from DB
    settings_resp = requests.get(REAL_DB_URL + "settings.json")
    if settings_resp.status_code != 200 or not settings_resp.json():
        print("No settings found or request error.")
        return
    settings_data = settings_resp.json()

    # 2) If no "slots" sub-node, fallback to single-slot or do nothing
    all_slots = settings_data.get("slots")
    if not isinstance(all_slots, dict):
        print("No multi-slot node => fallback single-slot or skip.")
        return

    any_slot_shifted = False
    
    # *** NEW: Reset Account Claims After Update ***
    print("Calling reset_account_claims() after slot update.")
    reset_account_claims()


    # 3) For each slot_{n}, if 24h+ since last_update => SHIFT
    for slot_id, slot_info in all_slots.items():
        if not isinstance(slot_info, dict):
            continue

        enabled = bool(slot_info.get("enabled", False))
        if not enabled:
            # skip disabled slot
            continue

        last_update_str = slot_info.get("last_update", "")
        if last_update_str:
            try:
                last_update_dt = parse_ist(last_update_str)
            except ValueError:
                last_update_dt = now_ist
        else:
            last_update_dt = now_ist

        delta = now_ist - last_update_dt
        if delta < timedelta(hours=24):
            print(f"[{slot_id}] Only {delta} since last update => skip SHIFT.")
            continue

        print(f"[{slot_id}] SHIFT: 24h+ since last update => shifting times.")

        slot_start_str = slot_info.get("slot_start","9999-12-31 09:00:00")
        slot_end_str   = slot_info.get("slot_end",  "9999-12-31 09:00:00")

        # parse current times
        try:
            slot_start_dt = parse_ist(slot_start_str)
        except ValueError:
            slot_start_dt = now_ist.replace(hour=9, minute=0, second=0, microsecond=0)

        try:
            slot_end_dt = parse_ist(slot_end_str)
        except ValueError:
            slot_end_dt = slot_start_dt + timedelta(days=1)

        freq = slot_info.get("frequency","daily").lower()
        if freq == "3day":
            shift_delta = timedelta(days=3)
        elif freq == "weekly":
            shift_delta = timedelta(days=7)
        else:
            shift_delta = timedelta(days=1)  # default daily

        new_start = slot_start_dt + shift_delta
        new_end   = slot_end_dt   + shift_delta

        slot_info["slot_start"]  = format_ist(new_start)
        slot_info["slot_end"]    = format_ist(new_end)
        slot_info["last_update"] = format_ist(now_ist)

        any_slot_shifted = True

    # 4) If we changed anything, patch back & optionally lock
    if any_slot_shifted:
        patch_resp = requests.patch(REAL_DB_URL + "settings.json", json={"slots": all_slots})
        if patch_resp.status_code == 200:
            print("Multi-slot SHIFT success => now lock if needed.")
            lock_by_slot()

          



        
        else:
            print("Failed to patch updated slots =>", patch_resp.text)
    else:
        print("No slot was shifted => no changes made.")


# --------------------------------------------------------------------
# Lock only credentials that belong to a slot whose end is within 2 min
# i.e. now >= slot_end_dt - 2min => lock belongs_to_slot that matches
# --------------------------------------------------------------------
def lock_by_slot():
    now_ist = datetime.now(ist)

    # read settings => see which slots are enabled
    settings_resp = requests.get(REAL_DB_URL + "settings.json")
    if settings_resp.status_code != 200 or not settings_resp.json():
        print("No settings => skip lock.")
        return
    settings_data = settings_resp.json()
    all_slots = settings_data.get("slots", {})

    # fetch entire DB for credentials
    db_resp = requests.get(REAL_DB_URL + ".json")
    if db_resp.status_code != 200 or not db_resp.json():
        print("No DB data => skip lock.")
        return
    db_data = db_resp.json()

    margin = timedelta(minutes=2)
    locked_count_total = 0

    for slot_id, slot_info in all_slots.items():
        if not isinstance(slot_info, dict):
            continue
        if not slot_info.get("enabled", False):
            continue

        slot_end_str = slot_info.get("slot_end","9999-12-31 09:00:00")
        try:
            slot_end_dt = parse_ist(slot_end_str)
        except ValueError:
            continue

        # if now >= slot_end_dt - margin => lock belongs_to_slot
        if now_ist >= (slot_end_dt - margin):
            # lock only creds that belongs_to_slot == slot_id & locked=0
            for cred_key, cred_data in db_data.items():
                if not is_credential(cred_data):
                    continue
                if cred_data.get("belongs_to_slot","") != slot_id:
                    # skip different slot
                    continue

                locked_val = int(cred_data.get("locked",0))
                if locked_val == 0:
                    patch_url  = REAL_DB_URL + f"/{cred_key}.json"
                    patch_data = {"locked":1}
                    p = requests.patch(patch_url, json=patch_data)
                    if p.status_code == 200:
                        locked_count_total += 1

    print(f"Locked {locked_count_total} credentials in total.")


# -------------------------------------------------------
# Endpoints to trigger SHIFT or LOCK from Cron-Job.org
# -------------------------------------------------------
@app.route("/update_slot")
def update_slot():
    update_slot_times_multi()
    return "Slot times updated!\n", 200

@app.route("/lock_check")
def lock_check():
    lock_by_slot()
    return "Lock check done.\n", 200


# -------------------------------------------------------
# Endpoint to reset account claims (for new slot windows)
# -------------------------------------------------------
def write_data_via_proxy(data):
    headers = {"X-Secret": PROXY_SECRET}
    try:
        resp = requests.put(f"{REAL_DB_URL}.json", json=data, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"Proxy write error: {resp.text}")
            return {}
    except Exception as e:
        print(f"write_data_via_proxy exception: {e}")
        return {}

def read_data_via_proxy():
    headers = {"X-Secret": PROXY_SECRET}
    try:
        print(f"Connecting to DB proxy at: {REAL_DB_URL}.json")
        resp = requests.get(f"{REAL_DB_URL}.json", headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json() or {}
            print("Successfully connected to DB via proxy.")
            
            return data
        else:
            print(f"Proxy read error: {resp.text}")
            return {}
    except Exception as e:
        print(f"read_data_via_proxy exception: {e}")
        return {}

def reset_account_claims():
    # Read the full DB data via proxy.
    db_data = read_data_via_proxy()
    if not db_data:
        print("No DB data available for reset.")
        return

    # Get slot settings.
    slots = db_data.get("settings", {}).get("slots", {})
    if not slots:
        print("No slot settings found.")
        return

    now_ist = datetime.now(ist)

    account_claims = db_data.get("account_claims", {})
    claims_updated = False

    # For each slot in settings, check and clear claims if needed.
    for slot_id, slot_info in slots.items():
        slot_end_str = slot_info.get("slot_end", "")
        if not slot_end_str:
            continue
        try:
            slot_end = parse_ist(slot_end_str)  # Use parse_ist for IST timezone parsing
        except Exception as e:
            print(f"Error parsing slot_end for {slot_id}: {e}")
            continue

        if now_ist > slot_end:
            for user_id, claims in account_claims.items():
                if slot_id in claims:
                    print(f"Clearing account claim for user {user_id} in slot {slot_id}.")
                    del claims[slot_id]
                    claims_updated = True

    if claims_updated:
        db_data["account_claims"] = account_claims
        result = write_data_via_proxy(db_data)
        print("Account claims reset updated in DB.")
    else:
        print("No account claims needed resetting.")


# -------------------------------------------------------
# Proxy routes to hide the real DB URL
# -------------------------------------------------------
@app.route("/getData", methods=["GET"])
def get_data():
    token = request.headers.get("X-Secret")
    if token != PROXY_SECRET:
        return jsonify({"error":"Unauthorized"}),403

    url = REAL_DB_URL + ".json"
    resp = requests.get(url)
    if resp.status_code != 200:
        return jsonify({"error":"Failed to read DB"}),500

    return jsonify(resp.json())

@app.route("/setData", methods=["POST"])
def set_data():
    token = request.headers.get("X-Secret")
    if token != PROXY_SECRET:
        return jsonify({"error":"Unauthorized"}),403

    data = request.get_json()
    url = REAL_DB_URL + ".json"
    resp = requests.put(url, json=data)
    if resp.status_code != 200:
        return jsonify({"error":"Failed to write DB"}),500

    return jsonify({"status":"ok","resp":resp.text})



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
