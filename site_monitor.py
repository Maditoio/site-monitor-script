import firebase_admin
from firebase_admin import credentials, firestore
import random
import time
import logging
import os
import sys
import json
from datetime import datetime, timedelta

# === Logging Setup ===
logging.basicConfig(
    filename='/home/mumbamukendi/site-monitor/site_monitor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# === Configuration Path ===
CONFIG_PATH = '/home/mumbamukendi/site-monitor/config.json'
CREDENTIALS_PATH = '/home/mumbamukendi/site-monitor/firebase-credentials/firebase.json'
STATE_FILE = '/home/mumbamukendi/site-monitor/state.json'

# === Read Site ID from config.json ===
try:
    with open(CONFIG_PATH) as f:
        config = json.load(f)
        SITE_ID = config.get("site_id", "default_site")
except Exception as e:
    logging.error(f"Failed to read config.json: {e}")
    SITE_ID = "default_site"

# === Firebase Initialization ===
def init_firestore():
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(CREDENTIALS_PATH)
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        logging.critical(f"Failed to initialize Firebase: {e}")
        return None

db = init_firestore()
if not db:
    sys.exit(1)

# === Site References ===
def get_site_ref():
    try:
        sites = db.collection('sites')
        query = sites.where("site_id", "==", SITE_ID).limit(1).stream()
        site_doc = next(query, None)
        if site_doc:
            return sites.document(site_doc.id)
        else:
            logging.error(f"Site with site_id={SITE_ID} not found")
            return None
    except Exception as e:
        logging.error(f"Failed to get site reference: {e}")
        return None

site_ref = get_site_ref()
if not site_ref:
    sys.exit(1)

events_ref = site_ref.collection("events")
readings_ref = site_ref.collection("latest")

# === State Tracking ===
last_state = {
    "phase1": True,
    "phase2": True,
    "phase3": True,
    "dc_power": True
}
last_event_time = {
    "phase1": datetime.utcnow() - timedelta(minutes=1),
    "phase2": datetime.utcnow() - timedelta(minutes=1),
    "phase3": datetime.utcnow() - timedelta(minutes=1),
    "dc_power": datetime.utcnow() - timedelta(minutes=1)
}
last_meter_report = datetime.utcnow() - timedelta(minutes=30)
last_meter_units = 1000

# === Load State ===
def load_state():
    global last_state, last_meter_report, last_meter_units, last_event_time
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            last_state = state.get('last_state', last_state)
            last_meter_report = datetime.fromisoformat(state.get('last_meter_report', (datetime.utcnow() - timedelta(minutes=30)).isoformat()))
            last_meter_units = state.get('last_meter_units', 1000)
            last_event_time = {k: datetime.fromisoformat(v) for k, v in state.get('last_event_time', last_event_time).items()}
        logging.info("State loaded from file")
    except FileNotFoundError:
        logging.info("No state file found, using default state")
    except Exception as e:
        logging.error(f"Failed to load state: {e}")

load_state()

# === Save State ===
def save_state():
    state = {
        'last_state': last_state,
        'last_meter_report': last_meter_report.isoformat(),
        'last_meter_units': last_meter_units,
        'last_event_time': {k: v.isoformat() for k, v in last_event_time.items()}
    }
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
        logging.info("State saved to file")
    except Exception as e:
        logging.error(f"Failed to save state: {e}")

# === Simulation Function ===
def simulate_readings():
    def get_voltage():
        if random.random() < 0.95:
            return round(random.uniform(220, 240), 2)
        return 0.0

    return {
        'phase1_voltage': get_voltage(),
        'phase2_voltage': get_voltage(),
        'phase3_voltage': get_voltage(),
        'dc_battery_level': round(random.uniform(12, 15), 2),
        'meter_units': round(random.uniform(1000, 2000), 2),
        'dc_power_status': random.random() < 0.9,
        'timestamp': firestore.SERVER_TIMESTAMP
    }

def voltage_ok(v):
    return v > 190

def detect_power_event(reading):
    global last_state, last_event_time
    state_now = {
        "phase1": voltage_ok(reading['phase1_voltage']),
        "phase2": voltage_ok(reading['phase2_voltage']),
        "phase3": voltage_ok(reading['phase3_voltage']),
        "dc_power": reading['dc_power_status']
    }

    changed_states = []
    now = datetime.utcnow()
    for key in state_now:
        if state_now[key] != last_state[key] and (now - last_event_time[key]).total_seconds() >= 60:
            status = "restored" if state_now[key] else "dropped"
            changed_states.append(f"{key}_status_{status}")
            last_state[key] = state_now[key]
            last_event_time[key] = now

    if changed_states:
        return [{
            "site_id": SITE_ID,
            "event_type": "power_status_update",
            "details": {
                "phase1": {"voltage": reading["phase1_voltage"], "status": "ok" if state_now["phase1"] else "off"},
                "phase2": {"voltage": reading["phase2_voltage"], "status": "ok" if state_now["phase2"] else "off"},
                "phase3": {"voltage": reading["phase3_voltage"], "status": "ok" if state_now["phase3"] else "off"},
                "dc_power": {"status": "on" if state_now["dc_power"] else "off"},
                "meter_units": reading["meter_units"],
                "changed_states": changed_states
            },
            "timestamp": datetime.utcnow()
        }]
    return []

def meter_event_due(reading, last_meter_units):
    global last_meter_report
    now = datetime.utcnow()
    if (now - last_meter_report).total_seconds() >= 1800:
        if abs(reading["meter_units"] - last_meter_units) > 10:
            last_meter_report = now
            return True
    return False

def create_meter_event(reading):
    return {
        "site_id": SITE_ID,
        "event_type": "meter_update",
        "details": {
            "phase1": {"voltage": reading["phase1_voltage"], "status": "ok" if voltage_ok(reading["phase1_voltage"]) else "off"},
            "phase2": {"voltage": reading["phase2_voltage"], "status": "ok" if voltage_ok(reading["phase2_voltage"]) else "off"},
            "phase3": {"voltage": reading["phase3_voltage"], "status": "ok" if voltage_ok(reading["phase3_voltage"]) else "off"},
            "dc_power": {"status": "on" if reading["dc_power_status"] else "off"},
            "meter_units": reading["meter_units"]
        },
        "timestamp": datetime.utcnow()
    }

def attempt_reconnect(max_retries=3, retry_delay=5):
    for attempt in range(max_retries):
        try:
            db.collection('sites').limit(1).get()
            logging.info(f"Reconnected to Firestore on attempt {attempt + 1}")
            return True
        except Exception as e:
            logging.warning(f"Reconnection attempt {attempt + 1}/{max_retries} failed: {e}")
            time.sleep(retry_delay)
    logging.critical("All reconnection attempts failed")
    return False

def main():
    global db, site_ref, last_meter_units
    while True:
        try:
            reading = simulate_readings()
            readings_ref.document("latest").set(reading)
            batch = db.batch()
            events = detect_power_event(reading)
            if meter_event_due(reading, last_meter_units):
                if events:
                    events[0]["event_type"] = "combined_power_meter_update"
                    events[0]["details"]["meter_update"] = True
                else:
                    events.append(create_meter_event(reading))
                last_meter_units = reading["meter_units"]
            for event in events:
                batch.set(events_ref.document(), event)
                logging.info(f"Event queued: {event}")
            if events:
                batch.commit()
                logging.info(f"Batch committed with {len(events)} events")
                save_state()
            time.sleep(random.randint(10, 20))
        except Exception as e:
            logging.error(f"Error: {e}")
            error_str = str(e).lower()
            if "failed to connect" in error_str or "503" in error_str or "timeout" in error_str:
                logging.critical("Connection error — attempting reconnect...")
                if attempt_reconnect():
                    continue
                logging.critical("Reconnection failed — restarting script...")
                time.sleep(5)
                os.execv(sys.executable, [sys.executable] + sys.argv)
            time.sleep(10)

if __name__ == "__main__":
    main()
