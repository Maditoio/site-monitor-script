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
last_meter_report = datetime.utcnow() - timedelta(minutes=30)

# === Simulation Function ===
def simulate_readings():
    return {
        'phase1_voltage': round(random.uniform(0, 240), 2),
        'phase2_voltage': round(random.uniform(0, 240), 2),
        'phase3_voltage': round(random.uniform(0, 240), 2),
        'dc_battery_level': round(random.uniform(10, 15), 2),
        'meter_units': round(random.uniform(1000, 2000), 2),
        'dc_power_status': random.choice([True, False]),
        'timestamp': firestore.SERVER_TIMESTAMP
    }

def voltage_ok(v):
    return v > 190

def detect_power_event(reading):
    global last_state
    events = []

    state_now = {
        "phase1": voltage_ok(reading['phase1_voltage']),
        "phase2": voltage_ok(reading['phase2_voltage']),
        "phase3": voltage_ok(reading['phase3_voltage']),
        "dc_power": reading['dc_power_status']
    }

    for key in state_now:
        if state_now[key] != last_state[key]:
            status = "restored" if state_now[key] else "dropped"
            events.append({
                "site_id": SITE_ID,
                "event_type": f"{key}_status_change",
                "details": {
                    "phase1": {"voltage": reading["phase1_voltage"], "status": "ok" if state_now["phase1"] else "off"},
                    "phase2": {"voltage": reading["phase2_voltage"], "status": "ok" if state_now["phase2"] else "off"},
                    "phase3": {"voltage": reading["phase3_voltage"], "status": "ok" if state_now["phase3"] else "off"},
                    "dc_power": {"status": "on" if state_now["dc_power"] else "off"},
                    "meter_units": reading["meter_units"]
                },
                "timestamp": datetime.utcnow()
            })
            last_state[key] = state_now[key]

    return events

def meter_event_due():
    global last_meter_report
    now = datetime.utcnow()
    if (now - last_meter_report).total_seconds() >= 1800:
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

def main():
    global db, site_ref

    while True:
        try:
            reading = simulate_readings()

            # Update real-time status
            readings_ref.document("latest").set(reading)

            # Detect and push events
            events = detect_power_event(reading)
            if meter_event_due():
                events.append(create_meter_event(reading))

            for event in events:
                events_ref.add(event)
                logging.info(f"Event sent: {event}")

            time.sleep(random.randint(10, 20))

        except Exception as e:
            logging.error(f"Error: {e}")
            error_str = str(e).lower()
            if "failed to connect" in error_str or "503" in error_str or "timeout" in error_str:
                logging.critical("Connection error — restarting script...")
                time.sleep(5)
                os.execv(sys.executable, [sys.executable] + sys.argv)
            time.sleep(10)

if __name__ == "__main__":
    main()
