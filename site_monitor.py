# v2.1 - Reliable AC/DC Event + Meter Updates
import firebase_admin
from firebase_admin import credentials, firestore
import random
import time
import logging
import os
from datetime import datetime
import json

# Logging setup
logging.basicConfig(
    filename='site_monitor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load SITE_ID from config
try:
    with open("/home/mumbamukendi/site-monitor/config.json") as f:
        config = json.load(f)
        SITE_ID = config.get("site_id", "default_site")
except Exception as e:
    logging.error(f"Failed to read config.json: {e}")
    SITE_ID = "default_site"

# Firebase init
try:
    cred = credentials.Certificate('/home/mumbamukendi/site-monitor/firebase-credentials/firebase.json')
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    logging.info(f"Firestore initialized for site {SITE_ID}")
except Exception as e:
    logging.error(f"Failed to initialize Firestore: {e}")
    exit(1)

# State tracking
last_state = {
    "phase1": True,
    "phase2": True,
    "phase3": True,
    "dc_power": True
}
last_meter_push = 0
METER_PUSH_INTERVAL = 1800  # 30 minutes

def simulate_readings():
    return {
        'phase1_voltage': round(random.uniform(0, 240), 2),
        'phase2_voltage': round(random.uniform(0, 240), 2),
        'phase3_voltage': round(random.uniform(0, 240), 2),
        'dc_battery_level': round(random.uniform(10, 15), 2),
        'meter_units': round(random.uniform(1000, 2000), 2),
        'dc_power_status': random.choice([True, False]),
        'timestamp': datetime.utcnow()
    }

def voltage_ok(v):
    return v > 190  # AC considered off at 190 and below

def build_event(reading, event_type):
    return {
        "site_id": SITE_ID,
        "event_type": event_type,
        "details": {
            "phase1": {
                "voltage": reading['phase1_voltage'],
                "status": "ok" if voltage_ok(reading['phase1_voltage']) else "off"
            },
            "phase2": {
                "voltage": reading['phase2_voltage'],
                "status": "ok" if voltage_ok(reading['phase2_voltage']) else "off"
            },
            "phase3": {
                "voltage": reading['phase3_voltage'],
                "status": "ok" if voltage_ok(reading['phase3_voltage']) else "off"
            },
            "dc_power": {
                "status": "on" if reading['dc_power_status'] else "off"
            },
            "meter_units": reading['meter_units']
        },
        "timestamp": reading['timestamp']
    }

def detect_changes(reading):
    global last_state
    changes_detected = False

    current_state = {
        "phase1": voltage_ok(reading['phase1_voltage']),
        "phase2": voltage_ok(reading['phase2_voltage']),
        "phase3": voltage_ok(reading['phase3_voltage']),
        "dc_power": reading['dc_power_status']
    }

    if current_state != last_state:
        changes_detected = True
        last_state = current_state.copy()

    return changes_detected

def main():
    global last_meter_push

    # Reference to Firestore site
    sites = db.collection('sites')
    target_doc = sites.where("site_id", "==", SITE_ID).limit(1).stream()
    site_doc = next(target_doc, None)

    if not site_doc:
        logging.error(f"Site with site_id={SITE_ID} not found")
        print(f"Site with site_id={SITE_ID} not found")
        return

    events_ref = sites.document(site_doc.id).collection("events")

    while True:
        try:
            reading = simulate_readings()
            now = time.time()

            # Always send meter reading every 30 minutes
            if now - last_meter_push > METER_PUSH_INTERVAL:
                meter_event = build_event(reading, "meter_update")
                events_ref.add(meter_event)
                logging.info(f"Meter event logged: {meter_event}")
                print(f"Meter event logged: {meter_event}")
                last_meter_push = now

            # Send power status if changed
            if detect_changes(reading):
                power_event = build_event(reading, "power_status_change")
                events_ref.add(power_event)
                logging.info(f"Power event logged: {power_event}")
                print(f"Power event logged: {power_event}")

            time.sleep(random.randint(50, 60))

        except Exception as e:
            logging.error(f"Error: {e}")
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
