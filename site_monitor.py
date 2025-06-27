import firebase_admin
from firebase_admin import credentials, firestore
import random
import time
import logging
import os
import json
from datetime import datetime
import socket

# Constants
CONFIG_PATH = "/home/mumbamukendi/site-monitor/config.json"
CREDENTIALS_PATH = "/home/mumbamukendi/site-monitor/firebase-credentials/firebase.json"

# Setup logging
logging.basicConfig(
    filename='site_monitor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Globals
db = None
last_state = {
    "phase1": True,
    "phase2": True,
    "phase3": True,
    "dc_power": True
}
last_meter_sent = None

def read_site_id():
    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
            return config.get("site_id", "default_site")
    except Exception as e:
        logging.error(f"Failed to read config.json: {e}")
        return "default_site"

def initialize_firestore():
    global db
    try:
        # Cleanup previous app if any
        if firebase_admin._apps:
            firebase_admin.delete_app(firebase_admin.get_app())
    except Exception:
        pass

    cred = credentials.Certificate(CREDENTIALS_PATH)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    logging.info("Firestore initialized.")

def is_connected():
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=5)
        return True
    except OSError:
        return False

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
    return v >= 190

def detect_events(reading, site_id):
    global last_state
    events = []

    current_state = {
        "phase1": voltage_ok(reading['phase1_voltage']),
        "phase2": voltage_ok(reading['phase2_voltage']),
        "phase3": voltage_ok(reading['phase3_voltage']),
        "dc_power": reading['dc_power_status']
    }

    # Detect changes per phase and dc_power
    for key in current_state:
        if current_state[key] != last_state[key]:
            status_text = "restored" if current_state[key] else "dropped"
            event = {
                "site_id": site_id,
                "event_type": f"{key}_{status_text}",
                "details": {
                    "phase1": {
                        "voltage": reading['phase1_voltage'],
                        "status": "ok" if current_state["phase1"] else "off"
                    },
                    "phase2": {
                        "voltage": reading['phase2_voltage'],
                        "status": "ok" if current_state["phase2"] else "off"
                    },
                    "phase3": {
                        "voltage": reading['phase3_voltage'],
                        "status": "ok" if current_state["phase3"] else "off"
                    },
                    "dc_power": {
                        "status": "on" if current_state["dc_power"] else "off"
                    },
                    "meter_units": reading['meter_units']
                },
                "timestamp": datetime.utcnow()
            }
            events.append(event)
            last_state[key] = current_state[key]

    return events

def create_meter_event(reading, site_id):
    event = {
        "site_id": site_id,
        "event_type": "meter_reading",
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
        "timestamp": datetime.utcnow()
    }
    return event

def main():
    global last_meter_sent
    site_id = read_site_id()
    logging.info(f"Starting site monitor for site_id={site_id}")

    while True:
        try:
            if not is_connected():
                logging.warning("No internet connection. Retrying in 30 seconds...")
                time.sleep(30)
                continue

            # Initialize Firestore client
            initialize_firestore()

            # Find site document once per run loop
            sites = db.collection('sites')
            target_doc = sites.where("site_id", "==", site_id).limit(1).stream()
            site_doc = next(target_doc, None)

            if not site_doc:
                logging.error(f"Site with site_id={site_id} not found in Firestore")
                time.sleep(60)
                continue

            events_ref = sites.document(site_doc.id).collection("events")

            # Track last meter event time for 30 min interval
            last_meter_sent = None
            while True:
                try:
                    reading = simulate_readings()
                    events = detect_events(reading, site_id)

                    # Send power status change events
                    for event in events:
                        events_ref.add(event)
                        logging.info(f"Event logged: {event}")

                    # Send meter reading every 30 minutes regardless of events
                    now = time.time()
                    if (last_meter_sent is None) or (now - last_meter_sent > 1800):
                        meter_event = create_meter_event(reading, site_id)
                        events_ref.add(meter_event)
                        logging.info(f"Meter reading event logged: {meter_event}")
                        last_meter_sent = now

                    time.sleep(10)  # short sleep to reduce load, adjust as needed

                except Exception as e:
                    logging.error(f"Inner loop error: {e}")
                    time.sleep(10)
                    # Attempt to reconnect Firestore on inner errors
                    initialize_firestore()

        except Exception as outer_e:
            logging.error(f"Outer loop error: {outer_e}")
            time.sleep(30)  # wait before retrying whole process

if __name__ == "__main__":
    main()
