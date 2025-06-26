import firebase_admin
from firebase_admin import credentials, firestore
import random
import time
import logging
import os
from datetime import datetime

# This is a new version of the code
# Set up logging
logging.basicConfig(
    filename='site_monitor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Get site_id from environment or fallback
SITE_ID = os.getenv('SITE_ID', 'default_site')

# Initialize Firebase
try:
    cred = credentials.Certificate('/home/mumbamukendi/site-monitor/firebase-credentials/firebase.json')
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    logging.info(f"Firestore initialized for site {SITE_ID}")
except Exception as e:
    logging.error(f"Failed to initialize Firestore: {e}")
    exit(1)

# Last known states
last_state = {
    "phase1": True,
    "phase2": True,
    "phase3": True,
    "dc_power": True
}

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

def detect_events(reading):
    global last_state
    events = []

    # Determine states
    current_state = {
        "phase1": voltage_ok(reading['phase1_voltage']),
        "phase2": voltage_ok(reading['phase2_voltage']),
        "phase3": voltage_ok(reading['phase3_voltage']),
        "dc_power": reading['dc_power_status']
    }

    # Compare with last state
    for key in current_state:
        if current_state[key] != last_state[key]:
            state_text = "restored" if current_state[key] else "dropped"
            event = {
                "site_id": SITE_ID,
                "event_type": f"{key}_{state_text}",
                "details": f"{key.replace('_', ' ').title()} {state_text}",
                "timestamp": datetime.utcnow()
            }
            events.append(event)
            last_state[key] = current_state[key]

    return events

def main():
    # Find site document
    sites = db.collection('sites')
    target_doc = sites.where("site_id", "==", SITE_ID).limit(1).stream()
    site_doc = next(target_doc, None)

    if not site_doc:
        logging.error(f"Site with site_id={SITE_ID} not found")
        print(f"Site with site_id={SITE_ID} not found")
        return

    # References
    events_ref = sites.document(site_doc.id).collection("events")

    while True:
        try:
            reading = simulate_readings()
            events = detect_events(reading)

            for event in events:
                events_ref.add(event)
                logging.info(f"Event logged: {event}")
                print(f"Event logged: {event}")

            time.sleep(random.randint(50, 60))

        except Exception as e:
            logging.error(f"Error: {e}")
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()

