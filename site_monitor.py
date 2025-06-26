import firebase_admin
from firebase_admin import credentials, firestore
import random
import time
import logging
import os
import json

# Set up logging
logging.basicConfig(
    filename='/home/mumbamukendi/site-monitor/site_monitor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load site_id from config.json
CONFIG_FILE = "/home/mumbamukendi/site-monitor/config.json"

try:
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
        SITE_ID = config.get("site_id", "default_site")
        logging.info(f"Loaded site_id from config: {SITE_ID}")
except Exception as e:
    SITE_ID = "default_site"
    logging.error(f"Failed to load config.json: {e}")

# Initialize Firebase
try:
    cred = credentials.Certificate('/home/mumbamukendi/site-monitor/firebase-credentials/firebase.json')
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    logging.info("Firestore initialized successfully")
except Exception as e:
    logging.error(f"Failed to initialize Firestore: {e}")
    exit(1)

def simulate_readings():
    return {
        'phase1_voltage': round(random.uniform(200, 240), 2),
        'phase2_voltage': round(random.uniform(200, 240), 2),
        'phase3_voltage': round(random.uniform(200, 240), 2),
        'dc_battery_level': round(random.uniform(10, 15), 2),
        'meter_units': round(random.uniform(1000, 2000), 2),
        'dc_power_status': random.choice([True, False]),
        'timestamp': firestore.SERVER_TIMESTAMP
    }

def main():
    sites = db.collection('sites')
    target_doc = sites.where("site_id", "==", SITE_ID).limit(1).stream()
    site_doc = next(target_doc, None)

    if not site_doc:
        logging.error(f"Site with site_id={SITE_ID} not found")
        print(f"❌ Site with site_id={SITE_ID} not found")
        return

    site_ref = sites.document(site_doc.id).collection("readings")

    while True:
        try:
            reading = simulate_readings()
            site_ref.add(reading)
            logging.info(f"✅ Sent data for {SITE_ID}: {reading}")
            print(f"✅ Sent data for {SITE_ID}: {reading}")
            time.sleep(random.randint(5, 30))
        except Exception as e:
            logging.error(f"Error sending reading: {e}")
            print(f"❌ Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()

