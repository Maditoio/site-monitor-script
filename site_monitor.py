import firebase_admin
from firebase_admin import credentials, firestore
import RPi.GPIO as GPIO
import time
import logging
import os
import sys
import json
from datetime import datetime, timedelta

# === GPIO Setup ===
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

# GPIO Pin definitions
AC_POWER_PIN = 17  # GPIO17 (Pin 11)
DC_POWER_PIN = 27  # GPIO27 (Pin 13)
DOOR_SENSOR_PIN = 23  # GPIO22 (Pin 16)

# Setup GPIO pins as inputs with pull-down resistors
GPIO.setup(AC_POWER_PIN, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)
GPIO.setup(DC_POWER_PIN, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)
GPIO.setup(DOOR_SENSOR_PIN, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)

# === Logging Setup ===
logging.basicConfig(
    filename='/home/mumbamukendi/site-monitor/site_monitor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# === Configuration Paths ===
CONFIG_PATH = '/home/mumbamukendi/site-monitor/config.json'
CREDENTIALS_PATH = '/home/mumbamukendi/site-monitor/firebase-credentials/firebase.json'
STATE_FILE = '/home/mumbamukendi/site-monitor/state.json'

# === Load SITE_ID from config.json ===
try:
    with open(CONFIG_PATH) as f:
        config = json.load(f)
        SITE_ID = config.get("site_id", "default_site")
except Exception as e:
    logging.error(f"Failed to read config.json: {e}")
    SITE_ID = "default_site"

# === Initialize Firebase Admin SDK and Firestore ===
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

# === Get Firestore Document Reference for the Site ===
def get_site_ref():
    try:
        sites = db.collection('sites')
        # Query sites collection to find document where site_id == SITE_ID
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

# === References for Firestore collections ===

# Flat top-level collection to store latest power status per site
latest_status_ref = db.collection('latest_status').document(SITE_ID)

# Root-level 'events' collection, each site has a document
# Inside each site document, 'event_docs' subcollection holds individual event documents
events_root_ref = db.collection('events')
events_ref = events_root_ref.document(SITE_ID).collection('event_docs')

# === Fetch site_name once for denormalization ===
def get_site_name(site_ref):
    try:
        doc = site_ref.get()
        if doc.exists:
            return doc.to_dict().get('site_name', 'Unknown')
    except Exception as e:
        logging.error(f"Failed to fetch site name: {e}")
    return 'Unknown'

site_name = get_site_name(site_ref)

# === State tracking variables for detecting changes ===
last_state = {
    "ac_power": False,
    "dc_power": False,
    "door_sensor": False
}
last_event_time = {
    "ac_power": datetime.utcnow() - timedelta(minutes=1),
    "dc_power": datetime.utcnow() - timedelta(minutes=1),
    "door_sensor": datetime.utcnow() - timedelta(minutes=1)
}

# === Load persisted state from file ===
def load_state():
    global last_state, last_event_time
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            
            # Safely load last_state with defaults
            saved_state = state.get('last_state', {})
            for key in last_state:
                if key in saved_state:
                    last_state[key] = saved_state[key]
            
            # Safely load last_event_time with defaults
            last_event_time_data = state.get('last_event_time', {})
            for key in last_event_time:
                if key in last_event_time_data:
                    try:
                        last_event_time[key] = datetime.fromisoformat(last_event_time_data[key])
                    except ValueError:
                        logging.warning(f"Invalid datetime format for {key}, using default")
                        
        logging.info("State loaded from file")
    except FileNotFoundError:
        logging.info("No state file found, using default state")
    except Exception as e:
        logging.error(f"Failed to load state: {e}")
        logging.info("Using default state values")

load_state()

# === Save current state to file ===
def save_state():
    state = {
        'last_state': last_state,
        'last_event_time': {k: v.isoformat() for k, v in last_event_time.items()}
    }
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
        logging.info("State saved to file")
    except Exception as e:
        logging.error(f"Failed to save state: {e}")

# === Read GPIO pins for power and door status ===
def read_gpio_status():
    try:
        ac_power_status = GPIO.input(AC_POWER_PIN) == GPIO.HIGH
        dc_power_status = GPIO.input(DC_POWER_PIN) == GPIO.HIGH
        door_sensor_status = GPIO.input(DOOR_SENSOR_PIN) == GPIO.HIGH  # HIGH = door open
        
        return {
            'ac_power_status': ac_power_status,
            'dc_power_status': dc_power_status,
            'door_sensor_status': door_sensor_status,
            'updated_at': firestore.SERVER_TIMESTAMP
        }
    except Exception as e:
        logging.error(f"Error reading GPIO pins: {e}")
        return {
            'ac_power_status': False,
            'dc_power_status': False,
            'door_sensor_status': False,
            'updated_at': firestore.SERVER_TIMESTAMP
        }

# === Detect power/door state changes for events generation ===
def detect_status_events(reading):
    global last_state, last_event_time
    state_now = {
        "ac_power": reading['ac_power_status'],
        "dc_power": reading['dc_power_status'],
        "door_sensor": reading['door_sensor_status']
    }

    events = []
    now = datetime.utcnow()
    
    for key in state_now:
        # Ensure the key exists in last_state and last_event_time
        if key not in last_state:
            last_state[key] = False
        if key not in last_event_time:
            last_event_time[key] = datetime.utcnow() - timedelta(minutes=1)
            
        # If status changed AND last change was at least 10 seconds ago, record event
        if state_now[key] != last_state[key] and (now - last_event_time[key]).total_seconds() >= 10:
            if key == "door_sensor":
                status = "opened" if state_now[key] else "closed"
                previous_status = "opened" if last_state[key] else "closed"
                event_type = "door_status_change"
            else:
                status = "on" if state_now[key] else "off"
                previous_status = "on" if last_state[key] else "off"
                event_type = "power_status_change"
            
            events.append({
                "site_id": SITE_ID,
                "event_type": event_type,
                "details": {
                    "component": key,
                    "status": status,
                    "previous_status": previous_status,
                    "ac_power_status": state_now["ac_power"],
                    "dc_power_status": state_now["dc_power"],
                    "door_sensor_status": state_now["door_sensor"]
                },
                "timestamp": datetime.utcnow()
            })
            
            last_state[key] = state_now[key]
            last_event_time[key] = now
            logging.info(f"{key} changed to {status}")

    return events

# === Create a heartbeat event document ===
def create_heartbeat_event(reading):
    return {
        "site_id": SITE_ID,
        "event_type": "heartbeat",
        "details": {
            "ac_power_status": reading["ac_power_status"],
            "dc_power_status": reading["dc_power_status"],
            "door_sensor_status": reading["door_sensor_status"],
            "message": "Regular status update - system operational"
        },
        "timestamp": datetime.utcnow()
    }

# === Attempt to reconnect Firestore if connection issues occur ===
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

# === Cleanup GPIO on exit ===
def cleanup():
    GPIO.cleanup()
    logging.info("GPIO cleanup completed")

# === Main loop: read GPIO pins, write latest status, handle events ===
def main():
    global db, site_ref
    last_heartbeat = datetime.utcnow() - timedelta(minutes=3)  # Force heartbeat on start
    
    try:
        while True:
            try:
                # Read GPIO status
                reading = read_gpio_status()
                now = datetime.utcnow()
                time_since_heartbeat = (now - last_heartbeat).total_seconds()

                # Detect status change events
                events = detect_status_events(reading)

                # Check if heartbeat is due (every 2 minutes)
                heartbeat_due = time_since_heartbeat >= 120

                # Send heartbeat every 2 minutes even if no events
                if heartbeat_due:
                    if not events:  # Only add heartbeat if no other events
                        events.append(create_heartbeat_event(reading))
                    last_heartbeat = now
                    logging.info(f"Heartbeat sent at {now.isoformat()}")

                # ONLY update Firestore under these 3 conditions:
                # 1. Power status changed (AC or DC)
                # 2. Door sensor changed
                # 3. Heartbeat is due (every 2 minutes)
                if events or heartbeat_due:
                    # Prepare latest status document for flat root-level collection
                    latest_doc = {
                        **reading,
                        'site_id': SITE_ID,
                        'site_name': site_name,  # denormalized for UI convenience
                    }

                    # Update latest status
                    latest_status_ref.set(latest_doc)

                    # If there are events, write them to Firestore
                    if events:
                        batch = db.batch()
                        for event in events:
                            batch.set(events_ref.document(), event)
                        batch.commit()
                        logging.info(f"Events batch committed with {len(events)} events")

                    # Persist current state to file
                    save_state()

                # Wait 5 seconds before next reading
                time.sleep(5)

            except Exception as e:
                logging.error(f"Error: {e}")
                error_str = str(e).lower()
                # Handle Firestore connectivity issues with retry logic
                if "failed to connect" in error_str or "503" in error_str or "timeout" in error_str:
                    logging.critical("Connection error — attempting reconnect...")
                    if attempt_reconnect():
                        continue
                    logging.critical("Reconnection failed — restarting script...")
                    time.sleep(5)
                    cleanup()
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                # Generic delay before retry on error
                time.sleep(10)

    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
        cleanup()
        sys.exit(0)
    except Exception as e:
        logging.critical(f"Fatal error: {e}")
        cleanup()
        sys.exit(1)

if __name__ == "__main__":
    main()
