import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, date, timedelta
from pyproj import Transformer

import google.auth
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import FlowControl

import psycopg2
from psycopg2.extras import execute_values

# Configuration
PROJECT_ID = 'dataeng-project-511022'
SUBSCRIPTION_ID = 'se_analysis_sub'

# How many messages can be held unacknowledged at once
flow_control = FlowControl(max_messages=10000)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# Create a transformer: EPSG (Oregon State Plane North, feet) -> WGS 84
_transformer = Transformer.from_crs("EPSG:2913", "EPSG:4326", always_xy=True)

# Portland area boundaries
PDXAREA_LAT_MIN, PDX_AREA_LAT_MAX = 45.0, 46.0
PDXAREA_LON_MIN, PDX_AREA_LON_MAX = -123.5, -122.0

TABLE_NAME = "StopEvent"        # table name for the database
BATCH_SIZE = 1000               # number of records to insert in a single batch

# Column names for the database table
COLUMNS = [
    "vehicle_number", "leave_time", "train", "route_number",
    "direction", "service_key", "trip_number", "stop_time",
    "arrive_time", "dwell", "location_id", "door", "lift",
    "ons", "offs", "estimated_load", "maximum_speed",
    "train_mileage", "pattern_distance", "location_distance",
    "GPS_latitude", "GPS_longitude", "data_source", "schedule_status",
]

# Check if the project ID exists
def check_project_id(project_id=None):
    project = project_id or os.environ.get('GOOGLE_CLOUD_PROJECT')
    if not project:
        _, project = google.auth.default()
    if not project:
        raise SystemExit ('Could not find the GCP project ID. Please set PROJECT_ID in this script or export GOOGLE_CLOUD_PROJECT.')
    return project


# Tracks one publisher run with a bitmap: 1 bit per record
class RunTracker:
    def __init__(self, total):
        self.total = total      # total of stop events received
        self.seen = bytearray((total + 7) // 8)
        self.unique = 0         # the number of unique vehicles
        self.duplicates = 0     # any number of duplicates

    def add(self, seq):
        byte, bit = divmod(seq, 8)
        mask = 1 << bit
        if self.seen[byte] & mask:
            self.duplicates += 1
        else:
            self.seen[byte] |= mask
            self.unique += 1

    # updates the total expected stop events
    def update_total(self, new_total):
        if new_total > self.total:
            new_byte_len = (new_total + 7) // 8
            current_byte_len = len(self.seen)

            # extends the bytearray
            if new_byte_len > current_byte_len:
                self.seen.extend(b'\x00' * (new_byte_len - current_byte_len))
        
            self.total = new_total

    @property
    def missing(self):
        return self.total - self.unique


subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(check_project_id(PROJECT_ID), SUBSCRIPTION_ID)

# Fail if the topic is missing or permission is lacking
subscriber.get_subscription(subscription=sub_path)

# Shared state
lock = threading.Lock()
runs = {}                       # run_id -> RunTracker
deliveries = 0                  # every call
untracked = 0                   # messages without valid run_id/seq attributes
bad_payloads = 0                # messages whose data is not valid JSON
prev_date = None
first_msg_t = None
last_msg_t = None
latency_sum = 0.0               # publish -> receive, from Pub/Sub's publish_time
latency_max = 0.0
start_time = time.time()
elapsed = 0.0
active = 0.0

invalid_timestamps = 0          # the number of invalid timestamps
invalid_records_list = []       # the list of invalid records with violations

conn = None                     # database connection
batch_buffer = []               # buffer for batch inserts
stored = 0                      # the number of records successfully inserted

same_trip_incr_arrive = {}      # stores the different arrival times with same trip number


# Reset all statistics data structures
def reset_daily_stats(date_str):
    global runs, deliveries, untracked, bad_payloads
    global first_msg_t, last_msg_t, latency_sum, latency_max
    global invalid_timestamps
    global invalid_records_list
    global stored
    global same_trip_incr_arrive

    # flushes any remaining records in the batch buffer
    flush_record()

    runs.clear()
    deliveries = 0
    untracked = 0
    bad_payloads = 0
    first_msg_t = None
    last_msg_t = None
    latency_sum = 0.0
    latency_max = 0.0
    invalid_timestamps = 0
    stored = 0
    same_trip_incr_arrive.clear()

    if invalid_records_list:
        # writes the invalid data from previous day to a json file
        write_invalid_records(invalid_records_list, date_str)
        invalid_records_list.clear()

    print(f"{date_str} — RESET ALL STATISTICS AND WAIT FOR THE NEW DAY.")


def print_summary(today):
    print(f'\n--- Subscriber Summary ({SUBSCRIPTION_ID}) - {today} ---')
    print(f'Deliveries         : {deliveries}   (includes any redeliveries)')
    print(f'Total wall time    : {elapsed:.3f}s   (includes waiting)')
    print(f'Active recv time   : {active:.3f}s   (first to last message)')
    if active > 0:
        print(f'Throughput         : {deliveries / active:.1f} msg/s')
    if deliveries:
        print(f'Latency avg / max  : {latency_sum / deliveries:.3f}s / {latency_max:.3f}s'
              f'   (publish to receive)')
    print(f'Untracked msgs     : {untracked}   (no run_id/seq attributes)')
    print(f'Bad payloads       : {bad_payloads}')
    print(f'Invalid records    : {len(invalid_records_list)}')
    print(f'Stored records     : {stored}')

    for run_id, t in runs.items():
        status = 'COMPLETE' if t.missing == 0 else 'INCOMPLETE'
        print(f'\nRun ID           : {run_id}')
        print(f'Expected records : {t.total}')
        print(f'Unique received  : {t.unique}')
        print(f'Duplicates       : {t.duplicates}')
        print(f'Missing          : {t.missing}')
        print(f'Status           : {status}')


# Runs all validation assertions against a single StopEvent record
# Returns True if the record passes all validation assertions, False otherwise
def validate_record(record):
    violations = []
    global same_trip_incr_arrive
    
    # --- Assertion 1: arrive_time must be in the range [0, 108000] (Limit) ---
    arrive_t = record.get("arrive_time")
    if arrive_t is None or not (0 <= arrive_t <= 108000):
        violations.append("A1: The arrive_time is null or out of range [0, 108000]")

    # --- Assertion 2: leave_time must be greater or equal to the arrival time (Intra-record) ---
    leave_t = record.get("leave_time")
    if leave_t is None or not (leave_t >= arrive_t):
        violations.append("A2: The leave_time is null or less than arrive_time!")

    # --- Assertion 3: The location_id must be non-null and not empty (Existence) ---
    loc_id = record.get("location_id")
    if loc_id is None:
        violations.append("A3: The location_id is null or empty!")

    # --- Assertion 4: dwell must be less or equal to the difference of leave_time and arrive_time (Intra-record) ---
    dwell = record.get("dwell")
    if dwell is None or not (dwell <= (leave_t - arrive_t)):
        violations.append("A4: The dwell is null or greater than the difference of leave_time and arrive_time!")

    # --- Assertion 5: direction must be either 0 or 1	(Limit) ---
    direction = record.get("direction")
    if direction is None or direction not in (0,1):
        violations.append("A5: The direction is null or neither 0 nor 1!")

    # --- Assertion 6: Within the same trip_number, the arrive_time must be increasing across consecutive stops	(Inter-record) ---
    trip_n = record.get("trip_number")
    if trip_n in same_trip_incr_arrive:
        if same_trip_incr_arrive[trip_n] > arrive_t:
            violations.append("A6: The arrive_time is decreasing!")
    else:
        same_trip_incr_arrive[trip_n] = arrive_t

    # --- Assertion 7: schedule_status must be non-null or empty (Existence) ---
    schedule_status = record.get("schedule_status")
    if schedule_status is None:
        violations.append("A7: The schedule_status is null or empty!")

    # --- Assertion 8: If schedule_status == 6, then leave_time == arrive_time (Intra-record) ---
    # Doc: the value of the Leave Time column has been set to the value of the recorded arrive time at the Last Stop of Trip
    if schedule_status == 6:
        if not (leave_t == arrive_t):
            violations.append("A8: The leave_time is not the same as the arrive_time when schedule_status is 6!")

    # --- Log and return result ---
    if violations:
        # for msg in violations:
            # logging.warning("VALIDATION VIOLATION [%s] | record: %s", msg, record)
        return False, violations

    return True, []


# Writes accumulated invalid records to a file
# invalid_records: a list of dictionaries containing a 'record' key (original data)
#                  and a 'violations' key (a list of assertion violation messages)
# run_date: a date string to be used in the file name; default is today (optional)
def write_invalid_records(invalid_records, run_date=None):
    if run_date is None:
        run_date = date.today().isoformat()

    filename = f"invalid_data_{run_date}.json"

    with open(filename, "w") as f:
        json.dump(invalid_records, f, indent=2, default=str)

    print(f"Wrote {len(invalid_records)} invalid records to {filename}")


# Transforms from seconds to timestamp
def _sec_to_timestamp(seconds_value, base_date):
    if seconds_value is None or str(seconds_value).strip() == "":
        return None
    midnight = datetime.combine(base_date, time.min)
    return midnight + timedelta(seconds=int(float(seconds_value)))

# Transforms the arrive_time, stop_time, leave_time into timestamps
# and add them into the base service_date
# The service_date in this case is the today's date
def time_fields_to_timestamps (arrive_t, stop_t, leave_t):
    service_date = date.today()
    arrive_seconds = _sec_to_timestamp(arrive_t, service_date)
    stop_seconds = _sec_to_timestamp(stop_t, service_date)
    leave_seconds = _sec_to_timestamp(leave_t, service_date)
    return arrive_seconds, stop_seconds, leave_seconds

# Convert Oregon State Plane Coordinate System coordinates (EPSG:2913)
# to WGS 84 GPS latitude and longitude.
def state_plane_to_latlon(x_coordinate, y_coordinate):
    lon, lat = _transformer.transform(x_coordinate, y_coordinate)
    return lat, lon

# Loads database credentials from a .env file
def load_env(filepath=".env"):
    if not os.path.exists(filepath):
        return

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.strip().split("=", 1)
                os.environ[key] = value


# Returns a psycopg2 connection for the database
def get_connection():
    global conn
    if conn is None or conn.closed != 0:
        try:
            load_env(".env")
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS")
            )
        except Exception as e:
            logging.error(f"Error loading .env file: {e}")
            conn = None
    return conn


# Flushes the batch buffer; batch buffer holds BATCH_SIZE records
def flush_record():
    global batch_buffer
    to_insert = []

    with lock:
        if not batch_buffer:
            return
        to_insert = batch_buffer
        batch_buffer = []

    # executes the batch insert outside of lock
    conn = get_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cols = ', '.join(COLUMNS)
                query = f"INSERT INTO {TABLE_NAME} ({cols}) VALUES %s"
                execute_values(cur, query, to_insert, page_size=BATCH_SIZE)
            conn.commit()
        except Exception as e:
            logging.error(f"Error flushing records to database: {e}")
            try:
                conn.rollback()
            except Exception as r:
                logging.error(f"Error rolling back transaction: {r}")


# Loads the transformed data into the database
def load_record(record):
    global batch_buffer
    global stored
    values = tuple(record.get(col) for col in COLUMNS)
    should_flush = False

    with lock:
        batch_buffer.append(values)
        stored += 1
        if len(batch_buffer) >= BATCH_SIZE:
            should_flush = True

    if should_flush:
        flush_record()


def callback(message):
    global deliveries, untracked, bad_payloads, first_msg_t, last_msg_t
    global latency_sum, latency_max
    global invalid_timestamps, prev_date
    now = time.time()
    today = datetime.now().strftime("%Y%m%d")

    try:
        payload = json.loads(message.data.decode('utf-8'))
    except (ValueError, TypeError, AttributeError):
        payload = None
    latency = now - message.publish_time.timestamp()
    attrs = message.attributes

    valid_to_load = False

    with lock:
        # check if it is a new day, reset and show new statistics
        if prev_date is not None and prev_date != today:
            print_summary(prev_date)
            reset_daily_stats(today)
        if prev_date is None or prev_date != today:
            prev_date = today

        deliveries += 1
        first_msg_t = first_msg_t or now
        last_msg_t = now
        latency_sum += latency
        latency_max = max(latency_max, latency)

        if payload is None or not isinstance(payload, dict):
            bad_payloads += 1
        else:
            valid_record, violations = validate_record(payload)
            if not valid_record:
                invalid_records_list.append({"violations": violations, "record": payload})

        try:
            run_id, seq, expected_total = attrs['run_id'], int(attrs['seq']), int(attrs['total'])
        except (KeyError, ValueError):
            untracked += 1
            message.ack()
            return

        tracker = runs.setdefault(run_id, RunTracker(expected_total))

        # if the message's expected total is higher than the current total,
        # update the current total to the message's expected total
        if expected_total > tracker.total:
            tracker.update_total(expected_total)

        if 0 <= seq < tracker.total:
            tracker.add(seq)
        else:
            untracked += 1
        
        # transforms data before loading into the database
        if isinstance(payload, dict) and valid_record:
                try:
                    arrive_ts, stop_ts, leave_ts = time_fields_to_timestamps(
                        payload.get("arrive_time"),
                        payload.get("stop_time"),
                        payload.get("leave_time")
                    )
                    payload["arrive_time"] = arrive_ts
                    payload["stop_time"] = stop_ts
                    payload["leave_time"] = leave_ts

                    x_val = payload.get("x_coordinate")
                    y_val = payload.get("y_coordinate")
                    if x_val not in (None, "") and y_val not in (None, ""):
                        lat, lon = state_plane_to_latlon(float(x_val), float(y_val))
                        payload["GPS_latitude"] = lat
                        payload["GPS_longitude"] = lon
                    else:
                        payload["GPS_latitude"] = None
                        payload["GPS_longitude"] = None

                    # arrive_time is a PRIMARY KEY
                    valid_to_load = payload["arrive_time"] is not None
                except Exception as e:
                    logging.error(f"Error transforming record: {e}")

    # loads database outside of lock to avoid blocking other threads
    if valid_to_load:
        load_record(payload)

    # Ack even unparseable messages; they are counted above,
    # unacked would make Pub/Sub redeliver them forever
    message.ack()


def all_runs_complete():
    return bool(runs) and all(t.missing == 0 for t in runs.values())


# Handles the shutdown if VM/systemd stops
def handle_shutdown(signum, frame):
    global elapsed, active
    print(f"Shutdown signal ({signum}) received. Writing summary...")

    flush_record()  # flush any remaining records in the batch buffer

    if conn and conn.closed == 0:
        try:
            conn.close()
        except Exception as e:
            logging.error(f"Error closing connection: {e}")
            try:
                conn.rollback()
            except Exception as r:
                logging.error(f"Error rolling back transaction: {r}")

    elapsed = time.time() - start_time
    active = (last_msg_t - first_msg_t) if first_msg_t else 0.0

    summary_date = prev_date or datetime.now().strftime("%Y%m%d")

    if invalid_records_list:
        write_invalid_records(invalid_records_list, summary_date)
    print_summary(summary_date)

    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

print(f'Listening on subscription: {SUBSCRIPTION_ID}')
sys.stdout.flush()

streaming_pull = subscriber.subscribe(sub_path, callback=callback,
                                      flow_control=flow_control,
                                      await_callbacks_on_shutdown=False)

with subscriber:
    try:
        streaming_pull.result()    # run the program daily until the user wants to stop
    except KeyboardInterrupt:
        streaming_pull.cancel()    # normal shutdown
        streaming_pull.result()    # wait for in-flight callbacks to finish
        print("Service stopped by the user!")
    except Exception as e:
        print(f"Error encountered: {e}. Program stopped!")
        sys.stdout.flush()
        sys.stderr.flush()

flush_record()  # flush any remaining records in the batch buffer

if conn and conn.closed == 0:
    try:
        conn.close()
    except Exception as e:
        logging.error(f"Error closing connection: {e}")
        try:
            conn.rollback()
        except Exception as r:
            logging.error(f"Error rolling back transaction: {r}")

elapsed = time.time() - start_time
active = (last_msg_t - first_msg_t) if first_msg_t else 0.0

summary_date = prev_date or datetime.now().strftime("%Y%m%d")

if invalid_records_list:
    write_invalid_records(invalid_records_list, summary_date)
print_summary(summary_date)