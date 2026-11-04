# analysis.py
# Paste your analysis.py code here.
# Pulls BreadCrumb messages from a Pub/Sub subscription.
# Usage: python3 analysis.py analysis_sub

import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, date, timedelta

import google.auth
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import FlowControl

# Configuration
PROJECT_ID = 'dataeng-project-511022'
SUBSCRIPTION_ID = 'analysis_sub'

# How many messages can be held unacknowledged at once
flow_control = FlowControl(max_messages=1000)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# Portland area boundaries
PDXAREA_LAT_MIN, PDX_AREA_LAT_MAX = 45.0, 46.0
PDXAREA_LON_MIN, PDX_AREA_LON_MAX = -123.5, -122.0


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
        self.total = total    # total of breadcrumbs received
        self.seen = bytearray((total + 7) // 8)
        self.unique = 0       # the number of unique vehicles
        self.duplicates = 0   # any number of duplicates

    def add(self, seq):
        byte, bit = divmod(seq, 8)
        mask = 1 << bit
        if self.seen[byte] & mask:
            self.duplicates += 1
        else:
            self.seen[byte] |= mask
            self.unique += 1

    # updates the total expected breadcrumbs
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
runs = {}                     # run_id -> RunTracker
deliveries = 0                # every call
untracked = 0                 # messages without valid run_id/seq attributes
bad_payloads = 0              # messages whose data is not valid JSON
prev_date = None
first_msg_t = None
last_msg_t = None
latency_sum = 0.0             # publish -> receive, from Pub/Sub's publish_time
latency_max = 0.0
start_time = time.time()
elapsed = 0.0
active = 0.0

earliest_breadcrumb = None    # the breadcrumb at the earliest time
latest_breadcrumb = None      # the breadcrumb at the latest time
unique_vehicle_ids = set()    # the number of unique vehicle IDs
unique_trips = set()          # the number of unique trips
invalid_timestamps = 0        # the number of invalid timestamps

# Global variables for validation
trip_opd = {}                 # stores the EVENT_NO_TRIP and OPD_DATE values
trip_meters = {}              # stores the EVENT_NO_TRIP and METERS values
seen_trip_time = set()        # stores the EVENT_NO_TRIP and ACT_TIME values


# Reset all statistics data structures
def reset_daily_stats(date_str):
    global runs, deliveries, untracked, bad_payloads
    global first_msg_t, last_msg_t, latency_sum, latency_max
    global earliest_breadcrumb, latest_breadcrumb
    global unique_vehicle_ids, unique_trips, invalid_timestamps

    runs.clear()
    deliveries = 0
    untracked = 0
    bad_payloads = 0
    first_msg_t = None
    last_msg_t = None
    latency_sum = 0.0
    latency_max = 0.0
    earliest_breadcrumb = None
    latest_breadcrumb = None
    unique_vehicle_ids.clear()
    unique_trips.clear()
    invalid_timestamps = 0
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
    print(f'Earliest breadcrumb: {earliest_breadcrumb}')
    print(f'Latest breadcrumb  : {latest_breadcrumb}')
    print(f'Unique vehicle IDs : {len(unique_vehicle_ids)}')
    print(f'Unique trips       : {len(unique_trips)}')

    for run_id, t in runs.items():
        status = 'COMPLETE' if t.missing == 0 else 'INCOMPLETE'
        print(f'\nRun ID           : {run_id}')
        print(f'Expected records : {t.total}')
        print(f'Unique received  : {t.unique}')
        print(f'Duplicates       : {t.duplicates}')
        print(f'Missing          : {t.missing}')
        print(f'Status           : {status}')


# Runs all validation assertions against a single breadcrumb record
# Returns True if the record passes all validation assertions, False otherwise
def validate_record(record):
    violations = []
    global trip_opd
    global trip_meters
    global seen_trip_time

    # --- Assertion 1: GPS_LATITUDE must be non-null and in [-90, 90] (Limit) ---
    lat = record.get("GPS_LATITUDE")
    if lat is None or not (-90 <= lat <= 90):
        violations.append("A1: GPS_LATITUDE is null or out of range [-90, 90]")

    # --- Assertion 2: GPS_LONGITUDE must be non-null and in [-180, 180] (Limit) ---
    lon = record.get("GPS_LONGITUDE")
    if lon is None or not (-180 <= lon <= 180):
        violations.append("A2: GPS_LONGITUDE is null or out of range [-180, 180]")

    # --- Assertion 3: VEHICLE_ID must be non-null and not empty (Existence) ---
    vid = record.get("VEHICLE_ID")
    if not vid:
      violations.append("A3: VEHICLE_ID is null or empty")

    # --- Assertion 4: EVENT_NO_TRIP must be non-null and non-empty (Existence) ---
    no_trip = record.get("EVENT_NO_TRIP")
    if not no_trip:
      violations.append("A4: EVENT_NO_TRIP is null or empty")

    # --- Assertion 5: VEHICLE_ID must be 1-4 positive digits (0-9999) (Limit) ---
    if vid < 0 or vid > 9999:
      violations.append("A5: VEHICLE_ID is not 1 to 4 digits (0-9999)")

    # --- Assertion 6: OPD_DATE must be between 1996-01-01 and today (inclusive) (Limit) ---
    opd_lower = "01JAN1996:00:00:00"
    opd_lower_date_time = datetime.strptime(opd_lower, "%d%b%Y:%H:%M:%S")

    opd = record.get("OPD_DATE")
    opd_date_time = datetime.strptime(opd,"%d%b%Y:%H:%M:%S")

    if opd_date_time < opd_lower_date_time or opd_date_time > datetime.today():
      violations.append("A6: OPD_DATE is not between 1996-01-01 and today")

    # --- Assertion 7: Every non-null value in OPD_DATE must have a non-null value
    #     in EVENT_NO_TRIP (Intra-record) ---
    # opd = record.get("OPD_DATE") from assertion #6
    # no_trip = record.get("EVENT_NO_TRIP") from assertion #4
    if opd and not no_trip:
      violations.append("A7: OPD_DATE is present but EVENT_NO_TRIP is null or empty.")

    # --- Assertion 8: Every non-null value in ACT_TIME must have a non-null value
	#     in EVENT_NO_STOP (Intra-record) ---
    non_null_act = record.get("ACT_TIME")
    non_null_event_stop = record.get("EVENT_NO_STOP")

    if non_null_act and not non_null_event_stop:
      violations.append("A8: ACT_TIME is present but EVENT_NO_STOP is null or empty.")

    # --- Log and return result ---
    if violations:
        for msg in violations:
            logging.warning("VALIDATION VIOLATION [%s] | record: %s", msg, record)
        return False

    return True


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


def callback(message):
    global deliveries, untracked, bad_payloads, first_msg_t, last_msg_t
    global latency_sum, latency_max
    global earliest_breadcrumb, latest_breadcrumb
    global invalid_timestamps, current_date, prev_date
    now = time.time()
    today = datetime.now().strftime("%Y%m%d")

    try:
        payload = json.loads(message.data.decode('utf-8'))
    except (ValueError, TypeError, AttributeError):
        payload = None
    latency = now - message.publish_time.timestamp()
    attrs = message.attributes

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

        try:
            run_id, seq, expected_total = attrs['run_id'], int(attrs['seq']), int(attrs['total'])
        except (KeyError, ValueError):
            untracked += 1
            message.ack()
            return

        # find the earliest and latest breadcrumb timestamps
        opd_date = payload.get('OPD_DATE') if payload else None
        act_time = payload.get('ACT_TIME') if payload else None
        try:
            base_date = datetime.strptime(str(opd_date), "%d%b%Y:%H:%M:%S")
            breadcrumb_time = base_date + timedelta(seconds=int(act_time))
            # if earliest_breadcrumb is empty, assign with new breadcrumb time
            # otherwise, if the current breadcrumb_time is earlier, assign it
            if (earliest_breadcrumb is None or breadcrumb_time < earliest_breadcrumb):
                earliest_breadcrumb = breadcrumb_time
            else:
                invalid_timestamps += 1
            # check the latest breadcrumb time
            if (latest_breadcrumb is None or breadcrumb_time > latest_breadcrumb):
                latest_breadcrumb = breadcrumb_time
            else:
                invalid_timestamps += 1
        except ValueError:
            invalid_timestamps += 1

        # check the unique vehicle_ids
        ve_id = payload.get('VEHICLE_ID') if isinstance(payload, dict) else None
        if ve_id is not None:
            unique_vehicle_ids.add(ve_id)

        # find the vehicle has no trip available
        event_no_trip = payload.get('EVENT_NO_TRIP') if payload else None
        if event_no_trip is not None:
            unique_trips.add(event_no_trip)

        tracker = runs.setdefault(run_id, RunTracker(expected_total))

        # if the message's expected total is higher than the current total,
        # update the current total to the message's expected total
        if expected_total > tracker.total:
            tracker.update_total(expected_total)

        if 0 <= seq < tracker.total:
            tracker.add(seq)
        else:
            untracked += 1

    # Ack even unparseable messages; they are counted above,
    # unacked would make Pub/Sub redeliver them forever
    message.ack()


def all_runs_complete():
    return bool(runs) and all(t.missing == 0 for t in runs.values())


print(f'Listening on subscription: {SUBSCRIPTION_ID}')

streaming_pull = subscriber.subscribe(sub_path, callback=callback,
                                      flow_control=flow_control,
                                      await_callbacks_on_shutdown=True)

with subscriber:
    try:
        streaming_pull.result()    # run the program daily until the user wants to stop
    except KeyboardInterrupt:
        streaming_pull.cancel()    # normal shutdown
        streaming_pull.result()    # wait for in-flight callbacks to finish
        print("Service stopped by the user!")
    except Exception as e:
        print(f"Error encountered: {e}. Program stopped!")

elapsed = time.time() - start_time
active = (last_msg_t - first_msg_t) if first_msg_t else 0.0

summary_date = prev_date or datetime.now().strftime("%Y%m%d")
print_summary(summary_date)