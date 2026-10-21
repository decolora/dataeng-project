# publisher.py
# Paste your publisher.py code here.
# Fetches and publishes Breadcrumb records, one JSON record per message.

import json
import os
import requests
import sys
import threading
import time
import uuid

import google.auth
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import BatchSettings

# Configuration
PROJECT_ID = 'dataeng-project-511022'
TOPIC_ID = 'bc_topic'
API_URL = 'https://busdata.cs.pdx.edu/api/getBreadCrumbs'
PROGRESS_EVERY_S = 10          # print a progress line every n seconds

# Batch settings
batch_settings = BatchSettings(max_messages=100, max_latency=0.05)

# Check if the project ID exists
def check_project_id(project_id=None):
    project = project_id or os.environ.get('GOOGLE_CLOUD_PROJECT')
    if not project:
        _, project = google.auth.default()
    if not project:
        raise SystemExit ('Could not find the GCP project ID. Please set PROJECT_ID in this script or export GOOGLE_CLOUD_PROJECT.')
    return project

# Create a topic path
publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
topic_path = publisher.topic_path(check_project_id(PROJECT_ID), TOPIC_ID)

# Fail if the topic is missing or permission is lacking
publisher.get_topic(topic=topic_path)

# List of vehicle IDs to fetch (250 in total)
VEHICLE_IDS = [
    4224, 4008, 3255, 3124, 4226, 3208, 3024, 3305, 3916, 3126,
    3555, 3961, 4210, 4001, 3740, 3508, 3750, 3409, 3220, 3411,
    3031, 4223, 3945, 3019, 3755, 3574, 3319, 3218, 3302, 2904,
    3210, 3231, 4502, 4232, 3623, 3607, 3013, 4227, 2925, 3263,
    2933, 4007, 3749, 3559, 3729, 4026, 3727, 3120, 4236, 3609,
    3730, 3329, 3619, 4513, 3133, 3608, 3526, 4065, 3034, 3565,
    3326, 3531, 3515, 3153, 3167, 3561, 3117, 4066, 2926, 3308,
    3618, 3041, 3030, 3518, 3417, 3514, 3059, 3733, 3412, 4002,
    3563, 3107, 3605, 3102, 3614, 4061, 4021, 3142, 4233, 3403,
    2935, 3520, 4015, 3328, 3504, 3307, 3407, 4215, 4510, 3502,
    3647, 4205, 3408, 3264, 3106, 3162, 3534, 3259, 3017, 4039,
    2937, 3136, 3301, 3533, 18724300, 3937, 3577, 3956, 3012, 2915,
    3721, 3234, 2934, 3645, 3642, 3648, 3145, 3725, 4011, 3159,
    3143, 4052, 2930, 3510, 4032, 3959, 4044, 3148, 3708, 3942,
    4515, 3058, 3009, 3155, 3045, 4220, 3262, 3506, 3548, 3505,
    3711, 3613, 3127, 3550, 4504, 3539, 99222, 4040, 3268, 4005,
    3560, 4018, 2928, 3204, 3229, 3738, 3251, 3702, 3101, 3537,
    4528, 3111, 3904, 3129, 4208, 3602, 3410, 4203, 2907, 3008,
    3245, 3221, 3731, 3641, 3562, 3320, 3513, 4049, 3616, 3115,
    3267, 4054, 3910, 3953, 3144, 3044, 4206, 3103, 3228, 3934,
    4204, 2919, 3421, 3112, 4228, 3146, 3746, 4234, 3318, 3405,
    3739, 3227, 4531, 3920, 2921, 3160, 3315, 3327, 4048, 3206,
    3116, 3710, 2940, 3936, 4517, 3728, 4017, 3141, 3907, 4521,
    4053, 4060, 3644, 3105, 4304, 4217, 4028, 3911, 3323, 3941,
    4212, 3325, 3222, 3532, 4043, 4042, 3958, 3140, 3649, 3170
]

if not VEHICLE_IDS:
    raise ValueError('The list of vehicle IDs is empty.')

# Print a unique RunID for this run of the publisher.
RUN_ID = time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6]

# Completion tracking (background threads)
done_cv = threading.Condition()
ok = 0
errors = 0

def on_publish_done(future):
    global ok, errors
    exc = future.exception()
    with done_cv:
        if exc is None:
            ok += 1
        else:
            errors += 1
            print(f'Publish failed: {exc}')
        done_cv.notify_all()

print(f'Run ID        : {RUN_ID}')
print(f'Publishing to : {topic_path}\n')

published = 0
total = 0
unique_vehicle_ids = set()
start = time.time()
last_progress = start

# Process all vehicle IDs
for vid in VEHICLE_IDS:
    try:
        # call the API endpoint to fetch vehicle records
        resp = requests.get(API_URL, params={'vehicle_id': vid}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        records = data.get('results') if isinstance(data, dict) else data
        if not isinstance(records, list):
            raise ValueError(f'Unexpected response: {type(data).__name__}')
        time.sleep(0.1)

        total += len(records)
        if records:
           unique_vehicle_ids.add(vid)

        # publish the records
        for record in records:
            payload = json.dumps(record, separators=(',', ':'))
            future = publisher.publish(topic_path, payload.encode('utf-8'),
                                       run_id=RUN_ID, seq=str(published),
                                       total=str(total))
            future.add_done_callback(on_publish_done)
            published += 1

        # print log message
        now = time.time()
        if now - last_progress >= PROGRESS_EVERY_S:
            print(f'  {now - start:6.1f}s queued {published}, confirmed {ok}')
            last_progress = now

    # NOTE: omitted error message to reduce log output
    except Exception as e:
        pass

# Wait for every in-flight publish to be confirmed or to fail.
with done_cv:
    while ok + errors < published:
        done_cv.wait(timeout=1)
        now = time.time()
        if now - last_progress >= PROGRESS_EVERY_S:
            print(f'  {now - start:6.1f}s confirmed {ok}/{published}, errors {errors}')
            last_progress = now

finish = time.time()
elapsed = time.time() - start

api_start = time.localtime(start)
final_publish = time.localtime(finish)

print(f'\n--- Publisher Summary ---')
print(f'Run ID                : {RUN_ID}')
print(f'API access start time : {time.strftime("%Y-%m-%d %H:%M:%S", api_start)}')
print(f'Final publish time    : {time.strftime("%Y-%m-%d %H:%M:%S", final_publish)}')
print(f'Breadcrumbs received  : {total}')
print(f'Breadcrumbs published : {ok}')
print(f'Publishing errors     : {errors}')
print(f'Total time            : {elapsed:.3f}s')
print(f'Throughput            : {ok / elapsed:.1f} msg/s')
print(f'Total unique IDs      : {len(unique_vehicle_ids)}')

sys.exit(1 if errors else 0)