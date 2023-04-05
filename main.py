# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START functions_cloudevent_pubsub]
import base64

import functions_framework
from get_data.tasks import get_summoner_matches, gcs_read
import asyncio

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def subscribe(cloud_event):
    from datetime import datetime, timedelta

    project_name = "verdant-wave-375715"
    gcs_bucket_name = "summoner_checklist"
    gcs_file_name = "summoner_list.csv"
    pubsub_topic_input = "twice-per-day"
    pubsub_topic_output = "player-matches"
    time_window_hours = 12
    time_offset_hours = 1
    run_scheduled = datetime(2023, 4, 3, 0, 0, 0)
    num_puuids = 3

    puuids = gcs_read(gcs_bucket_name, gcs_file_name)
    to_use_puuids = puuids.split()[1:num_puuids + 1]
    my_start_ts = run_scheduled - timedelta(hours=time_window_hours) - timedelta(hours=time_offset_hours)
    my_end_ts = run_scheduled - timedelta(hours=time_offset_hours)
    my_output_topic_name = "projects/{}/topics/{}".format(project_name, pubsub_topic_output)
    my_matches = asyncio.run(get_summoner_matches(to_use_puuids, my_start_ts, my_end_ts, my_output_topic_name))
    print(my_matches)

    # print("Hello, " + base64.b64decode(cloud_event.data["message"]["data"]).decode() + "!")
# [END functions_cloudevent_pubsub]
