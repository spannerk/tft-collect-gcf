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
from get_data.tasks import process_run, gcs_write
import asyncio
from datetime import datetime

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def read_event(cloud_event):

    configs = {
        'project_name': 'verdant-wave-375715',
        'input_bucket_name': "summoner_checklist",
        'input_blob_name': "summoner_list.csv",
        'summoner_output_func': gcs_write,
        'summoner_output_location': 'tft-summoners',
        'match_output_func': gcs_write,
        'match_output_location': 'tft-player-matches',
        'num_puuids':None,
        'time_window_hours': 12,
        'time_offset_hours': 1
    }

    run_scheduled = datetime.strptime(cloud_event.data['message']['publish_time'][:19], "%Y-%m-%dT%H:%M:%S")
    
    asyncio.run(process_run(run_scheduled, configs))

