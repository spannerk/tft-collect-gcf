from pyot.models import tft
from pyot.core.queue import Queue
from google.cloud import pubsub_v1
from google.cloud import storage
from datetime import timedelta, datetime

import asyncio


async def consume_summoner(summoner, start_ts, end_ts, summoner_output_func=print, match_output_func=None):
    await summoner.get()
    print("getting data for {}".format(summoner.name))
    summoner_data = summoner.raw()
    history = summoner.match_history
    history.query(count=100, start_time=start_ts, end_time=end_ts)
    await history.get()
    summoner_data['match_history'] = history.raw()

    if summoner_output_func is not None:
        summoner_output_func(summoner_data)

    if match_output_func is not None:
        for match in history.matches:
            await consume_match(match, match_output_func)

    return summoner_data


async def get_summoner_matches(puuids, start_ts, end_ts, topic_name):
    publisher = pubsub_v1.PublisherClient()
    async with Queue() as queue:
        for puuid in puuids:
            summoner = tft.Summoner(puuid=puuid)
            await queue.put(consume_summoner(summoner, start_ts, end_ts, topic_name, publisher))
        return await queue.join()


def gcs_read(bucket_name, blob_name):
    """Write and read a blob from GCS using file-like IO"""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        return f.read()


async def consume_match(match, output_func):
    await match.get()
    print("{} : {}".format(match.id, match.info.datetime))
    return output_func(str(match.raw()))


def get_time_range(run_scheduled, time_window_hours, time_offset_hours):
    end_ts = run_scheduled - timedelta(hours=time_offset_hours)
    start_ts = end_ts - timedelta(hours=time_window_hours)
    return start_ts, end_ts
