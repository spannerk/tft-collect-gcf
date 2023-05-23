from pyot.models import tft
from pyot.core.queue import Queue
from google.cloud import pubsub_v1
from google.cloud import storage
from datetime import timedelta, datetime
import asyncio


async def consume_summoner(summoner, start_ts, end_ts, summoner_output_func=print, match_output_func=None,
                           summoner_output_location=None, match_output_location=None):
    await summoner.get()
    print("getting data for {}".format(summoner.name))
    summoner_data = summoner.raw()
    history = summoner.match_history
    history.query(count=100, start_time=start_ts, end_time=end_ts)
    await history.get()
    summoner_data['match_history'] = history.raw()

    if summoner_output_func is not None:
        if summoner_output_location is None:
            summoner_output_func_args = []
        else:
            summoner_output_func_args = [summoner_output_location, "{}_{}_{}.json".format(summoner.puuid, str(start_ts), str(end_ts))]
        summoner_output_func(str(summoner_data), *summoner_output_func_args)

    if match_output_func is not None:
        for match in history.matches:
            if match_output_location is None:
                match_output_func_args = []
            else:
                match_output_func_args = [match_output_location, "{}.json".format(match.id)]
            await consume_match(match, match_output_func, match_output_func_args)

    return summoner_data


async def get_summoner_matches(puuids, start_ts, end_ts, summoner_output_func, match_output_func,
                               summoner_output_func_args=[], match_output_func_args=[]):
    async with Queue() as queue:
        for puuid in puuids:
            summoner = tft.Summoner(puuid=puuid)
            await queue.put(consume_summoner(summoner, start_ts, end_ts, summoner_output_func, match_output_func,
                                             summoner_output_func_args, match_output_func_args))
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


def gcs_write(content, bucket_name, blob_name):
    """Write and read a blob from GCS using file-like IO"""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        f.write(content)


async def consume_match(match, output_func, output_func_args=[]):
    await match.get()
    print("{} : {}".format(match.id, match.info.datetime))
    return output_func(str(match.raw()), *output_func_args)


def get_time_range(run_scheduled, time_window_hours, time_offset_hours):
    end_ts = run_scheduled - timedelta(hours=time_offset_hours)
    start_ts = end_ts - timedelta(hours=time_window_hours)
    return start_ts, end_ts


async def process_run(run_scheduled, config_dict):
    start_time, end_time = get_time_range(run_scheduled, config_dict['time_window_hours'],
                                          config_dict['time_offset_hours'])
    puuids = gcs_read(config_dict['input_bucket_name'], config_dict['input_blob_name']).split()
    if config_dict['num_puuids'] is not None:
        to_use_puuids = puuids[:config_dict['num_puuids']]
    else:
        to_use_puuids = puuids

    await get_summoner_matches(to_use_puuids,
                               start_time,
                               end_time,
                               config_dict['summoner_output_func'],
                               config_dict['match_output_func'],
                               config_dict['summoner_output_location'],
                               config_dict['match_output_location']
                               )
