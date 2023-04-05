from pyot.models import tft
from pyot.core.queue import Queue
from google.cloud import pubsub_v1
from google.cloud import storage

import asyncio


async def consume_summoner(summoner, start_ts, end_ts, topic_name, publisher):
    await summoner.get()
    print("getting data for {}".format(summoner.name))
    summoner_data = summoner.raw()
    history = await summoner.match_history.get()

    for match in history.matches:
        await match.get()
        print("{} : {}".format(match.id, match.info.datetime))
        if match.info.datetime < start_ts:
            break
        if match.info.datetime < end_ts:
            await asyncio.wrap_future(publisher.publish(topic_name, bytes(str(match.raw()), 'utf-8')))

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