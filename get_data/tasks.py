from pyot.models import tft
from pyot.core.queue import Queue
from google.cloud import pubsub_v1
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