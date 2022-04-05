import logging
import random
import trio
from .bt import Bittorrent

MAX_FETCERS = 100
MAX_CONNS_PER_FETCHER = 15

logger = logging.getLogger(__name__)

async def fetch_metadata(infohash_db, dhts):
    fetchers_limit = trio.CapacityLimiter(MAX_FETCERS)
    async with trio.open_nursery() as nursery:
        while True:
            while infohash_db.size() == 0:
                await trio.sleep(1)

            ih = random.choice(list(infohash_db.infohashes))
            dht = random.choice(dhts)
            async with fetchers_limit:
                nursery.start_soon(
                    _fetch_metadata, ih, dht, infohash_db)


async def _fetch_metadata(infohash, dht, ihdb):
    logger.info(f'Looking for metadata for: {infohash.hex()}')
    async def fetch(ih, ip, port, nursery):
        bt = Bittorrent(ip, port, ih)
        await bt.run()
        if bt.metadata is not None:
            logger.info(f'Obtained metadata for infohash: {ih.hex()}')
            nursery.cancel_scope.cancel()
            ihdb.store_metadata(ih, bt.metadata)
        else:
            ihdb.update_ih_status(ih, 'FAILED_FETCH_METADATA | HAD_PEERS')

    conns_limit = trio.CapacityLimiter(MAX_CONNS_PER_FETCHER)
    peer_send_chan, peer_recv_chan = trio.open_memory_channel(0)
    async with trio.open_nursery() as nursery:
        nursery.start_soon(dht.fetch_peers, infohash, peer_send_chan)
        async for ip, port in peer_recv_chan:
            async with conns_limit:
                nursery.start_soon(fetch, infohash, ip, port, nursery)

    peer_send_chan.close()
    peer_recv_chan.close()
