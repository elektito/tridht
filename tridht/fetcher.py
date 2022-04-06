import logging
import random
import trio
from .bt import Bittorrent

MAX_FETCERS = 100
MAX_CONNS_PER_FETCHER = 15
MAX_TOTAL_CONNS = 950

logger = logging.getLogger(__name__)

class MetadataFetcher:
    def __init__(self, infohash_db, dhts):
        self.dhts = dhts
        self.infohash_db = infohash_db

        self.fetches_in_flight = 0

    async def run(self):
        fetchers_limit = trio.CapacityLimiter(MAX_FETCERS)
        async with trio.open_nursery() as nursery:
            while True:
                while self.infohash_db.size() == 0:
                    await trio.sleep(1)

                ih = random.choice(list(self.infohash_db.infohashes))
                dht = random.choice(self.dhts)
                async with fetchers_limit:
                    nursery.start_soon(
                        self._fetch_metadata, ih, dht)

    async def _fetch_metadata(self, infohash, dht):
        logger.info(f'Looking for metadata for: {infohash.hex()}')

        async def fetch(ih, ip, port, nursery):
            self.fetches_in_flight += 1
            try:
                bt = Bittorrent(ip, port, ih)
                await bt.run()
            finally:
                self.fetches_in_flight -= 1
            if bt.metadata is not None:
                logger.info(f'Obtained metadata for infohash: {ih.hex()}')
                nursery.cancel_scope.cancel()
                self.infohash_db.store_metadata(ih, bt.metadata)
            else:
                self.infohash_db.update_ih_status(ih, 'FAILED_FETCH_METADATA | HAD_PEERS')

        conns_limit = trio.CapacityLimiter(MAX_CONNS_PER_FETCHER)
        total_conns_limit = trio.CapacityLimiter(MAX_TOTAL_CONNS)
        peer_send_chan, peer_recv_chan = trio.open_memory_channel(0)
        async with trio.open_nursery() as nursery:
            nursery.start_soon(dht.fetch_peers, infohash, peer_send_chan)
            async for ip, port in peer_recv_chan:
                async with conns_limit, total_conns_limit:
                    nursery.start_soon(fetch, infohash, ip, port, nursery)

        peer_send_chan.close()
        peer_recv_chan.close()
