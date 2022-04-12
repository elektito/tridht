import argparse
import logging
import random
import trio
from urllib.parse import urlsplit
from .bt import Bittorrent
from .utils import config_logging, launch_limited
from .database import Database
from .dht import Dht
from .routing_table import BucketRoutingTable
from .peer_table import PeerTable

MAX_FETCHERS = 200
MAX_CONNS_PER_FETCHER = 15
MAX_TOTAL_CONNS = 950

logger = logging.getLogger(__name__)

class MetadataFetcher:
    def __init__(self, db, dhts):
        self.dhts = dhts

        self.fetches_in_flight = 0
        self.canceled_with_no_fetch = 0
        self.fetched = 0

        self._db = db
        self._fetchers_semaphore = trio.Semaphore(MAX_FETCHERS)
        self._total_conns_limit = trio.CapacityLimiter(MAX_TOTAL_CONNS)

    async def run(self):
        logger.info('Metadata fetcher started.')

        async with trio.open_nursery() as nursery:
            while True:
                if self.dhts[0]._routing_table.size() == 0:
                    await trio.sleep(0.1)
                    continue
                results = await self._db.get_some_due_infohashes()
                for ih, peer_ip, peer_port in results:
                    dht = random.choice(self.dhts)
                    if peer_ip:
                        peers = [(peer_ip, peer_port)]
                    else:
                        peers = []
                    await launch_limited(
                        self._fetch_metadata, ih, dht, peers,
                        nursery=nursery,
                        semaphore=self._fetchers_semaphore)

    async def _fetch_metadata(self, infohash, dht, initial_peers):
        logger.info(f'Looking for metadata for: {infohash.hex()}')

        fetched = False
        conns_limit = trio.CapacityLimiter(MAX_CONNS_PER_FETCHER)
        async def fetch(ih, ip, port, nursery):
            nonlocal fetched
            self.fetches_in_flight += 1
            try:
                async with conns_limit, self._total_conns_limit:
                    bt = Bittorrent(ip, port, ih)
                    await bt.run()
            finally:
                self.fetches_in_flight -= 1
            if bt.metadata is not None:
                logger.info(f'Obtained metadata for infohash: {ih.hex()}')
                await self._db.store_metadata(ih, bt.metadata)
                fetched = True
                nursery.cancel_scope.cancel()

        peer_send_chan, peer_recv_chan = trio.open_memory_channel(0)
        started_fetch = False
        with trio.move_on_after(60) as cancel_scope:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(dht.fetch_peers, infohash, peer_send_chan)
                for ip, port in initial_peers:
                    nursery.start_soon(fetch, infohash, ip, port, nursery)
                    started_fetch = True
                async for ip, port in peer_recv_chan:
                    nursery.start_soon(fetch, infohash, ip, port, nursery)
                    started_fetch = True

        if cancel_scope.cancelled_caught:
            if not started_fetch:
                # timed out without even starting a single fetcher
                # (due to no peers)
                self.canceled_with_no_fetch += 1

        peer_send_chan.close()
        peer_recv_chan.close()

        if fetched:
            self.fetched += 1
        else:
            await self._db.mark_fetch_metadata_failure(infohash)

    @property
    def stats(self):
        return {
            'fetches_running': self.fetches_in_flight,
            'cancel_no_fetch': self.canceled_with_no_fetch,
            'fetched': self.fetched,
        }


async def periodically_log_stats(stats_period, dht, fetcher):
    while True:
        await trio.sleep(stats_period)
        dht_stats = ' '.join(
            f'{k}={v}'for k, v in dht.stats.items())
        fetcher_stats = ' '.join(
            f'{k}={v}'for k, v in fetcher.stats.items())
        logger.info(
            f'Stats: rt-size={dht._routing_table.size()} ' +
            dht_stats + ' ' +
            fetcher_stats
        )


def log_level_value(value):
    return value.upper()


def node(value):
    result = urlsplit('//' + value)
    host, port = result.hostname, result.port
    if port is None:
        port = 6881
    return host, port


async def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--log-level', '-l', default='INFO', type=log_level_value,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the log level. Defaults to %(default)s.')

    parser.add_argument(
        '--port', '-p', default=6881, type=int,
        help='DHT node port. Defaults to %(default)s.')

    parser.add_argument(
        '--seed', '-s', default='router.bittorrent.com:6881', type=node,
        help='The node to seed the routing table from. We will send a '
        'find_node query to this node with a random node id and '
        'attempt getting more nodes. It will then be removed from the '
        'routing table. If no port is specified, 6881 is assumed. '
        'Defaults to %(default)s.')

    parser.add_argument(
        '--stats', action='store_true', default=False,
        help='Periodically log some stats.')

    parser.add_argument(
        '--stats-period', type=int, default=60,
        help='The period in which stats are logged when --stats is '
        'set. Defaults to %(default)s seconds.')

    parser.add_argument(
        '--database', '-d', default='postgresql+asyncpg:///tridht',
        help='The postgres database to use. Defaults to "%(default)s".')

    args = parser.parse_args()

    config_logging(args.log_level)

    db = Database(args.database)
    routing_table = BucketRoutingTable(db)
    peer_table = PeerTable(db)
    seed_host, seed_port = args.seed
    dht = Dht(
        args.port,
        db=db,
        seed_host=seed_host,
        seed_port=seed_port,
        routing_table=routing_table,
        peer_table=peer_table,
        no_expand=False,
        no_index=True,
        readonly=True,
    )
    fetcher = MetadataFetcher(db, [dht])

    async with trio.open_nursery() as nursery:
        nursery.start_soon(db.run)
        await db.ready.wait()

        routing_table.dht = dht

        nursery.start_soon(dht.run)
        nursery.start_soon(routing_table.run)
        nursery.start_soon(peer_table.run)
        nursery.start_soon(fetcher.run)

        if args.stats:
            nursery.start_soon(
                periodically_log_stats,
                args.stats_period, dht, fetcher)

if __name__ == '__main__':
    trio.run(main)
