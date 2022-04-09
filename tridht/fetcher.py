import argparse
import logging
import random
import trio
from urllib.parse import urlsplit
from .bt import Bittorrent
from .utils import config_logging
from .infohash_db import InfohashDb
from .dht import Dht

MAX_FETCHERS = 300
MAX_CONNS_PER_FETCHER = 15
MAX_TOTAL_CONNS = 950

logger = logging.getLogger(__name__)

class MetadataFetcher:
    def __init__(self, infohash_db, dhts):
        self.dhts = dhts
        self.infohash_db = infohash_db

        self.fetches_in_flight = 0

        self._fetchers_semaphore = trio.Semaphore(MAX_FETCHERS)

    async def run(self):
        logger.info('Metadata fetcher started.')

        async with trio.open_nursery() as nursery:
            while True:
                if self.dhts[0]._routing_table.size() == 0:
                    await trio.sleep(0.1)
                    continue
                results = await self.infohash_db.get_some_due_infohashes()
                for ih, peer_ip, peer_port in results:
                    dht = random.choice(self.dhts)
                    if peer_ip:
                        peers = [(peer_ip, peer_port)]
                    else:
                        peers = []
                    await self._fetchers_semaphore.acquire()
                    nursery.start_soon(
                        self._fetch_metadata_and_release,
                        ih, dht, peers)

    async def _fetch_metadata_and_release(self, *args, **kwargs):
        try:
            await self._fetch_metadata(*args, **kwargs)
        finally:
            self._fetchers_semaphore.release()

    async def _fetch_metadata(self, infohash, dht, initial_peers):
        logger.info(f'Looking for metadata for: {infohash.hex()}')

        fetched = False
        conns_limit = trio.CapacityLimiter(MAX_CONNS_PER_FETCHER)
        total_conns_limit = trio.CapacityLimiter(MAX_TOTAL_CONNS)
        async def fetch(ih, ip, port, nursery):
            nonlocal fetched
            self.fetches_in_flight += 1
            try:
                async with conns_limit, total_conns_limit:
                    bt = Bittorrent(ip, port, ih)
                    await bt.run()
            finally:
                self.fetches_in_flight -= 1
            if bt.metadata is not None:
                logger.info(f'Obtained metadata for infohash: {ih.hex()}')
                await self.infohash_db.store_metadata(ih, bt.metadata)
                fetched = True
                nursery.cancel_scope.cancel()

        peer_send_chan, peer_recv_chan = trio.open_memory_channel(0)
        async with trio.open_nursery() as nursery:
            nursery.start_soon(dht.fetch_peers, infohash, peer_send_chan)
            for ip, port in initial_peers:
                nursery.start_soon(fetch, infohash, ip, port, nursery)
            async for ip, port in peer_recv_chan:
                nursery.start_soon(fetch, infohash, ip, port, nursery)

        peer_send_chan.close()
        peer_recv_chan.close()

        if not fetched:
            await self.infohash_db.mark_fetch_metadata_failure(infohash)


async def periodically_log_stats(stats_period, dht, fetcher):
    while True:
        await trio.sleep(stats_period)
        logger.info(
            f'Stats: rt-size={dht._routing_table.size()} '
            f'get_peers={dht.get_peers_in_flight} '
            f'gp_no_resp={dht.get_peers_no_response} '
            f'gp_error={dht.get_peers_errors} '
            f'gp_w_values={dht.get_peers_with_values} '
            f'gp_w_nodes={dht.get_peers_with_nodes} '
            f'fetches={fetcher.fetches_in_flight}'
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
        help='The postgres database to use. Defaults to "%(defaults)s".')

    args = parser.parse_args()

    config_logging(args.log_level)

    infohash_db = InfohashDb(args.database)
    seed_host, seed_port = args.seed
    dht = Dht(
        args.port,
        seed_host=seed_host,
        seed_port=seed_port,
        infohash_db=infohash_db,
        no_expand=False,
        no_index=True,
        readonly=True,
    )
    fetcher = MetadataFetcher(infohash_db, [dht])

    async with trio.open_nursery() as nursery:
        nursery.start_soon(dht.run)
        nursery.start_soon(fetcher.run)
        nursery.start_soon(infohash_db.run)

        if args.stats:
            nursery.start_soon(
                periodically_log_stats,
                args.stats_period, dht, fetcher)

if __name__ == '__main__':
    trio.run(main)
