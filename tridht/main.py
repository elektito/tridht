import argparse
import logging
import logging.config
import random
import signal
import time
import json
import trio
from functools import partial
from urllib.parse import urlsplit
from trio import socket
from .bencode import bencode, bdecode, BDecodingError
from .dht import Dht
from .routing_table import FullBucketRoutingTable
from .peer_table import PeerTable
from .infohash_db import InfohashDb
from .utils import config_logging

logger = logging.getLogger(__name__)


def log_level_value(value):
    return value.upper()


async def write_state(filename, dhts, routing_table, peer_table,
                      infohash_db):
    state = {
        'rt': routing_table.serialize(),
        'pt': peer_table.serialize(),
        'ihdb': infohash_db.serialize(),
        'dhts': [
            dht.serialize(serialize_routing_table=False,
                          serialize_peer_table=False)
            for dht in dhts
        ],
    }
    state = json.dumps(state).encode('utf-8')
    async with await trio.open_file(filename, 'wb') as f:
        await f.write(state)


async def load_state(args, RoutingTableClass, PeerTableClass,
                     InfohashDbClass):
    try:
        async with await trio.open_file(args.state_file, 'rb') as f:
            state = await f.read()
    except FileNotFoundError:
        logger.debug(f'State file {args.state_file} does not exist.')
        return False, None, None, None, None

    state = json.loads(state)
    rt = RoutingTableClass.deserialize(state['rt'])
    pt = PeerTableClass.deserialize(state['pt'])
    ihdb = InfohashDbClass.deserialize(state.get('ihdb'))
    dhts = [
        Dht.deserialize(dht_state)
        for dht_state in state['dhts']
    ]

    if len(dhts) != args.count:
        logger.warning(
            f'Number of DHTs in saved state ({len(dhts)}) does not '
            f'match requested count ({args.count}). Ignoring saved '
            f'state.')
        return False, None, None, None, None

    ports = range(args.port, args.port + args.count)
    for dht, port in zip(dhts, ports):
        dht.routing_table = rt
        dht.peer_table = pt
        dht.infohash_db = ihdb
        dht.port = port

    return True, rt, pt, ihdb, dhts


def node(value):
    result = urlsplit('//' + value)
    host, port = result.hostname, result.port
    if port is None:
        port = 6881
    return host, port


async def periodically_log_stats(stats_period, dhts, routing_table,
                                 peer_table):
    while True:
        await trio.sleep(stats_period)
        dht_stats = ' '.join(
            f'{k}={v}'for k, v in dhts[0].stats.items())
        logger.info(
            f'Stats: rt-size={routing_table.size()} '
            f'pt-size={peer_table.size()} ' +
            dht_stats
        )


async def periodically_save_state(args, dhts, routing_table,
                                  peer_table, infohash_db):
    while True:
        await trio.sleep(60)
        await write_state(
            args.state_file, dhts, routing_table, peer_table,
            infohash_db)


async def signal_handler(nursery):
    signals_to_handle = [signal.SIGINT, signal.SIGTERM]
    with trio.open_signal_receiver(*signals_to_handle) as sig:
        async for signal_event in sig:
            logger.info('Received signal. Quitting...')
            nursery.cancel_scope.cancel()


async def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--log-level', '-l', default='INFO', type=log_level_value,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the log level. Defaults to %(default)s.')

    parser.add_argument(
        '--seed', '-s', default='router.bittorrent.com:6881', type=node,
        help='The node to seed the routing table from. We will send a '
        'find_node query to this node with a random node id and '
        'attempt getting more nodes. It will then be removed from the '
        'routing table. If no port is specified, 6881 is assumed. '
        'Defaults to %(default)s.')

    parser.add_argument(
        '--port', '-p', default=6881, type=int,
        help='The port to bind to. Defaults to %(default)s. If --count '
        'is set to a number greater than 1, consecutive ports after '
        'this will be used.')

    parser.add_argument(
        '--stats', action='store_true', default=False,
        help='Periodically log some stats.')

    parser.add_argument(
        '--stats-period', type=int, default=60,
        help='The period in which stats are logged when --stats is '
        'set. Defaults to %(default)s seconds.')

    parser.add_argument(
        '--count', '-n', type=int, default=1,
        help='Number of DHT instances to run.')

    parser.add_argument(
        '--no-load-state', action='store_true', default=False,
        help='Do not load state on startup.')

    parser.add_argument(
        '--no-save-state', action='store_true', default=False,
        help='Do not save state.')

    parser.add_argument(
        '--state-file', default='tridht-state.json',
        help='The file to write the state to and read it from.')

    args = parser.parse_args()

    config_logging(args.log_level)

    RoutingTableClass = FullBucketRoutingTable
    PeerTableClass = PeerTable
    InfohashDbClass = InfohashDb

    async with trio.open_nursery() as nursery:
        seed_host, seed_port = args.seed

        nursery.start_soon(partial(signal_handler, nursery=nursery))

        if not args.no_load_state:
            (
                have_state, routing_table, peer_table, infohash_db, dhts
            ) = await load_state(
                args, RoutingTableClass, PeerTableClass, InfohashDb)
        else:
            have_state = False

        if not have_state:
            routing_table = RoutingTableClass()
            peer_table = PeerTableClass()
            infohash_db = InfohashDbClass()
            dhts = [
                Dht(args.port + i,
                    seed_host=seed_host,
                    seed_port=seed_port,
                    routing_table=routing_table,
                    peer_table=peer_table,
                    infohash_db=infohash_db,
                )
                for i in range(args.count)
            ]
        else:
            logger.info('Using saved state.')

        routing_table.dht = dhts[0]

        if not args.no_save_state:
            nursery.start_soon(
                periodically_save_state,
                args, dhts, routing_table, peer_table, infohash_db)

        nursery.start_soon(routing_table.run)
        nursery.start_soon(peer_table.run)
        nursery.start_soon(infohash_db.run)

        if args.stats:
            nursery.start_soon(
                periodically_log_stats, args.stats_period, dhts,
                routing_table, peer_table,
            )

        for dht in dhts:
            nursery.start_soon(dht.run)

    logger.info('Writing state...')
    if not args.no_save_state:
        await write_state(
            args.state_file, dhts, routing_table, peer_table,
            infohash_db)

    logger.info('Done.')


if __name__ == '__main__':
    trio.run(main)
