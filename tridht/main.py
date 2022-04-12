import argparse
import logging
import signal
import json
import trio
from functools import partial
from urllib.parse import urlsplit
from .dht import Dht
from .routing_table import FullBucketRoutingTable
from .peer_table import PeerTable
from .database import Database
from .utils import config_logging, Tracer

logger = logging.getLogger(__name__)


def log_level_value(value):
    return value.upper()


async def write_state(filename, dhts):
    state = {
        'dhts': [
            dht.get_state()
            for dht in dhts
        ],
    }
    state = json.dumps(state).encode('utf-8')
    async with await trio.open_file(filename, 'wb') as f:
        await f.write(state)


async def load_state(args, dhts):
    try:
        async with await trio.open_file(args.state_file, 'rb') as f:
            state = await f.read()
    except FileNotFoundError:
        logger.debug(f'State file {args.state_file} does not exist.')
        return False, None, None, None, None

    state = json.loads(state)
    for dht_state, dht in zip(state['dhts'], dhts):
        dht.load_state(dht_state)

    if len(dhts) < len(state['dhts']):
        logger.warning(
            'Not enough DHT states in state files. Some nodes will not '
            'have their state loaded.')
    if len(dhts) > len(state['dhts']):
        logger.warning(
            'More DHT states in state file than DHT nodes started.')


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


async def periodically_save_state(args, dhts):
    while True:
        await trio.sleep(60)
        await write_state(args.state_file, dhts)


async def signal_handler(nursery, quit_event):
    signals_to_handle = [signal.SIGINT, signal.SIGTERM]
    with trio.open_signal_receiver(*signals_to_handle) as sig:
        async for signal_event in sig:
            logger.info('Received signal. Quitting...')
            quit_event.set()
            break


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

    parser.add_argument(
        '--database', '-d', default='postgresql+asyncpg:///tridht',
        help='The postgres database to use. Defaults to "%(default)s".')

    args = parser.parse_args()

    config_logging(args.log_level)

    RoutingTableClass = FullBucketRoutingTable
    PeerTableClass = PeerTable

    async with trio.open_nursery() as nursery:
        seed_host, seed_port = args.seed

        quit = trio.Event()
        nursery.start_soon(partial(signal_handler,
                                   quit_event=quit,
                                   nursery=nursery))

        db = Database(args.database)
        nursery.start_soon(db.run)
        await db.ready.wait()

        routing_table = RoutingTableClass(db)
        peer_table = PeerTableClass(db)
        dhts = [
            Dht(args.port + i,
                db=db,
                seed_host=seed_host,
                seed_port=seed_port,
                routing_table=routing_table,
                peer_table=peer_table,
            )
            for i in range(args.count)
        ]

        if not args.no_load_state:
            await load_state(args, dhts)

        routing_table.dht = dhts[0]

        if not args.no_save_state:
            nursery.start_soon(periodically_save_state, args, dhts)

        nursery.start_soon(routing_table.run)
        nursery.start_soon(peer_table.run)

        if args.stats:
            nursery.start_soon(
                periodically_log_stats, args.stats_period, dhts,
                routing_table, peer_table,
            )

        for dht in dhts:
            nursery.start_soon(dht.run)

        await quit.wait()

        logger.info('Stopping DHT nodes...')
        for dht in dhts:
            dht.stop()

        logger.info('Stopping routing table...')
        routing_table.stop()

        logger.info('Stopping peer table...')
        peer_table.stop()

        logger.info('Waiting for DHT nodes to stop...')
        for dht in dhts:
            await dht.stopped.wait()

        logger.info('Waiting for routing table to stop...')
        await routing_table.stopped.wait()

        logger.info('Waiting for peer table to stop...')
        await peer_table.stopped.wait()

        logger.info('Stopping database...')
        db.stop()

        logger.info('Waiting for database to stop...')
        await db.stopped.wait()

        logger.info('Canceling remaining tasks...')
        nursery.cancel_scope.cancel()

    logger.info('Writing state...')
    if not args.no_save_state:

        await write_state(args.state_file, dhts)

    logger.info('Done.')


if __name__ == '__main__':
    trio.run(main)
