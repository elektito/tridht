import argparse
import logging
import logging.config
import random
import time
import trio
from urllib.parse import urlsplit
from trio import socket
from .bencode import bencode, bdecode, BDecodingError
from .dht import Dht
from .routing_table import FullRoutingTable
from .peer_table import PeerTable

logger = logging.getLogger(__name__)

def config_logging(log_level):
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            }
        },
        'handlers': {
            'default': {
                'level': log_level,
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            }
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': log_level,
                'propagate': True,
            },
        }
    })


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

    args = parser.parse_args()

    config_logging(args.log_level)

    async with trio.open_nursery() as nursery:
        seed_host, seed_port = args.seed

        routing_table = FullRoutingTable()
        peer_table = PeerTable()
        dhts = [
            Dht(args.port + i,
                seed_host=seed_host,
                seed_port=seed_port,
                log_stats=args.stats,
                log_stats_period=args.stats_period,
                routing_table=routing_table,
                peer_table=peer_table,
            )
            for i in range(args.count)
        ]
        routing_table.dht = dhts[0]
        nursery.start_soon(routing_table.run)
        nursery.start_soon(peer_table.run)

        for dht in dhts:
            nursery.start_soon(dht.run)

    logger.info('Done.')


if __name__ == '__main__':
    trio.run(main)
