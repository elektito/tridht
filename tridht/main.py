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

    args = parser.parse_args()

    config_logging(args.log_level)

    async with trio.open_nursery() as nursery:
        seed_host, seed_port = args.seed
        dht = Dht(6881,
                  seed_host=seed_host,
                  seed_port=seed_port)
        nursery.start_soon(dht.run)
    logger.info('Done.')


if __name__ == '__main__':
    trio.run(main)
