import logging.config
from .node import Node


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
            'tridht.dht': {
                'handlers': ['default'],
                'level': 'WARNING',
                'propagate': True,
            },
            'tridht.routing_table': {
                'handlers': ['default'],
                'level': 'WARNING',
                'propagate': True,
            },
            '': {
                'handlers': ['default'],
                'level': log_level,
                'propagate': True,
            },
        }
    })


try:
    int.bit_count # python >= 3.10
except AttributeError:
    import warnings
    warnings.warn(
        'Using a slow popcount function. Consider upgrading to Python '
        '3.10 or later.')
    def hamming_distance(x, y):
        return bin(x ^ y).count('1')
else:
    def hamming_distance(x, y):
        return (x ^ y).bit_count()


def int_distance_to(value):
    if isinstance(value, bytes):
        value = int.from_bytes(value, byteorder='big')
    if isinstance(value, Node):
        value = node.intid
    def distance_func(int_node_id):
        return hamming_distance(int_node_id, value)
    return distance_func


def bytes_distance_to(value):
    if isinstance(value, bytes):
        value = int.from_bytes(value, byteorder='big')
    if isinstance(value, Node):
        value = node.intid
    def distance_func(node_id):
        int_nid = int.from_bytes(node_id, byteorder='big')
        return hamming_distance(int_nid, value)
    return distance_func


def node_distance_to(value):
    if isinstance(value, bytes):
        value = int.from_bytes(value, byteorder='big')
    if isinstance(value, Node):
        value = node.intid
    def distance_func(node):
        return hamming_distance(node.intid, value)
    return distance_func
