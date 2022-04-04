import logging.config


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
