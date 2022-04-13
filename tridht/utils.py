import logging.config
import trio
import chardet
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
                'level': log_level,
                'propagate': False,
            },
            'tridht.routing_table': {
                'handlers': ['default'],
                'level': log_level,
                'propagate': False,
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


def metadata_to_json(metadata):
    """Converts the given metadata to json. Metadata should already be
    bdecoded.

    """
    def decode(v):
        def force_ascii(v):
            v = bytes((b if 32 <= b < 128 else ord(b'-')) for b in v)
            return v.decode('ascii')

        if v is None:
            return None
        try:
            return v.decode('utf-8')
        except UnicodeDecodeError:
            pass

        det = chardet.detect(v)
        enc = det['encoding']
        if not enc:
            return force_ascii(v)

        try:
            return v.decode(enc)
        except UnicodeDecodeError:
            return force_ascii(v)

    def get_name():
        name_utf8 = decode(metadata.get(b'name.utf-8'))
        name = decode(metadata.get(b'name'))
        return name_utf8 or name

    def get_sha256():
        sha256 = metadata.get(b'sha256')
        if not sha256:
            return None
        return sha256.hex()

    def get_files():
        files = metadata.get(b'files')
        if not isinstance(files, list) or files == []:
            return None

        try:
            return [
                {
                    'path': '/'.join(decode(i) for i in f[b'path']),
                    'length': f[b'length'],
                }
                for f in files
            ]
        except (KeyError, UnicodeDecodeError):
            return None

    def get_json_value(value):
        if isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                return None

        if isinstance(value, int):
            return value

        if isinstance(value, list):
            return [get_json_value(i) for i in value]

        if isinstance(value, dict):
            ret = {}
            for k, v in value.items():
                try:
                    k = k.decode('utf-8')
                except UnicodeDecodeError:
                    continue

                v = get_json_value(v)
                if v is None:
                    continue

                ret[k] = v

            return ret or None

        raise ValueError

    if not isinstance(metadata, dict):
        raise ValueError(
            'metadata should be a dict (the output of bdecode)')

    name = get_name()
    files = get_files()
    sha256 = get_sha256()
    ret = {}
    if name:
        ret['name'] = name
    if files:
        ret['files'] = files
    if sha256:
        ret['sha256'] = sha256

    ignore = [
        b'files',
        b'name',
        b'name.utf-8',
        b'pieces',
        b'sha256',
    ]

    for k, v in metadata.items():
        if k in ignore:
            continue

        try:
            k = k.decode('utf-8')
        except UnicodeDecodeError:
            continue

        v = get_json_value(v)
        if v:
            ret[k] = v

    # some keys have a variant with a .utf-8 suffix. for those, prefer
    # the utf-8 version.
    to_fix = []
    for k in ret:
        if k.endswith('.utf-8'):
            to_fix.append(k)
    for k in to_fix:
        # this is for catching corner cases involving multiple .utf-8
        # suffixes
        if k not in ret:
            continue

        non_utf8_key = k[:-len('.utf-8')]
        value = ret[k]

        del ret[k]
        try:
            del ret[non_utf8_key]
        except KeyError:
            pass

        ret[non_utf8_key] = value

    return ret


async def launch_limited(async_fn, *args,
                         nursery, semaphore, name=None):
    # launch the given function in the given nursery, limited to the
    # given semaphore. the task will only be launched after the
    # semaphore is acquired.

    async def _run(task_status):
        await semaphore.acquire()

        try:
            task_status.started()
            await async_fn(*args)
        finally:
            semaphore.release()

    await nursery.start(_run, name=name)
