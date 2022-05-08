import trio
import argparse
import pgtrio
from pprint import pprint
from tridht.bencode import bdecode
from tridht.utils import metadata_to_json


def fmt_value(v):
    if isinstance(v, int):
        return v
    if isinstance(v, list):
        return [
            (metadata_to_json(i) if isinstance(i, dict) else str(i))
            for i in v
        ]
    if isinstance(v, dict):
        return metadata_to_json(v)

    try:
        v = v.decode('utf-8')
    except UnicodeDecodeError:
        v = b''.join((bytes([c]) if 32 <= c < 128 else b'-') for c in v)
        v = v.decode('ascii')

    if len(v) > 200:
        return f'<...{len(v)} bytes...>'

    return v


def fmt_files(v):
    ret = ''
    for f in v:
        try:
            path = '/'.join(i.decode('utf-8') for i in f[b'path'])
        except UnicodeDecodeError:
            path = '/'.join(str(i) for i in f[b'path'])
        ret += f' - {path}: {f[b"length"]}\n'
    return ret.rstrip('\n')


async def main():
    parser = argparse.ArgumentParser(
        description='Read torrent metadata from the database.')

    parser.add_argument(
        'infohash', nargs='?',
        help='The torrent infohash of the metadata to read. If not '
        'specified, a list of available infohashes with metadata is '
        'displayed.')

    parser.add_argument(
        '--database', '-d', default='postgresql:///tridht',
        help='The postgres database to use. Defaults to "%(default)s".')

    parser.add_argument(
        '--json', '-j', action='store_true',
        help='Also print the json form of the metadata as returned by '
        'the metadata_to_json function.')

    args = parser.parse_args()

    async with pgtrio.connect(args.database) as conn:
        if not args.infohash:
            result = await conn.execute(
                'select ih from infohashes where metadata is not null')
            for ih, in result:
                print(ih.hex())
            return

        ih = bytes.fromhex(args.infohash)
        result = await conn.execute(
            'select metadata from infohashes where ih = $1', ih)
        if not result:
            print('Infohash not found in the database.')
            return

        metadata, = result[0]
        metadata, _ = bdecode(bytes(metadata))

        print(f'# {args.infohash}')

        for k, v in metadata.items():
            if k == b'files':
                print('files:')
                print(fmt_files(v))
            else:
                print(f'{k.decode("ascii")}: {fmt_value(v)}')

        if args.json:
            print('\nJSON:')
            pprint(metadata_to_json(metadata))


if __name__ == '__main__':
    trio.run(main)
