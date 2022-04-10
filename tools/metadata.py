import argparse
from pprint import pprint
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tridht.bencode import bdecode
from tridht.utils import metadata_to_json


def fmt_value(v):
    if isinstance(v, int):
        return v
    if isinstance(v, list):
        return str(v)
    if isinstance(v, dict):
        return f'{{dict with {len(v)} key(s)}}'

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


def main():
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

    engine = create_engine(args.database)
    session = sessionmaker(engine)()

    if not args.infohash:
        result = session.execute('select ih from infohashes where metadata is not null')
        for ih, in result.all():
            print(ih.hex())
        return

    ih = bytes.fromhex(args.infohash)

    metadata, = session.execute(
        'select metadata from infohashes where ih = :ih',
        {'ih': ih}
    ).one_or_none()

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
    main()
