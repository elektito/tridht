import logging
import random
import trio
import pgtrio
from urllib.parse import urlparse
from enum import Enum
from datetime import datetime, timedelta
from collections import namedtuple
from .bencode import bdecode
from .node import Node
from .utils import metadata_to_json


logger = logging.getLogger(__name__)

AnnounceTuple = namedtuple('AnnounceTuple',
                           ['ih', 'peer_ip', 'peer_port'])


class Database:
    def __init__(self, database=None):
        self.ready = trio.Event()
        self.stopped = trio.Event()
        self._quit = trio.Event()

        if database is None:
            database = 'postgresql:///tridht'
        if '://' not in database:
            database = 'postgresql:///' + database

        url = urlparse(database)
        if url.scheme != 'postgresql':
            raise ValueError('Database URL scheme should be "postgresql"')
        if not url.path:
            raise ValueError('No database name in database URL')
        assert url.path.startswith('/')

        self._db_name = url.path[1:]
        self._db_host = url.hostname
        self._db_port = url.port
        self._db_username = url.username
        self._db_password = url.password

    async def run(self):
        async with pgtrio.create_pool(
                self._db_name,
                host=self._db_host,
                port=self._db_port,
                username=self._db_username,
                password=self._db_password,
        ) as self._pool:
            self.ready.set()
            await self._quit.wait()

        self.stopped.set()

    def stop(self):
        self._quit.set()

    async def add_infohash_for_get_peers(self, ih):
        async with self._pool.acquire() as conn:
            await conn.execute(
                '''
                insert into infohashes (ih, get_peers, fetch_due_time)
                values ($1, 1, now() + interval '1 hour')
                on conflict (ih) do update set
                get_peers = infohashes.get_peers + 1,
                fetch_due_time = case
                    when infohashes.score >= 10
                    then now()
                    else infohashes.fetch_due_time
                    end
                ''',
                ih,
            )

    async def add_infohash_for_announce(self, ih, node_id, peer_ip, peer_port):
        async with self._pool.acquire() as conn:
            await conn.execute(
                '''
                insert into infohashes (
                    ih,
                    announces,
                    fetch_due_time,
                    last_announce_time,
                    last_announce_ip,
                    last_announce_port
                )
                values ($1, 1, now(), now(), $2, $3)
                on conflict (ih) do update set
                announces = infohashes.announces + 1,
                fetch_due_time = now(),
                last_announce_time = now(),
                last_announce_ip = $2,
                last_announce_port = $3
                ''',
                ih, peer_ip, peer_port
            )

            await conn.execute(
                '''
                insert into announces (
                    ih,
                    node_id,
                    peer_ip,
                    peer_port,
                    time
                )
                values ($1, $2, $3, $4, now())
                ''',
                ih, node_id, peer_ip, peer_port,
            )

    async def add_infohash_for_sample(self, ih):
        async with self._pool.acquire() as conn:
            await conn.execute(
                '''
                insert into infohashes (ih, samples, fetch_due_time)
                values ($1, 1, now() + interval '1 hour')
                on conflict (ih) do update set
                samples = infohashes.samples + 1,
                fetch_due_time = case
                    when infohashes.score >= 10
                    then now()
                    else infohashes.fetch_due_time
                    end
                ''',
                ih,
            )

    async def store_metadata(self, ih, metadata):
        parsed_metadata, _ = bdecode(metadata)
        parsed_metadata = metadata_to_json(parsed_metadata)
        async with self._pool.acquire() as conn:
            stmt = await conn.prepare(
                '''
                update infohashes
                set
                    metadata = $1,
                    parsed_metadata = $2
                where ih = $3
                '''
            )
            await stmt.execute(metadata, parsed_metadata, ih)
            if stmt.rowcount == 0:
                # infohash was not in database
                await self._add_infohash(ih, conn)
                await stmt.execute(metadata, parsed_metadata, ih)

    async def mark_fetch_metadata_failure(self, ih):
        async with self._pool.acquire() as conn:
            stmt = await conn.prepare(
                'select fetch_failures from infohashes where ih=$1')
            result = await stmt.execute(ih)
            if not result:
                await self._add_infohash(ih, conn)
                result = await stmt.execute(ih)
                if not result:
                    logger.warning(
                        'Could not add non-existing infohash to '
                        'database.')
                    return

            nfailures, = result[0]
            delay_hours = 2**(nfailures + 1)
            if delay_hours > 100000:
                delay_hours = 100000
            delay = timedelta(hours=delay_hours)

            stmt = await conn.prepare(
                '''
                update infohashes set
                    fetch_failures = infohashes.fetch_failures + 1,
                    fetch_due_time = infohashes.fetch_due_time + $1
                where ih = $2
                '''
            )
            result = await stmt.execute(delay, ih)
            if stmt.rowcount == 0:
                await self._add_infohash(ih, conn)
                await stmt.execute(delay, ih)

    async def get_some_due_infohashes(self):
        async with self._pool.acquire() as conn:
            results = await conn.execute(
                '''
                select ih, last_announce_ip, last_announce_port
                from infohashes
                where fetch_due_time <= now() and metadata is null
                order by score desc
                limit 1000
                '''
            )
            if len(results) > 10:
                results = random.sample(results, 10)
            return results

    async def add_nodes(self, nodes):
        if len(nodes) == 0:
            return

        # this (possibly) stops deadlocks that can happen between
        # multiple instances of DHT nodes in different processes. See:
        # https://stackoverflow.com/a/59018315/363949
        nodes = sorted(nodes, key=lambda node: node.id)

        async with self._pool.acquire() as conn:
            args = []
            for node in nodes:
                values_caluses = ', '.join(
                    f'(${i}, ${i+1}, ${i+2}, ${i+3}, ${i+4})'
                    for i in range(1, len(nodes) * 5 + 1, 5)
                )
                args.extend([
                    node.id,
                    node.ip,
                    node.port,
                    node.last_response_time,
                    node.last_query_time,
                ])
            await conn.execute(
                f'''
                insert into nodes (
                    id,
                    ip,
                    port,
                    last_response_time,
                    last_query_time
                )
                values {values_caluses}
                on conflict (id) do update
                set
                    ip = excluded.ip,
                    port = excluded.port,
                    last_response_time = excluded.last_response_time,
                    last_query_time = excluded.last_query_time
                ''',
                *args,
            )

    async def del_nodes(self, nodes):
        node_ids = [n.id for n in nodes]
        async with self._pool.acquire() as conn:
            await conn.execute(
                'delete from nodes where id = any($1)', node_ids)

    async def get_all_nodes(self, limit=None):
        async with self._pool.acquire() as conn:
            # Note: passing NULL for the limit clause means limit
            # everything (like "LIMIT ALL" or not limit clause at all)
            db_nodes = await conn.execute(
                '''
                select id, ip, port, last_query_time, last_response_time
                from nodes limit $1
                ''',
                limit,
            )
        dht_nodes = []
        for id, ip, port, last_query_time, last_resp_time in db_nodes:
            node = Node(id, ip, port)
            node.last_query_time = last_query_time
            node.last_response_time = last_resp_time
            dht_nodes.append(node)

        return dht_nodes

    async def get_announces(self, age: timedelta):
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                '''
                select ih, peer_ip, peer_port
                from announces
                where time > now() - $1::interval
                ''',
                age,
                tuple_class=AnnounceTuple,
            )

        return result

    async def _add_infohash(self, ih, conn):
        await conn.execute(
            '''
            insert into infohashes (ih, fetch_due_time)
            values ($1, now() + interval '1 hour')''',
            ih,
        )
