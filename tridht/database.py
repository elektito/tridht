import logging
import random
import trio
import trio_asyncio
from enum import Enum
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from datetime import datetime, timedelta
from . import models


logger = logging.getLogger(__name__)

class _Cmd(Enum):
    ADD_IH_GET_PEERS = 1
    ADD_IH_ANNOUNCE = 2
    ADD_IH_SAMPLE = 3
    STORE_METADATA = 4
    MARK_FETCH_METADATA_FAILURE = 5
    GET_SOME_DUE_INFOHASHES = 6
    ADD_NODES = 7
    DEL_NODES = 8
    GET_ALL_NODES = 9
    GET_ANNOUNCES = 10

class Database:
    def __init__(self, database=None):
        self.ready = trio.Event()
        self.stopped = trio.Event()
        self._quit = trio.Event()

        if database is None:
            database = 'postgresql+asyncpg:///tridht'

        self._engine = create_async_engine(database)
        self._session = None
        self._cmd_send, self._cmd_recv = trio.open_memory_channel(0)
        self._ret_send, self._ret_recv = trio.open_memory_channel(0)

    async def run(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._main_loop)

            await self._quit.wait()

            self._cmd_recv.close()

        self.stopped.set()

    async def _main_loop(self):
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with trio_asyncio.open_loop() as loop, \
                   trio_asyncio.aio_as_trio(async_session()) as session, \
                   trio.open_nursery() as nursery:
            self._session = session
            self.ready.set()
            try:
                async for cmd, *args in self._cmd_recv:
                    ret = await trio_asyncio.aio_as_trio(self._aio_process_cmd)(cmd, args)
                    await self._ret_send.send(ret)
                    await trio_asyncio.aio_as_trio(session.commit)()
            except trio.ClosedResourceError:
                pass

    def stop(self):
        self._quit.set()

    async def _aio_process_cmd(self, cmd, args):
        return await {
            _Cmd.ADD_IH_GET_PEERS: self._aio_process_add_ih_get_peers,
            _Cmd.ADD_IH_ANNOUNCE: self._aio_process_add_ih_announce,
            _Cmd.ADD_IH_SAMPLE: self._aio_process_add_ih_sample,
            _Cmd.STORE_METADATA: self._aio_process_store_metadata,
            _Cmd.MARK_FETCH_METADATA_FAILURE: (
                self._aio_process_mark_fetch_metadata_failure
            ),
            _Cmd.GET_SOME_DUE_INFOHASHES: (
                self._aio_process_get_some_due_infohashes
            ),
            _Cmd.ADD_NODES: self._aio_process_add_nodes,
            _Cmd.DEL_NODES: self._aio_process_del_nodes,
            _Cmd.GET_ALL_NODES: self._aio_process_get_all_nodes,
            _Cmd.GET_ANNOUNCES: self._aio_process_get_announces,
        }[cmd](*args)

    async def _aio_process_add_ih_get_peers(self, ih):
        return await models.Infohash.aio_add_get_peers(
            self._session, ih)

    async def _aio_process_add_ih_announce(self, ih, node_id, peer_ip,
                                           peer_port):
        await models.Infohash.aio_add_announce(
            self._session, ih, peer_ip, peer_port)
        await models.Announce.aio_add_announce(
            self._session, ih, node_id, peer_ip, peer_port)

    async def _aio_process_add_ih_sample(self, ih):
        return await models.Infohash.aio_add_sample(self._session, ih)

    async def _aio_process_store_metadata(self, ih, metadata):
        return await models.Infohash.aio_store_metadata(
            self._session, ih, metadata)

    async def _aio_process_mark_fetch_metadata_failure(self, ih):
        return await models.Infohash.aio_mark_fetch_metadata_failure(
            self._session, ih)

    async def _aio_process_get_some_due_infohashes(self):
        stmt = (
            select(models.Infohash.ih,
                   models.Infohash.last_announce_ip,
                   models.Infohash.last_announce_port)
            .where(models.Infohash.metadata_==None)
            .where(models.Infohash.fetch_due_time<=datetime.now())
            .order_by(models.Infohash.score.desc())
            .limit(1000)
        )
        results = await self._session.execute(stmt)
        results = list(results)
        results = random.sample(results, 10)
        return results

    async def _aio_process_add_nodes(self, nodes):
        return await models.Node.aio_add_nodes(self._session, nodes)

    async def _aio_process_del_nodes(self, nodes):
        return await models.Node.aio_del_nodes(self._session, nodes)

    async def _aio_process_get_all_nodes(self):
        return await models.Node.aio_get_all_nodes(self._session)

    async def _aio_process_get_announces(self, age: timedelta):
        return await models.Announce.aio_get_announces(
            self._session, age)

    async def _run_cmd(self, cmd, *args):
        await self._cmd_send.send((cmd, *args))
        return await self._ret_recv.receive()

    async def add_infohash_for_get_peers(self, ih):
        return await self._run_cmd(_Cmd.ADD_IH_GET_PEERS, ih)

    async def add_infohash_for_announce(self, ih, node_id, peer_ip, peer_port):
        return await self._run_cmd(
            _Cmd.ADD_IH_ANNOUNCE, ih, node_id, peer_ip, peer_port)

    async def add_infohash_for_sample(self, ih):
        return await self._run_cmd(_Cmd.ADD_IH_SAMPLE, ih)

    async def store_metadata(self, ih, metadata):
        return await self._run_cmd(_Cmd.STORE_METADATA, ih, metadata)

    async def mark_fetch_metadata_failure(self, ih):
        return await self._run_cmd(_Cmd.MARK_FETCH_METADATA_FAILURE, ih)

    async def get_some_due_infohashes(self):
        return await self._run_cmd(_Cmd.GET_SOME_DUE_INFOHASHES)

    async def add_nodes(self, nodes):
        return await self._run_cmd(_Cmd.ADD_NODES, nodes)

    async def del_nodes(self, nodes):
        return await self._run_cmd(_Cmd.DEL_NODES, nodes)

    async def get_all_nodes(self):
        return await self._run_cmd(_Cmd.GET_ALL_NODES)

    async def get_announces(self, age: timedelta):
        return await self._run_cmd(_Cmd.GET_ANNOUNCES, age)
