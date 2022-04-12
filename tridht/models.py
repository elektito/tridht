from datetime import datetime, timedelta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import INET, JSONB, Insert
from sqlalchemy import (
    Column, LargeBinary, Integer, DateTime, Computed, Boolean,
)
from sqlalchemy import text, update, delete
from sqlalchemy.future import select
from .utils import metadata_to_json
from .bencode import bdecode
from .node import Node as DhtNode

Base = declarative_base()

class Infohash(Base):
    __tablename__ = 'infohashes'

    ih = Column(LargeBinary, primary_key=True)
    get_peers = Column(Integer, server_default='0', nullable=False)
    announces = Column(Integer, server_default='0', nullable=False)
    samples = Column(Integer, server_default='0', nullable=False)
    last_announce_time = Column(DateTime)
    last_announce_ip = Column(INET)
    last_announce_port = Column(Integer)
    metadata_ = Column('metadata', LargeBinary)
    fetch_due_time = Column(DateTime, nullable=False)
    fetch_failures = Column(Integer, server_default='0', nullable=False)
    score = Column(
        Integer,
        Computed('ih_score(get_peers, announces, samples, fetch_failures)'),
    )
    parsed_metadata = Column(JSONB)

    @staticmethod
    async def add(session, ih):
        one_hour_later = datetime.now() + timedelta(hours=1)
        await session.execute(
            Insert(Infohash)
            .values(ih=ih,
                    fetch_due_time=one_hour_later)
            .on_conflict_do_nothing()
        )

    @staticmethod
    async def aio_add_get_peers(session, ih):
        one_hour_later = datetime.now() + timedelta(hours=1)
        await session.execute(
            Insert(Infohash)
            .values(ih=ih,
                    get_peers=1,
                    fetch_due_time=one_hour_later)
            .on_conflict_do_update(
                constraint=Infohash.__table__.primary_key,
                set_={
                    'get_peers': Infohash.get_peers + 1,
                    'fetch_due_time': text(
                        '''case when infohashes.score >= 10
                              then now()
                              else infohashes.fetch_due_time
                           end'''),
                },
            )
        )

    @staticmethod
    async def aio_add_announce(session, ih, peer_ip, peer_port):
        now = datetime.now()
        await session.execute(
            Insert(Infohash)
            .values(ih=ih,
                    announces=1,
                    fetch_due_time=now,
                    last_announce_time=now,
                    last_announce_ip=peer_ip,
                    last_announce_port=peer_port)
            .on_conflict_do_update(
                constraint=Infohash.__table__.primary_key,
                set_={
                    'announces': Infohash.announces + 1,
                    'fetch_due_time': now,
                    'last_announce_time': now,
                    'last_announce_ip': peer_ip,
                    'last_announce_port': peer_port,
                },
            )
        )

    @staticmethod
    async def aio_add_sample(session, ih):
        one_hour_later = datetime.now() + timedelta(hours=1)
        await session.execute(
            Insert(Infohash)
            .values(ih=ih,
                    samples=1,
                    fetch_due_time=one_hour_later)
            .on_conflict_do_update(
                constraint=Infohash.__table__.primary_key,
                set_={
                    'samples': Infohash.samples + 1,
                    'fetch_due_time': text(
                        '''case when infohashes.score >= 10
                              then now()
                              else infohashes.fetch_due_time
                           end'''),
                },
            )
        )

    @staticmethod
    async def aio_store_metadata(session, ih, metadata):
        parsed_metadata, _ = bdecode(metadata)
        parsed_metadata = metadata_to_json(parsed_metadata)
        stmt = (
            update(Infohash)
            .filter_by(ih=ih)
            .values(metadata_=metadata,
                    parsed_metadata=parsed_metadata)
        )
        result = await session.execute(stmt)
        if result.rowcount == 0:
            # infohash was not in database
            await Infohash.add(session, ih)
            await session.execute(stmt)

    @staticmethod
    async def aio_mark_fetch_metadata_failure(session, ih):
        # calculate how long later we are going to retry
        stmt = (
            select(Infohash.fetch_failures)
            .where(Infohash.ih==ih)
        )
        result = list(await session.execute(stmt))
        if not result:
            # infohash was not in database
            await Infohash.add(session, ih)
            result = list(await session.execute(stmt))
            if not result:
                logger.warning(
                    'Could not add non-existing infohash to database.')
                return
        nfailures, = result[0]
        delay_hours = 2**(nfailures + 1)
        if delay_hours > 100000:
            delay_hours = 100000
        delay = timedelta(hours=delay_hours)

        stmt = (
            update(Infohash)
            .filter_by(ih=ih)
            .values(fetch_failures=Infohash.fetch_failures + 1,
                    fetch_due_time=Infohash.fetch_due_time + delay)
        )
        result = await session.execute(stmt)
        if result.rowcount == 0:
            # infohash was not in database
            await Infohash.add(session, ih)
            result = await session.execute(stmt)


class Node(Base):
    __tablename__ = 'nodes'

    id = Column(LargeBinary, primary_key=True)
    ip = Column(INET, nullable=False)
    port = Column(Integer, nullable=False)
    last_response_time = Column(DateTime)
    last_query_time = Column(DateTime)

    @staticmethod
    async def aio_add_nodes(session, nodes):
        for node in nodes:
            await session.execute(
                Insert(Node)
                .values(id=node.id,
                        ip=node.ip,
                        port=node.port,
                        last_response_time=node.last_response_time,
                        last_query_time=node.last_query_time)
                .on_conflict_do_update(
                    constraint=Node.__table__.primary_key,
                    set_={
                        'ip': node.ip,
                        'port': node.port,
                        'last_response_time': node.last_response_time,
                        'last_query_time': node.last_query_time,
                    },
                )
            )

    @staticmethod
    async def aio_del_nodes(session, nodes):
        node_ids = [n.id for n in nodes]
        await session.execute(
            delete(Node)
            .where(Node.id.in_(node_ids))
        )

    @staticmethod
    async def aio_get_all_nodes(session):
        db_nodes = await session.execute(
            select(Node.id,
                   Node.ip,
                   Node.port,
                   Node.last_query_time,
                   Node.last_response_time)
        )
        dht_nodes = []
        for id, ip, port, last_query_time, last_resp_time in db_nodes:
            node = DhtNode(id, ip, port)
            node.last_query_time = last_query_time
            node.last_response_time = last_resp_time
            dht_nodes.append(node)

        return dht_nodes


class Announce(Base):
    __tablename__ = 'announces'

    id = Column(Integer, primary_key=True)
    ih = Column(LargeBinary)
    node_id = Column(LargeBinary)
    time = Column(DateTime)
    peer_ip = Column(INET)
    peer_port = Column(Integer)

    @staticmethod
    async def aio_add_announce(session, ih, node_id, ip, port):
        await session.execute(
            Insert(Announce)
            .values(ih=ih,
                    node_id=node_id,
                    peer_ip=ip,
                    peer_port=port,
                    time=datetime.now())
        )

    @staticmethod
    async def aio_get_announces(session, age: timedelta):
        result = await session.execute(
            select(Announce)
            .where(Announce.time <= datetime.now() - age)
        )

        # unbind the objects from the session since we're only
        # interested in these for their data
        result = list(result.scalars())
        for obj in result:
            session.expunge(obj)

        return result
