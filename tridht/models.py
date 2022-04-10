from datetime import datetime, timedelta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import INET, JSONB, Insert
from sqlalchemy import Column, LargeBinary, Integer, DateTime, Computed
from sqlalchemy import text, update
from sqlalchemy.future import select
from .utils import metadata_to_json
from .bencode import bdecode

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
