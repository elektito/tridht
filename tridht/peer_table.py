import time
import random
import trio
from datetime import timedelta, datetime
from collections import defaultdict

K = 8

class PeerTable:
    def __init__(self, db):
        self.ready = trio.Event()
        self.stopped = trio.Event()

        self._db = db
        self._quit = trio.Event()
        self._peer_timeout = 24 * 3600
        self._announce_age = timedelta(days=1)
        self._peers = defaultdict(set)
        self._update_times = {}

    async def announce(self, info_hash, node_id, ip, port):
        self._peers[info_hash].add((ip, port))
        self._update_times[ip, port] = time.time()

        await self._db.add_infohash_for_announce(
            info_hash, node_id, ip, port)

    def get_peers(self, info_hash):
        peers = self._peers.get(info_hash)
        if peers is None:
            return []
        if len(peers) < K:
            return peers
        else:
            return random.sample(peers, K)

    def get_sample(self, sample_size, compact=False):
        if len(self._peers) <= sample_size:
            sample = list(self._peers)
        else:
            sample = random.sample(list(self._peers), sample_size)
        if compact:
            sample = b''.join(sample)
        return sample

    def size(self):
        return len(self._peers)

    def stop(self):
        self._quit.set()

    async def run(self):
        await self._load()
        self.ready.set()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._periodically_reload)

            await self._quit.wait()

            nursery.cancel_scope.cancel()

        self.stopped.set()

    async def _periodically_reload(self):
        while True:
            await trio.sleep(60)

            # we reload peers periodically and rebuild peer table so
            # that we get any updates from other instances, and also
            # expire old announces
            await self._load()

    async def _load(self):
        self._announces = await self._db.get_announces(
            age=self._announce_age)
        self._peers = defaultdict(set)
        for a in self._announces:
            self._peers[a.ih].add((a.peer_ip, a.peer_port))
