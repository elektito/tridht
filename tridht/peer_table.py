import time
import random
import trio
from collections import defaultdict

K = 8

class PeerTable:
    def __init__(self):
        self._peer_timeout = 24 * 3600
        self._peers = defaultdict(list)
        self._update_times = {}

    def announce(self, info_hash, ip, port):
        self._peers[info_hash].append((ip, port))
        self._update_times[ip, port] = time.time()

    def get_peers(self, info_hash):
        peers = self._peers.get(info_hash)
        if peers is None:
            return []
        if len(peers) < K:
            return peers
        else:
            return random.sample(peers, K)

    def size(self):
        return len(self._peers)

    def serialize(self):
        return {
            'peers': {
                ih.hex(): [[ip, port] for ip, port in peers]
                for ih, peers in self._peers.items()
            },
            'update_times': {
                f'{ip},{port}': value
                for (ip, port), value in self._update_times.items()
            }
        }

    @classmethod
    def deserialize(cls, state):
        pt = cls()
        pt._peers = defaultdict(list, {
            bytes.fromhex(ih): [(ip, port) for ip, port in peers]
            for ih, peers in state['peers'].items()
        })
        pt._update_times = {
            (k.split(',')[0], int(k.split(',')[1])): v
            for k, v in state['update_times'].items()
        }
        return pt

    async def run(self):
        while True:
            now = time.time()
            to_delete = []
            for (ip, port), update_time in self._update_times.items():
                if now - update_time > self._peer_timeout:
                    to_delete.append((ip, port))
            for ip, port in to_delete:
                del self._update_times[ip, port]
                for ih in self._peers:
                    self._peers[ih].remove((ip, port))

            await trio.sleep(60)
