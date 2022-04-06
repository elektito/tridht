import time


class Node:
    def __init__(self, node_id, ip, port):
        self.id = node_id
        self.intid = int.from_bytes(self.id, byteorder='big')
        self.ip = ip
        self.port = port
        self.bad = False

        self.last_response_time = None
        self.last_query_time = None
        self.ever_responded = False

        self._precomputed_hash = self.intid % (2**64)

    def serialize(self):
        return {
            'id': self.id.hex(),
            'ip': self.ip,
            'port': self.port,
            'bad': self.bad,
            'last_response_time': self.last_response_time,
            'last_query_time': self.last_query_time,
            'ever_responded': self.ever_responded,
        }

    @classmethod
    def deserialize(cls, state):
        node = cls(bytes.fromhex(state['id']),
                   state['ip'],
                   state['port'])
        node.bad = state['bad']
        node.last_response_time = state['last_response_time']
        node.last_query_time = state['last_query_time']
        node.ever_responded = state['ever_responded']
        return node

    @property
    def good(self):
        # should we use wall clock time here?
        return (
            (self.last_response_time is not None and
             time.time() - self.last_response_time <= 15 * 60)
            or
            (self.ever_responded and
             self.last_query_time is not None and
             self.last_query_time <= 15 * 60)
        )

    @property
    def questionable(self):
        return not self.good and not self.bad

    def __hash__(self):
        return self._precomputed_hash

    def __eq__(self, other):
        return self.id == other.id

    def __str__(self):
        return f'<Node id={self.id.hex()}>'
