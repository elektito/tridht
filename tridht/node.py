import time


class Node:
    def __init__(self, node_id, ip, port):
        self.id = node_id
        self.intid = int.from_bytes(self.id, byteorder='big')
        self.ip = ip
        self.port = port

        self.last_response_time = None
        self.last_query_time = None
        self.ever_responded = False

    def is_good(self):
        # should we use wall clock time here?
        return (
            (self.last_response_time is not None and
             time.time() - self.last_response_time <= 15 * 60)
            or
            (self.ever_responded and
             self.last_query_time is not None and
             self.last_query_time <= 15 * 60)
        )

    def __hash__(self):
        return self.intid % (2**64)

    def __eq__(self, other):
        return self.id == other.id

    def __str__(self):
        return f'<Node id={self.id.hex()}>'
