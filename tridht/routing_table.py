import time
import logging
import trio
from ipaddress import IPv4Address
from .node import Node

K = 8
logger = logging.getLogger(__name__)


class BaseRoutingTable:
    def __init__(self, dht):
        self.dht = dht

    async def run(self):
        while not self.dht or not self.dht.started:
            await trio.sleep(1)

        while True:
            to_remove = []
            for node in self.get_all_nodes():
                if node.questionable:
                    self.dht.check_node_goodness(node)
                    continue
                if node.bad:
                    logger.info(f'Removing bad node: {node.id.hex()}')
                    to_remove.append(node)

            for node in to_remove:
                self.remove(node)

            await trio.sleep(15 * 60)

    def serialize(self):
        return {
            'nodes': [n.serialize() for n in self.get_all_nodes()],
        }

    @classmethod
    def deserialize(cls, state):
        rt = cls(None)
        for node_state in state['nodes']:
            node = Node.deserialize(node_state)
            rt.add_node(node)
        return rt

    def add_or_update_node(self, node_id, node_ip, node_port,
                           interaction):
        node = self.find_node(node_id)

        should_add_node = False
        if node is None:
            node = Node(node_id, node_ip, node_port)
            should_add_node = True

        if interaction == 'query':
            node.last_query_time = time.time()
        elif interaction == 'response_to_query':
            node.last_response_time = time.time()
            node.ever_responded = True
            node.bad = False

        if node.ip != node_ip:
            logger.info(
                f'Updating IP address of node {node_id.hex()} '
                f'from {node.ip} to {node_ip}')
            node.ip = node_ip
        if node.port != node_port:
            logger.info(
                f'Updating port of node {node_id.hex()} from '
                f'{node.port} to {node_port}')
            node.port = node_port

        if should_add_node:
            self.add_node(node)

    def get_close_nodes(self, node_id, compact=False):
        distances = {}
        node_id = int.from_bytes(node_id,
                                 byteorder='big',
                                 signed=False)
        for node in self.get_all_nodes():
            distance = bin(node.intid ^ node_id).count('1')
            distances[node] = distance

        distances = list(distances.items())
        distances.sort(key=lambda r: r[1])
        distances = distances[:K]

        nodes = b'' if compact else []
        for node, dist in distances:
            if compact:
                nodes += (
                    node.id +
                    IPv4Address(node.ip).packed +
                    node.port.to_bytes(length=2,
                                       byteorder='big',
                                       signed=False)
                )
            else:
                nodes.append(node)

        return nodes

    def add_node(self, node):
        raise NotImplementedError

    def find_node(self, node_id=None, node_ip=None, node_port=None):
        raise NotImplementedError

    def remove(self, node):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def get_all_nodes(self):
        raise NotImplementedError


class BucketRoutingTable(BaseRoutingTable):
    """A Routing Table implementation close to BEP 5 description."""
    def __init__(self, dht=None, min_id=0, max_id=2**160):
        super().__init__(dht)

        self.min_id = min_id
        self.max_id = max_id

        self._nodes = {}
        self._first_half = None
        self._second_half = None

    def add_node(self, node):
        if self._is_split:
            if self._first_half._node_fits(node):
                return self._first_half.add_node(node)
            else:
                return self._second_half.add_node(node)
        else:
            if len(self._nodes) < K:
                prev_size = len(self._nodes)
                self._nodes[node.id] = node
                if len(self._nodes) > prev_size:
                    logger.info(
                        f'Added node to routing table: {node.id.hex()}')
                    return True
                else:
                    logger.debug(
                        'Node %s was not added to the routing table '
                        'because it was already there.',
                        node.id.hex())
                    return False

            if self._node_fits(self.dht.node_id):
                if self.max_id - self.min_id <= K:
                    logger.info(
                        f'Not adding node {node.id.hex()} because '
                        'bucket cannot be split any further.')
                    return False

                self._split()
                return self.add_node(node)
            else:
                bad_node = None
                for n in self._nodes.values():
                    if n.bad:
                        bad_node = n
                        break
                if bad_node is not None:
                    logger.debug(
                        f'Replacing bad node {bad_node.id.hex()} with '
                        f'new node {node.id.hex()}.')
                    del self._nodes[bad_node.id]
                    self._nodes[node.id] = node
                    return
                self.dht.retry_add_node_after_refresh(
                    node, self._nodes.values())

    def find_node(self, node_id=None, node_ip=None, node_port=None):
        if self._is_split:
            node = self._first_half.find_node(
                node_id, node_ip, node_port)
            if node:
                return node
            return self._first_half.find_node(
                node_id, node_ip, node_port)
        else:
            if node_id:
                return self._nodes.get(node_id)
            else:
                for node in self._nodes:
                    if node.ip == node_ip and node.port == node_port:
                        return node
                return None

    def remove(self, node):
        if self._is_split:
            self._first_half.remove(node)
            self._second_half.remove(node)
        else:
            try:
                self._nodes.remove(node)
            except KeyError:
                pass

    def clear(self):
        self._first_half = None
        self._second_half = None
        self._nodes = {}

    def size(self):
        if self._is_split:
            return self._first_half.size() + self._second_half.size()
        else:
            return len(self._nodes)

    def get_all_nodes(self):
        if self._is_split:
            yield from self._first_half.get_all_nodes()
            yield from self._second_half.get_all_nodes()
        else:
            yield from iter(self._nodes.values())

    def _node_fits(self, node):
        if isinstance(node, Node):
            node_id = node.id
        else:
            node_id = node
        if isinstance(node_id, bytes):
            node_id = int.from_bytes(node_id, byteorder='big')
        return (self.min_id <= node_id < self.max_id)

    def _split(self):
        middle = self.min_id + (self.max_id - self.min_id) // 2
        self._first_half = BucketRoutingTable(
            self.dht, self.min_id, middle)
        self._second_half = BucketRoutingTable(
            self.dht, middle, self.max_id)
        for node in self._nodes.values():
            if self._first_half._node_fits(node):
                self._first_half.add_node(node)
            else:
                self._second_half.add_node(node)
        self._nodes = {}

    @property
    def _is_split(self):
        return self._first_half is not None

    def __str__(self):
        if self._is_split:
            return f'<RT SP 1={self._first_half} 2={self._second_half}>'
        else:
            return f'<RT NSP nodes={len(self._nodes)}>'


class FullRoutingTable(BaseRoutingTable):
    """A Routing Table implementation that keeps all nodes added to
it. There are no buckets."""

    def __init__(self, dht=None):
        super().__init__(dht)

        self._nodes = {}

    def add_node(self, node):
        prev_size = len(self._nodes)
        self._nodes[node.id] = node
        if len(self._nodes) > prev_size:
            logger.debug(
                f'Added node to routing table: {node.id.hex()}')
            return True
        else:
            logger.debug(
                'Node %s was not added to the routing table '
                'because it was already there.',
                node.id.hex())
            return False

    def find_node(self, node_id=None, node_ip=None, node_port=None):
        if node_id:
            return self._nodes.get(node_id)
        else:
            for node in self._nodes.values():
                if node.ip == node_ip and node.port == node_port:
                    return node
            return None

    def remove(self, node):
        try:
            del self._nodes[node.id]
        except KeyError:
            pass

    def clear(self):
        self._nodes = {}

    def size(self):
        return len(self._nodes)

    def get_all_nodes(self):
        return self._nodes.values()
