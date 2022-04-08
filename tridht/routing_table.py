import time
import logging
import trio
from ipaddress import IPv4Address
from .node import Node
from .utils import hamming_distance, node_distance_to

K = 8
logger = logging.getLogger(__name__)


class BaseRoutingTable:
    def __init__(self, dht):
        self.dht = dht
        self.ready = trio.Event()

    async def run(self):
        await self.dht.started.wait()
        self.ready.set()

        while True:
            logger.debug('Looking for bad nodes...')
            to_remove = []
            for node in self.get_all_nodes():
                if node.questionable:
                    self.dht.check_node_goodness(node)
                    continue
                if node.bad:
                    logger.info(f'Removing bad node: {node.id.hex()}')
                    to_remove.append(node)

            self.remove_nodes(to_remove)
            logger.debug('Finished looking for bad nodes.')

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
            distance = hamming_distance(node.intid, node_id)
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

    def remove_nodes(self, nodes):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

    def get_all_nodes(self):
        raise NotImplementedError


class BucketRoutingTable(BaseRoutingTable):
    """A Routing Table implementation close to BEP 5 description."""
    def __init__(self, dht=None, min_id=0, max_id=2**160, *,
                 full=False, parent=None):
        super().__init__(dht)

        self.min_id = min_id
        self.max_id = max_id

        self._nodes = {}
        self._first_half = None
        self._second_half = None
        self._full = full
        self._parent = parent

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
                    logger.debug(
                        f'Added node to routing table: {node.id.hex()}')
                    return True
                else:
                    logger.debug(
                        'Node %s was not added to the routing table '
                        'because it was already there.',
                        node.id.hex())
                    return False

            if self._full or self._node_fits(self.dht.node_id):
                if len(self._nodes) < K:
                    self._nodes[node.id] = node
                    return True

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
                for node in self._nodes.values():
                    if node.ip == node_ip and node.port == node_port:
                        return node
                return None

    def remove(self, node):
        if self._is_split:
            self._first_half.remove(node)
            self._second_half.remove(node)
        else:
            try:
                del self._nodes[node]
            except KeyError:
                pass

    def remove_nodes(self, nodes):
        if len(nodes) == 0:
            return

        size = self.size()
        if size > 1000 and len(nodes) / size > 0.1:
            # re-constructing the routing table might actually be a
            # lot faster than going through all buckets and removing
            # nodes
            all_nodes = list(self.get_all_nodes())
            for node in nodes:
                all_nodes.remove(node)
            self.clear()
            for node in all_nodes:
                self.add_node(node)
            return

        if self._is_split:
            self._first_half.remove_nodes(nodes)
            self._second_half.remove_nodes(nodes)
        else:
            for node in nodes:
                try:
                    del self._nodes[node.id]
                except KeyError:
                    pass

    def _get_leaf_buckets(self):
        if self._is_split:
            yield from self._first_half._get_leaf_buckets()
            yield from self._second_half._get_leaf_buckets()
        else:
            yield self

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

    def get_close_nodes(self, node_id, compact=False):
        if self._is_split:
            if self._first_half._node_fits(node_id):
                nodes = self._first_half.get_close_nodes(node_id)
            else:
                nodes = self._second_half.get_close_nodes(node_id)
        else:
            nodes = list(self._nodes.values())

        if len(nodes) < K:
            prev_bucket = self._get_prev_bucket()
            if prev_bucket:
                nodes += prev_bucket.get_all_nodes()

            next_bucket = self._get_next_bucket()
            if next_bucket:
                nodes += next_bucket.get_all_nodes()

        if len(nodes) > K:
            int_nid = int.from_bytes(node_id, byteorder='big')
            nodes.sort(key=node_distance_to(int_nid))
            return nodes[:K]

        if compact:
            nodes = b''.join(
                node.id +
                IPv4Address(node.ip).packed +
                node.port.to_bytes(length=2,
                                   byteorder='big',
                                   signed=False)
                for node in nodes
            )

        return nodes

    def _get_prev_bucket(self):
        parent = self._parent
        child = self
        while True:
            if not parent:
                return None
            if child == parent._second_half:
                return parent._first_half._get_last_leaf_bucket()
            child, parent = parent, parent._parent

    def _get_next_bucket(self):
        parent = self._parent
        child = self
        while True:
            if not parent:
                return None
            if child == parent._first_half:
                return parent._second_half._get_first_leaf_bucket()
            child, parent = parent, parent._parent

    def _get_first_leaf_bucket(self):
        if not self._is_split:
            return self

        return self._first_half._get_first_leaf_bucket()

    def _get_last_leaf_bucket(self):
        if not self._is_split:
            return self

        return self._second_half._get_last_leaf_bucket()

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
        self._first_half = type(self)(
            self.dht, self.min_id, middle, parent=self)
        self._second_half = type(self)(
            self.dht, middle, self.max_id, parent=self)
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


class FullBucketRoutingTable(BucketRoutingTable):
    def __init__(self, dht=None, min_id=0, max_id=2**160, parent=None):
        super().__init__(dht, min_id, max_id, full=True, parent=parent)


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

    def remove_nodes(self, nodes):
        for node in nodes:
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
