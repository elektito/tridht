import logging
import hashlib
import random
import time
import os
import trio
from datetime import datetime
from functools import partial
from ipaddress import IPv4Address
from copy import copy
from trio import socket
from crc32c import crc32c
from .bencode import bencode, bdecode, BDecodingError
from .routing_table import BucketRoutingTable
from .peer_table import PeerTable
from .node import Node
from .utils import launch_limited
from .semaphore import Semaphore

logger = logging.getLogger(__name__)

DefaultRoutingTable = BucketRoutingTable
DefaultPeerTable = PeerTable

MAX_CONCURRENT_GOODNESS_CHECKS = 200
MAX_CONCURRENT_PING_AND_ADD_NODES = 200

def get_random_node_id():
    node_id = random.randint(0, 2**160)
    return node_id.to_bytes(20, byteorder='big', signed=False)


class DhtErrorMessage:
    def __init__(self, error_code, error_desc, original_msg):
        self.error_code = error_code
        self.error_desc = error_desc
        self.original_msg = original_msg


class DhtResponseMessage:
    def __init__(self, r, original_msg):
        self.r = r
        self.original_msg = original_msg


class Dht:
    def __init__(self, port, db, *,
                 seed_host,
                 seed_port,
                 routing_table,
                 peer_table,
                 response_timeout=20,
                 retries=2,
                 sample_infohash_interval=3600,
                 infohash_sample_size=20,
                 no_index=False,
                 no_expand=False,
                 readonly=False):
        self.db = db
        self.port = port
        self.no_index = no_index
        self.no_expand = no_expand
        self.readonly = readonly
        self.node_id = get_random_node_id()

        self.started = trio.Event()
        self.ready = trio.Event()
        self.stopped = trio.Event()
        self._quit = trio.Event()

        self.fetch_peers_in_flight = 0
        self.find_nodes_waiting = 0
        self.find_nodes_running = 0
        self.get_peers_in_flight = 0
        self.get_peers_no_response = 0
        self.get_peers_errors = 0
        self.get_peers_with_nodes = 0
        self.get_peers_with_values = 0

        self.response_timeout = response_timeout
        self.retries = retries

        self._seed_host = seed_host
        self._seed_port = seed_port
        self._nursery = None
        self._sock = None
        self._response_channels = {}
        self._next_tid = 0
        self._self_ip_votes = {}
        self._ip = None
        self._goodness_sem = Semaphore(
            MAX_CONCURRENT_GOODNESS_CHECKS,
            name='_goodness_sem')
        self._ping_and_add_node_sem = Semaphore(
            MAX_CONCURRENT_PING_AND_ADD_NODES,
            name='_ping_and_add_node_sem')

        self._prev_token_secret = None
        self._cur_token_secret = None
        self._routing_table = routing_table
        self._peer_table = peer_table
        self._infohash_sample_size = infohash_sample_size
        self._sample_infohash_interval = sample_infohash_interval
        self._update_infohash_sample()

    @property
    def stats(self):
        return {
            'get_peers': self.get_peers_in_flight,
            'gp_no_resp': self.get_peers_no_response,
            'gp_error': self.get_peers_errors,
            'gp_w_values': self.get_peers_with_values,
            'gp_w_nodes': self.get_peers_with_nodes,
            'waiting_chans': len(self._response_channels),
            'fetch_peers': self.fetch_peers_in_flight,
            'fn_wait': self.find_nodes_waiting,
            'fn_run': self.find_nodes_running,
        }

    async def fetch_peers(self, infohash, return_channel):
        try:
            self.fetch_peers_in_flight += 1
            return await self._fetch_peers(infohash, return_channel)
        finally:
            self.fetch_peers_in_flight -= 1

    async def _fetch_peers(self, infohash, return_channel):
        already_tried_for_nodes = set()
        already_returned_peers = set()
        no_response_nodes = set()
        sem = Semaphore(100, name='_fetch_peers/sem')
        async def launch_find_nodes_or_peers(node, nursery):
            async def find_and_release(node, nursery):
                try:
                    await _find_nodes_or_peers(node, nursery)
                finally:
                    sem.release()
                    self.find_nodes_running -= 1

            with trio.move_on_after(10) as cancel_scope:
                try:
                    self.find_nodes_waiting += 1
                    await sem.acquire()
                finally:
                    self.find_nodes_waiting -= 1
                self.find_nodes_running += 1
                nursery.start_soon(find_and_release, node, nursery)

        async def _find_nodes_or_peers(node, nursery):
            if not self._validate_bep42_node_id(node.id, node.ip):
                return

            msg = {
                b't': self._get_next_tid(),
                b'y': b'q',
                b'q': b'get_peers',
                b'a': {
                    b'id': self.node_id,
                    b'info_hash': infohash,
                },
            }

            self.get_peers_in_flight += 1
            try:
                resp = await self._send_and_get_response(msg, node)
            finally:
                self.get_peers_in_flight -= 1

            if resp is None:
                self.get_peers_no_response += 1
                no_response_nodes.add(node)
                return
            if not isinstance(resp, DhtResponseMessage):
                self.get_peers_errors += 1
                return
            if b'values' in resp.r:
                for value in resp.r[b'values']:
                    if len(value) != 6:
                        logger.warning(
                            'peer info length not divisible by 6.')
                        return
                    self.get_peers_with_values += 1
                    ip = str(IPv4Address(value[:4]))
                    port = int.from_bytes(value[4:], byteorder='big')
                    if (ip, port) not in already_returned_peers:
                        await return_channel.send((ip, port))
                        already_returned_peers.add((ip, port))
            elif b'nodes' in resp.r:
                nodes = resp.r[b'nodes']
                if len(nodes) % 26 != 0:
                    logger.warning(
                        'nodes field length not divisible by 26.')
                    return
                self.get_peers_with_nodes += 1
                nodes = self._parse_find_node_response(resp)
                for node in nodes:
                    if node not in already_tried_for_nodes and \
                       node not in no_response_nodes:
                        await launch_find_nodes_or_peers(node, nursery)
                        already_tried_for_nodes.add(node)
            else:
                logger.debug(
                    f'Neither values nor nodes in get_peers response. {resp.r}')

        nodes = self._routing_table.get_close_nodes(infohash)
        async with trio.open_nursery() as nursery:
            for node in nodes:
                await launch_find_nodes_or_peers(node, nursery)

        return_channel.close()
        logger.debug(f'Finished fetch_peers for: {infohash.hex()}')

    async def run(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._main_loop)

            await self._quit.wait()

            nursery.cancel_scope.cancel()

        self.stopped.set()

    def stop(self):
        self._quit.set()

    async def _main_loop(self):
        logger.info(f'Starting DHT on port {self.port}...')

        logger.info('Binding server socket...')
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        await self._sock.bind(('0.0.0.0', self.port))

        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            self.started.set()

            await self._routing_table.ready.wait()
            await self._peer_table.ready.wait()

            nursery.start_soon(self._seed_routing_table)

            if not self.readonly:
                nursery.start_soon(self._periodically_update_infohash_sample)
                nursery.start_soon(self._keep_token_secrets_updated)

            if not self.no_expand:
                nursery.start_soon(self._periodically_expand_routing_table)

            if not self.no_index:
                nursery.start_soon(self._index_infohashes)

            while True:
                try:
                    data, addr = await self._sock.recvfrom(65535)
                except (trio.ClosedResourceError, OSError):
                    # another task has closed the socket, so an error
                    # has happened (probably while initializing)
                    nursery.cancel_scope.cancel()
                    break
                logger.debug(f'Received {len(data)} bytes from {addr[0]}:{addr[1]}.')
                nursery.start_soon(self._process_msg, data, addr)

        logger.info(f'DHT on port {self.port} finished.')

    def get_state(self):
        return {
            'node_id': self.node_id.hex(),
            'next_tid': self._next_tid,
            'prev_token_secret': self._prev_token_secret.hex(),
            'cur_token_secret': self._cur_token_secret.hex(),
        }

    def load_state(self, state):
        self.node_id = bytes.fromhex(state['node_id'])
        self._next_tid = state['next_tid']
        self._prev_token_secret = bytes.fromhex(state['prev_token_secret'])
        self._cur_token_secret = bytes.fromhex(state['cur_token_secret'])

    async def _index_infohashes(self):
        not_supporting = set()
        next_sample_time = {}

        all_nodes = list(self._routing_table.get_all_nodes())
        random.shuffle(all_nodes)

        while True:
            if not all_nodes:
                all_nodes = list(self._routing_table.get_all_nodes())
                random.shuffle(all_nodes)
                await trio.sleep(0.1)
                continue

            node = all_nodes.pop()

            now = trio.current_time()
            if node in not_supporting or \
               next_sample_time.get(node, now) < now:
                await trio.sleep(0.1)
                continue

            random_node_id = get_random_node_id()
            resp = await self._perform_sample_infohashes(node, random_node_id)
            if isinstance(resp, DhtErrorMessage):
                not_supporting.add(node)
                await trio.sleep(0.1)
                continue
            if not resp or not isinstance(resp, DhtResponseMessage):
                await trio.sleep(0.1)
                continue

            num = resp.r.get(b'num')
            interval = resp.r.get(b'interval')
            nodes = resp.r.get(b'nodes')
            samples = resp.r.get(b'samples')

            if samples is None:
                not_supporting.add(node)
                await trio.sleep(0.1)
                continue

            if not isinstance(samples, bytes):
                logger.info(
                    'sample_infohashes response contains invalid value '
                    'in response.')
                logger.debug('Invalid samples value: %s', samples)
                return

            if not interval or not isinstance(interval, int):
                interval = 60

            for i in range(0, len(samples), 20):
                ih = samples[i:i+20]
                await self.db.add_infohash_for_sample(ih)
            if samples:
                logger.info(
                    f'Sampled {len(samples)//20} infohashes '
                    f'(out of {num}).')

            next_sample_time[node] = trio.current_time() + interval

    async def _keep_token_secrets_updated(self):
        self._prev_token_secret = os.urandom(16)
        self._cur_token_secret = self._prev_token_secret
        while True:
            await trio.sleep(5 * 60)
            self._prev_token_secret = self._cur_token_secret
            self._cur_token_secret = os.urandom(16)
            logger.debug('Updated token secret.')

    async def _seed_routing_table(self):
        if self._routing_table.dht != self:
            # routing table might be shared, but only one of the nodes
            # is responsible for seeding and updating it.
            return

        need_seeding = True
        if self._routing_table.size() >= 5:
            # possibly we don't need seeding; check the goodness of
            # nodes in the routing table.
            logger.debug('Checking goodness of existing nodes...')
            good_ones = 0
            all_nodes = list(self._routing_table.get_all_nodes())
            async with trio.open_nursery() as nursery:
                for node in all_nodes:
                    await self.check_node_goodness(node)

            for node in all_nodes:
                if node.good:
                    good_ones += 1
            if good_ones >= 8:
                logger.info('Routing table already seeded.')
                need_seeding = False
            else:
                logger.info('Not enough good nodes. Re-seeding...')

        if not need_seeding:
            return

        logger.info('Seeding routing table...')

        logger.info('Resolving seed host name...')
        try:
            addrs = await socket.getaddrinfo(self._seed_host,
                                             port=self._seed_port,
                                             family=socket.AF_INET,
                                             type=socket.SOCK_DGRAM)
        except socket.gaierror as e:
            logger.error(f'Could not resolve seed host name: {e}')
            return
        if not addrs:
            logger.fatal(f'No IP addresses found for the seed host name.')
            return
        _, _, _, _, seedaddr = random.choice(addrs)

        # we don't know the node id of the seed node, but it doesn't
        # really matter here, since
        seed_ip, seed_port = seedaddr
        seed_node = Node(get_random_node_id(), seed_ip, seed_port)

        # now perform a find_node query on a random node id to seed
        # the routing table
        random_node_id = get_random_node_id()
        logger.info(f'Requesting random nodes from seed...')
        resp = await self._perform_find_node(seed_node, random_node_id)
        if resp is None:
            logger.error('Seed node did not respond to query.')
            self._sock.close()
            return

        if isinstance(resp, DhtErrorMessage):
            logger.error(
                f'Seed node returned an error to query: {resp}')
            self._sock.close()
            return

        if not isinstance(resp, DhtResponseMessage):
            logger.error(
                'Seed node returned invalid response to query.')
            self._sock.close()
            return

        nodes = self._parse_find_node_response(resp)
        if nodes is None:
            logger.error('Seed node did not return a valid response.')
            self._sock.close()
            return
        if len(nodes) == 0:
            logger.error('Seed node did not return any nodes.')
            self._sock.close()
            return

        async with trio.open_nursery() as nursery:
            for node in nodes:
                nursery.start_soon(partial(
                    self._ping_and_add_node, node, seeding=True))

        # seed node is now probably added to the routing table. remove
        # it.
        node = self._routing_table.find_node(
            node_ip=seed_ip, node_port=seed_port)
        if node:
            self._routing_table.remove(node)

        logger.info(
            f'Added {len(nodes)} node(s) returned by the seed '
            'node.')
        self.ready.set()

    async def _periodically_update_infohash_sample(self):
        # this updates the sample we return every time we receive a
        # BEP-51 style sample_infohashes command.
        while True:
            self._update_infohash_sample()
            logger.debug('Infohash sample updated.')
            await trio.sleep(self._sample_infohash_interval)

    def _update_infohash_sample(self):
        sample_size = self._infohash_sample_size
        if sample_size > self._peer_table.size():
            sample_size = self._peer_table.size()

        self._cur_infohash_sample = self._peer_table.get_sample(
            self._infohash_sample_size)

    async def _periodically_expand_routing_table(self):
        while True:
            await trio.sleep(10)

            nodes = list(self._routing_table.get_all_nodes())
            if not nodes:
                continue

            existing_node = random.choice(nodes)

            random_node_id = get_random_node_id()
            resp = await self._perform_find_node(existing_node, random_node_id)
            if resp is None:
                continue

            if isinstance(resp, DhtErrorMessage):
                continue

            if not isinstance(resp, DhtResponseMessage):
                continue

            nodes = self._parse_find_node_response(resp)
            if nodes is None:
                continue

            async with trio.open_nursery() as nursery:
                for node in nodes:
                    await launch_limited(
                        self._ping_and_add_node, node,
                        nursery=nursery,
                        semaphore=self._ping_and_add_node_sem)

    async def _process_msg(self, data, addr):
        if not data:
            logger.info('Got empty packet.')
            return

        try:
            msg, _ = bdecode(data)
        except BDecodingError as e:
            logger.info(f'Error decoding received message: {e}')
            return

        if not isinstance(msg, dict):
            logger.info(f'Invalid message received: not a dictionary.')
            return

        tid = msg.get(b't')
        if tid is None:
            logger.debug('No transaction id in received message.')
            return

        msg_type = msg.get(b'y')
        if msg_type is None:
            logger.info('Input message type is not set.')
            return

        client_version = msg.get(b'v')
        if client_version is None:
            logger.debug('Client version not set in received message.')

        if msg_type == b'q':
            if self.readonly:
                logger.debug(
                    'Ignoring query because in read-only mode.')
            else:
                await self._process_query(msg, tid, addr)
        elif msg_type == b'r':
            await self._process_response(msg, tid, addr)
        elif msg_type == b'e':
            await self._process_error(msg, tid, addr)
        else:
            logger.info(
                b'Invalid message type in input message: {msg_type}')
            return

    async def _process_error(self, msg, tid, addr):
        logger.debug(f'Got error packet: {msg}')

        e = msg.get(b'e')
        if e is None:
            logger.info(
                'Invalid error packet received: "e" key not present.')
            return

        if not isinstance(e, list):
            logger.info(
                'Invalid error packet received: "e" is not a list.')
            return

        if len(e) == 0:
            logger.info(
                'Invalid error packet received: "e" is an empty list.')
            return

        error_code = e[0]

        if len(e) == 1:
            logger.info(
                'Received error packet does not have a description.')
            error_desc = ''
        else:
            error_desc = e[1].decode('ascii')

        if len(e) > 2:
            logger.info(
                'Received error packet has more than two values in the '
                '"e" field.')

        logger.info(
            f'Error packet received: code={error_code} '
            f'desc={error_desc}')

        error_msg = DhtErrorMessage(error_code, error_desc, msg)

        resp_channel = self._response_channels.get(tid)
        if resp_channel:
            try:
                await resp_channel.send(error_msg)
            except trio.ClosedResourceError:
                logger.debug('Response channel already closed.')
        else:
            logger.debug(
                'Got an error packet not corresponding to any query.')

    async def _process_response(self, msg, tid, addr):
        logger.debug(f'Got response packet: {msg}')

        r = msg.get(b'r')
        if r is None:
            logger.info(
                'Invalid response packet received: "r" key not '
                'present.')
            return

        if not isinstance(r, dict):
            logger.info(
                'Invalid response packet: "r" is not a dictionary.')
            return

        node_id = r.get(b'id')
        if not isinstance(node_id, bytes):
            logger.info(
                'Invalid response packet: did not find a valid node id')
            return

        resp_msg = DhtResponseMessage(r, msg)

        resp_channel = self._response_channels.get(tid)
        if resp_channel:
            self._process_self_ip(msg, addr)
            try:
                await resp_channel.send(resp_msg)
            except trio.ClosedResourceError:
                logger.debug('Response channel already closed.')

            self._routing_table.add_or_update_node(
                node_id, addr[0], addr[1],
                interaction='response_to_query')
        else:
            logger.debug(
                'Got a response packet not corresponding to any '
                'query.')

            self._routing_table.add_or_update_node(
                node_id, addr[0], addr[1],
                interaction='response_unsolicited')

    async def _process_query(self, msg, tid, addr):
        logger.debug(f'Got query packet: {msg}')

        found_errors = False

        method = msg.get(b'q')
        if method is None:
            logger.info('Received query does not have a method name.')
            resp = {
                b't': tid,
                b'y': b'e',
                b'e': [203, b'No method name'],
            }
            found_errors = True

        args = msg.get(b'a')
        if args is None:
            logger.info('Received query does not have any arguments.')
            resp = {
                b't': tid,
                b'y': b'e',
                b'e': [203, b'No query arguments'],
            }
            found_errors = True

        node_id = args.get(b'id')
        if node_id is None:
            logger.info('Received query does not have a node id.')
            resp = {
                b't': tid,
                b'y': b'e',
                b'e': [203, b'No node id'],
            }
            found_errors = True

        if not self._validate_bep42_node_id(node_id, addr[0]):
            # we may decide to not allow non-compliant nodes to do
            # announce_peer or store queries later, and possibly also
            # not add them to the routing table.
            logger.debug('Node ID is not BEP42-compliant (not enforced).')

        if not found_errors:
            if method == b'ping':
                logger.info(f'Got a ping query from {node_id.hex()}')
                resp = {
                    b't': tid,
                    b'y': b'r',
                    b'r': {b'id': self.node_id},
                }
            elif method == b'find_node':
                resp = await self._process_query_find_node(
                    msg, tid, addr, args, node_id)
            elif method == b'get_peers':
                resp = await self._process_query_get_peers(
                    msg, tid, addr, args, node_id)
            elif method == b'announce_peer':
                resp = await self._process_query_announce_peer(
                    msg, tid, addr, args, node_id)
            elif method == b'sample_infohashes':
                resp = await self._process_query_sample_infohashes(
                    msg, tid, addr, args, node_id)
            else:
                try:
                    logger.info(
                        f'Unknown query received: '
                        f'{method.decode("ascii")}')
                except UnicodeDecodeError:
                    logger.info(f'Unknown query received: {method}')
                resp = {
                    b't': tid,
                    b'y': b'e',
                    b'e': [204, b'Method unknown'],
                }

        if resp is not None:
            await self._send_msg(resp, addr)

            self._routing_table.add_or_update_node(
                node_id, addr[0], addr[1], interaction='query')

    async def _process_query_find_node(self, msg, tid, addr, args, node_id):
        target = args.get(b'target')
        if target is None:
            logger.info('find_node query has no target.')
            return {
                b't': tid,
                b'y': b'e',
                b'e': [203, b'find_node query has no target'],
            }

        logger.info(
            f'Got a find_node query from {node_id.hex()} for node '
            f'{target.hex()}')

        nodes = self._routing_table.get_close_nodes(
            target, compact=True)
        return {
            b't': tid,
            b'y': b'r',
            b'r': {
                b'id': self.node_id,
                b'nodes': nodes,
            }
        }

    async def _process_query_get_peers(self, msg, tid, addr, args, node_id):
        info_hash = args.get(b'info_hash')
        if info_hash is None:
            logger.info('get_peers query has no info_hash.')
            return {
                b't': tid,
                b'y': b'e',
                b'e': [203, b'get_peers query has no info_hash'],
            }

        logger.info(
            f'Got a get_peers query from {node_id.hex()} for info_hash '
            f'{info_hash.hex()}')

        await self.db.add_infohash_for_get_peers(info_hash)

        peers = self._peer_table.get_peers(info_hash)
        if peers:
            peers_list = []
            for ip, port in peers:
                compact_peer = (
                    IPv4Address(ip).packed +
                    port.to_bytes(length=2,
                                  byteorder='big',
                                  signed=False)
                )
                peers_list.append(compact_peer)
            return {
                b't': tid,
                b'y': b'r',
                b'r': {
                    b'id': self.node_id,
                    b'token': self._get_token(addr[0]),
                    b'peers': peers_list,
                },
            }

        nodes = self._routing_table.get_close_nodes(info_hash,
                                                    compact=True)
        return {
            b't': tid,
            b'y': b'r',
            b'r': {
                b'id': self.node_id,
                b'token': self._get_token(addr[0]),
                b'nodes': nodes,
            }
        }

    async def _process_query_announce_peer(self, msg, tid, addr,
                                           args, node_id):
        info_hash = args.get(b'info_hash')
        if info_hash is None:
            logger.info('announce_peer query has no info_hash.')
            return {
                b't': tid,
                b'y': b'e',
                b'e': [203, b'announce_peer query has no info_hash'],
            }

        logger.info(
            f'Got an announce_peer query from {node_id.hex()} for '
            f'info_hash {info_hash.hex()}')

        token = args.get(b'token')
        if token is None:
            logger.info('announce_peer query has no token.')
            return {
                b't': tid,
                b'y': b'e',
                b'e': [203, b'announce_peer query has no token'],
            }

        if not self._is_token_valid(token, addr[0]):
            logger.info('Invalid token in announce_peer query.')
            return None

        port = args.get(b'port')
        implied_port = args.get(b'implied_port')
        if port is None and implied_port is None:
            return {
                b't': tid,
                b'y': b'e',
                b'e': [
                    203,
                    b'announce_peer query has no port or implied_port'
                ],
            }
        if implied_port is not None and implied_port not in [0, 1]:
            return {
                b't': tid,
                b'y': b'e',
                b'e': [
                    203,
                    b'announce_peer query has invalid implied_port '
                    b'value',
                ],
            }
        if implied_port == 1:
            port = addr[1]

        await self._peer_table.announce(
            info_hash, node_id, addr[0], port)

        return {
            b't': tid,
            b'y': b'r',
            b'r': {b'id': self.node_id},
        }

    async def _process_query_sample_infohashes(self, msg, tid, addr,
                                               args, node_id):
        target = args.get(b'target')
        if target:
            nodes = self._routing_table.get_close_nodes(
                target, compact=True)
        else:
            logger.debug('sample_infohashes query has no target.')
            nodes = b''

        logger.info(
            f'Got an sample_infohashes query from {node_id.hex()} with '
            f'target {target.hex()}')

        return {
            b't': tid,
            b'y': b'r',
            b'r': {
                b'id': self.node_id,
                b'nodes': nodes,
                b'interval': self._sample_infohash_interval,
                b'num': self._peer_table.size(),
                b'samples': self._cur_infohash_sample,
            }
        }

    def _process_self_ip(self, msg, voter_addr):
        if len(self._self_ip_votes) >= 3:
            return

        ip_port = msg.get(b'ip')
        if ip_port is None:
            return

        if not isinstance(ip_port, bytes) or len(ip_port) != 6:
            logger.debug('Invalid ip field.')
            return

        ip = IPv4Address(ip_port[:4])
        self._self_ip_votes[voter_addr] = ip

        if len(self._self_ip_votes) == 3:
            ips = list(self._self_ip_votes.values())
            if ips[0] == ips[1] == ips[2]:
                self._ip = ips[0]
                logger.info(f'Self-IP voted as: {self._ip}')

                if self._validate_bep42_node_id(self.node_id, self._ip):
                    logger.info('Node ID already BEP42-compliant.')
                    return

                self.node_id = self._generate_bep42_node_id(ip)

                # clear routing table and add nodes again based on the
                # new node id
                nodes = list(self._routing_table.get_all_nodes())
                self._routing_table.clear()
                for node in nodes:
                    self._routing_table.add_node(node)

                logger.info(
                    f'Changed node ID to the BEP-42 compliant value: '
                    f'{self.node_id.hex()}')
            else:
                logger.info(
                    f'First three self-IPs do not match. Trying again.')
                self._self_ip_votes = {}

    async def _ping_and_add_node(self, node, *, seeding=False):
        resp = await self._ping_node(node)
        if resp is None:
            if seeding:
                logger.info(
                    'Node obtained from seed did not reply to ping.')
            return
        node.last_response_time = datetime.now()
        node.ever_responded = True
        self._routing_table.add_node(node)

    async def _send_and_get_response(self, msg, node):
        assert isinstance(msg, dict)

        resp = None
        retries = self.retries
        while resp is None and retries >= 0:
            tid = self._get_next_tid()
            msg[b't'] = tid

            send_channel, recv_channel = trio.open_memory_channel(0)

            # register the send channel so that when the response for this
            # query arrives, it's sent to us.
            self._response_channels[tid] = send_channel

            await self._send_msg(msg, node)

            resp = None
            with trio.move_on_after(self.response_timeout):
                resp = await recv_channel.receive()

            send_channel.close()
            recv_channel.close()

            try:
                del self._response_channels[tid]
            except KeyError:
                # this shouldn't happen but we've seen it happen once,
                # so until we figure out how it happened, this is
                # gonna stay here.
                logger.debug('tid not found in _response_channels.')

            retries -= 1

        if resp is None:
            node.bad = True

        return resp

    async def _send_msg(self, msg, node):
        if isinstance(node, Node):
            addr = (node.ip, node.port)
        else:
            addr = node

        msg[b'v'] = b'TD\x00\x01'

        if self.readonly:
            msg[b'ro'] = 1

        if msg[b'y'] in [b'r', b'e']:
            ip, port = addr
            ip = IPv4Address(ip).packed
            port = port.to_bytes(length=2,
                                 byteorder='big',
                                 signed=False)
            msg[b'ip'] = ip + port

        msg = bencode(msg)

        if isinstance(addr[0], IPv4Address):
            addr = (str(addr[0]), addr[1])

        try:
            await self._sock.sendto(msg, addr)
        except OSError as e:
            logger.warning(
                f'Error sending packet to {addr[0]}:{addr[1]}: {e}')

    async def _ping_node(self, node):
        msg = {
            b'y': b'q',
            b'q': b'ping',
            b'a': {
                b'id': self.node_id,
            },
        }
        return await self._send_and_get_response(msg, node)

    async def _perform_find_node(self, dest_node, sought_node_id):
        msg = {
            b'y': b'q',
            b'q': b'find_node',
            b'a': {
                b'id': self.node_id,
                b'target': sought_node_id,
            },
        }
        return await self._send_and_get_response(msg, dest_node)

    async def _perform_sample_infohashes(self, dest_node,
                                         sought_node_id):
        msg = {
            b'y': b'q',
            b'q': b'sample_infohashes',
            b'a': {
                b'id': self.node_id,
                b'target': sought_node_id,
            },
        }
        return await self._send_and_get_response(msg, dest_node)

    def retry_add_node_after_refresh(self, node_to_add,
                                     nodes_to_refresh):
        self._nursery.start_soon(self._retry_add_node_after_refresh,
                                node_to_add, list(nodes_to_refresh))

    async def check_node_goodness(self, node):
        await launch_limited(self._check_node_goodness, node,
                             nursery=self._nursery,
                             semaphore=self._goodness_sem)

    def _get_next_tid(self):
        ret = self._next_tid.to_bytes(length=2,
                                      byteorder='big',
                                      signed=False)
        self._next_tid += 1
        self._next_tid &= 0xffff
        return ret

    def _is_token_valid(self, token, node_or_ip):
        if isinstance(node_or_ip, Node):
            ip = node_or_ip.ip
        else:
            ip = node_or_ip
        ip = IPv4Address(ip).packed
        tok1 = hashlib.sha1(ip + self._cur_token_secret).digest()
        tok2 = hashlib.sha1(ip + self._prev_token_secret).digest()
        return token == tok1 or token == tok2

    def _get_token(self, node_or_ip):
        if isinstance(node_or_ip, Node):
            ip = node_or_ip.ip
        else:
            ip = node_or_ip
        ip = IPv4Address(ip).packed
        return hashlib.sha1(ip + self._cur_token_secret).digest()

    def _parse_find_node_response(self, resp):
        nodes = resp.r.get(b'nodes')
        if nodes is None:
            return None
        if not isinstance(nodes, bytes):
            logger.info(
                'find_node response "nodes" field is not a string.')
            return None
        if len(nodes) % 26 != 0:
            logger.info(
                'find_node response "nodes" value size is not '
                'divisible by 26.')
            return None
        nodes = [nodes[i:i+26] for i in range(0, len(nodes), 26)]
        ret_nodes = []
        for node_info in nodes:
            node_id = node_info[:20]
            node_ip = node_info[20:24]
            node_port = node_info[24:]

            node_ip = str(IPv4Address(node_ip))
            node_port = int.from_bytes(node_port, byteorder='big')

            ret_nodes.append(Node(node_id, node_ip, node_port))

        return ret_nodes

    def _validate_bep42_node_id(self, node_id, ip):
        rand = node_id[-1]
        r = rand & 0x07

        ip = int(IPv4Address(ip))
        ip &= 0x030f3fff
        ip |= r << (5+24)
        crc = crc32c(ip.to_bytes(length=4, byteorder='big', signed=False))

        crc_first_21_bits = crc >> 11
        nid_first_21_bits = int.from_bytes(node_id[:3],
                                           byteorder='big',
                                           signed=False)
        nid_first_21_bits >>= 3
        return crc_first_21_bits == nid_first_21_bits

    def _generate_bep42_node_id(self, ip: IPv4Address):
        nid = bytearray(os.urandom(20))

        rand = random.randint(0, 255)
        r = rand & 0x07

        ip = int(IPv4Address(ip))
        ip &= 0x030f3fff
        ip |= r << (5+24)
        crc = crc32c(ip.to_bytes(length=4, byteorder='big', signed=False))

        nid[0] = (crc >> 24) & 0xff
        nid[1] = (crc >> 16) & 0xff
        nid[2] = ((crc >> 8) & 0xf8) | (random.randint(0, 255) & 0x07)

        nid[-1] = rand

        return bytes(nid)

    async def _retry_add_node_after_refresh(self, node_to_add, nodes_to_refresh):
        for node in nodes_to_refresh:
            await self._check_node_goodness(node)
        if not all(n.good for n in nodes_to_refresh):
            self._routing_table.add_node(node_to_add)

    async def _check_node_goodness(self, node):
        resp = await self._ping_node(node)
        if resp is None:
            node.bad = True
            return
        now = datetime.now()
        node.last_query_time = node.last_response_time = now
        node.ever_responded = True
