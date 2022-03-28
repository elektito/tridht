import logging
import random
import time
import os
import trio
from ipaddress import IPv4Address
from copy import copy
from trio import socket
from crc32c import crc32c
from .bencode import bencode, bdecode, BDecodingError
from .routing_table import RoutingTable
from .peer_table import PeerTable
from .node import Node

logger = logging.getLogger(__name__)

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
    def __init__(self, port, *, seed_host, seed_port,
                 response_timeout=20, retries=2):
        self.port = port
        self.node_id = get_random_node_id()

        self.response_timeout = response_timeout
        self.retries = retries

        self._seed_host = seed_host
        self._seed_port = seed_port
        self._nursery = None
        self._sock = None
        self._routing_table = RoutingTable(self)
        self._peer_table = PeerTable()
        self._response_channels = {}
        self._next_tid = 0
        self._self_ip_votes = {}
        self._ip = None

        self._prev_token_secret = None
        self._cur_token_secret = None

    async def run(self):
        logger.info(f'Starting DHT on port {self.port}...')

        logger.info('Binding server socket...')
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        await self._sock.bind(('0.0.0.0', self.port))

        async with trio.open_nursery() as nursery:
            self._nursery = nursery
            nursery.start_soon(self._keep_token_secrets_updated)
            nursery.start_soon(self._seed_routing_table)
            nursery.start_soon(self._peer_table.run)

            while True:
                data, addr = await self._sock.recvfrom(8192)
                logger.debug(f'Received {len(data)} bytes from {addr[0]}:{addr[1]}.')
                nursery.start_soon(self._process_msg, data, addr)

        logger.info(f'DHT on port {self.port} finished.')

    async def _keep_token_secrets_updated(self):
        self._prev_token_secret = os.urandom(16)
        self._cur_token_secret = self._prev_token_secret
        while True:
            await trio.sleep(5 * 60)
            self._prev_token_secret = self._cur_token_secret
            self._cur_token_secret = os.urandom(16)
            logger.info('Updated token secret.')

    async def _seed_routing_table(self):
        logger.info('Seeding routing table...')

        logger.info('Resolving seed host name...')
        try:
            addrs = await socket.getaddrinfo(self._seed_host,
                                             port=self._seed_port,
                                             family=socket.AF_INET,
                                             type=socket.SOCK_DGRAM)
        except socket.gaierror as e:
            logger.fatal(f'Could not resolve seed host name: {e}')
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
        resp = await self._perform_find_node(seed_node, random_node_id)
        if resp is None:
            raise RuntimeError('Seed node did not respond to query.')

        if isinstance(resp, DhtErrorMessage):
            raise RuntimeError(
                f'Seed node returned an error to query: {resp}')

        if not isinstance(resp, DhtResponseMessage):
            raise RuntimeError(
                'Seed node returned invalid response to query.')

        nodes = self._parse_find_node_response(resp)
        if nodes is None:
            raise RuntimeError(
                'Seed node did not return a valid response.')
        if len(nodes) == 0:
            raise RuntimeError('Seed node did not return any nodes.')

        async def ping_and_add_node(node):
            resp = await self._ping_node(node)
            if resp is None:
                logger.info(
                    'Node obtained from seed did not reply to ping.')
                return
            node.last_response_time = time.time()
            node.ever_responded = True
            self._routing_table.add_node(node)
        async with trio.open_nursery() as nursery:
            for node in nodes:
                nursery.start_soon(ping_and_add_node, node)

        # seed node is now probably added to the routing table. remove
        # it.
        node = self._routing_table.find_node(
            node_ip=seed_ip, node_port=seed_port)
        if node:
            self._routing_table.remove(node)

        logger.info(
            f'Added {len(nodes)} node(s) returned by the seed '
            'node.')

    async def _process_msg(self, data, addr):
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
            logger.info('No transaction id in received message.')
            return

        msg_type = msg.get(b'y')
        if msg_type is None:
            logger.info('Input message type is not set.')
            return

        client_version = msg.get(b'v')
        if client_version is None:
            logger.debug('Client version not set in received message.')

        if msg_type == b'q':
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

        error_code == e[0]

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
            await resp_channel.send(error_msg)
        else:
            logger.info(
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
        else:
            logger.info(
                'Got a response packet not corresponding to any '
                'query.')

        self._routing_table.add_or_update_node(
            node_id, addr[0], addr[1], interaction='response')

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
            else:
                logger.info(b'Unknown query received: {q}')
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
                b'nodes': compact_nodes,
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

        self._peer_table.announce(info_hash, addr[0], port)

        return {
            b't': tid,
            b'y': b'r',
            b'r': {b'id': self.node_id},
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

            del self._response_channels[tid]

            retries -= 1

        return resp

    async def _send_msg(self, msg, node):
        if isinstance(node, Node):
            addr = (node.ip, node.port)
        else:
            addr = node

        msg[b'v'] = b'TD\x00\x01'

        if msg[b'y'] in [b'r', b'e']:
            ip, port = addr
            ip = IPv4Address(ip).packed
            port = port.to_bytes(length=2,
                                 byteorder='big',
                                 signed=False)
            msg[b'ip'] = ip + port

        msg = bencode(msg)
        await self._sock.sendto(msg, addr)

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

    def retry_add_node_after_refresh(self, node_to_add,
                                     nodes_to_refresh):
        self._nursery.start_soon(self._retry_add_node_after_refresh,
                                node_to_add, copy(nodes_to_refresh))

    def _get_next_tid(self):
        ret = self._next_tid.to_bytes(length=2,
                                      byteorder='big',
                                      signed=False)
        self._next_tid += 1
        self._next_tid &= 0xffff
        return ret

    def _is_token_valid(self, token, node):
        tok1 = hashlib.sha1(node.ip + self._cur_token_secret).digest()
        tok2 = hashlib.sha1(node.ip + self._prev_token_secret).digest()
        return token == tok1 or token == tok2

    def _get_token(self, node_or_ip):
        if isinstance(node_or_ip, Node):
            ip = node_or_ip.ip
        else:
            ip = node_or_ip
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
        if not all(n.is_good() for n in nodes_to_refresh):
            self._routing_table.add_node(node_to_add)

    async def _check_node_goodness(self, node):
        resp = await self._ping_node(node)
        if resp is None:
            return
        now = time.time()
        node.last_query_time = node.last_response_time = now
        node.ever_responded = True
