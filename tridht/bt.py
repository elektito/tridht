import logging
import hashlib
import os
import trio
from enum import Enum
from ipaddress import IPv4Address
from .bencode import bencode, bdecode, BDecodingError

logger = logging.getLogger(__name__)


def get_random_peer_id():
    peer_id = os.urandom(20)
    return peer_id


class BtStatus(Enum):
    UNKNOWN = 0
    WORKING = 1
    ERROR = 2
    SUCCESS = 3
    TIMEOUT = 4


class BtErr(Enum):
    CONNECTION_FAILED = 1
    BT_HANDSHAKE_INCOMPLETE = 2
    INVALID_BT_HANDSHAKE = 3
    WRONG_INFOHASH = 4
    PEER_DOESNT_SUPPORT_EXT_PROTOCOL = 5
    PEER_DOESNT_SUPPORT_FAST_EXT = 6
    CONN_RESET_BY_PEER = 7
    EOF_WHILE_READING = 8
    ERROR_PARSING_UT_METADATA = 9
    PEER_ASKING_FOR_METADATA = 10
    PEER_HAS_NO_METADATA = 11
    INVALID_EXT_MSG = 12
    PEER_DOESNT_SUPPORT_UT_METADATA = 13
    NO_METADATA_SIZE = 14
    INVALID_METADATA_HASH = 15

    def __str__(self):
        return {
            BtErr.CONNECTION_FAILED: 'Connection failed',
            BtErr.BT_HANDSHAKE_INCOMPLETE: 'Bittorrent handshake incomplete',
            BtErr.INVALID_BT_HANDSHAKE: 'Invalid bittorrent handshake received',
            BtErr.WRONG_INFOHASH: 'Peer sent different infohash',
            BtErr.PEER_DOESNT_SUPPORT_EXT_PROTOCOL: 'Peer does not support extension protocol',
            BtErr.PEER_DOESNT_SUPPORT_FAST_EXT: 'Peer does not support fast extension',
            BtErr.CONN_RESET_BY_PEER: 'Connection reset while reading from peer',
            BtErr.EOF_WHILE_READING: 'Reached EOF while reading from stream',
            BtErr.ERROR_PARSING_UT_METADATA: 'Error parsing ut_metadata message',
            BtErr.PEER_ASKING_FOR_METADATA: 'Peer is asking for metadata',
            BtErr.PEER_HAS_NO_METADATA: 'Peer does not have metadata',
            BtErr.INVALID_EXT_MSG: 'Invalid extension-protocol message',
            BtErr.PEER_DOESNT_SUPPORT_UT_METADATA: 'Peer does not support metadata protocol',
            BtErr.NO_METADATA_SIZE: 'metadata_size not available',
            BtErr.INVALID_METADATA_HASH: 'Received metadata hash does not check out',
        }[self]

class _InternalBtError(Exception):
    def __init__(self, code: BtErr, desc=None):
        self.code = code
        if desc is None:
            self.desc = str(self.code)
        else:
            self.desc = desc

        super().__init__(self.desc)


class BittorrentLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger):
        self.task_logs = []
        super().__init__(logger, {})

    def process(self, msg, kwargs):
        self.task_logs.append(msg)
        return msg, kwargs

    def save(self, filename):
        with open(filename, 'w') as f:
            for log in self.task_logs:
                f.write(log + '\n')


class Bittorrent:
    def __init__(self, peer_ip, peer_port, infohash, *,
                 peer_id=None, timeout=30):
        self.peer_ip = peer_ip
        self.peer_port = peer_port
        self.infohash = infohash
        self.timeout = timeout

        if peer_id:
            self.id = self_peer_id
        else:
            self.id = get_random_peer_id()

        self.status = BtStatus.UNKNOWN
        self.error_code = None
        self.error_desc = None

        self.metadata = None
        self.metadata_available = trio.Event()
        self.metadata_size = None
        self._metadata_piece_count = None
        self._metadata_pieces = {}
        self._already_requested_metadata_pieces = 0

        self._ut_metadata_id = 10
        self._peer_ut_metadata_id = None

        self._received_ext_handshake = False
        self._peer_supports_fast = False

        self._stream = None
        self._log = logging.getLogger(__name__)

    async def run(self):
        self._log = BittorrentLoggerAdapter(self._log)
        self.status = BtStatus.WORKING
        with trio.move_on_after(self.timeout):
            try:
                await self._run()
            except _InternalBtError as e:
                self._log.warning(
                    f'Error in bittorrent peer communication: {e}')
                self.status = BtStatus.ERROR
                self.error_code = e.code
                self.error_desc = e.desc
            else:
                self.status = BtStatus.SUCCESS
            finally:
                if self.status == BtStatus.ERROR:
                    filename = (
                        f'{self.infohash.hex()}-{self.peer_ip}-'
                        f'{self.peer_port}.log'
                    )
                    self._log.save(filename)
            return
        self.status = BtStatus.TIMEOUT

    async def _run(self):
        try:
            self._stream = await trio.open_tcp_stream(
                self.peer_ip, self.peer_port)
        except OSError as e:
            raise _InternalBtError(
                BtErr.CONNECTION_FAILED,
                f'Error opening connection to '
                f'{self.peer_ip}:{self.peer_port}: {e}')

        async with self._stream:
            await self._send_bt_handshake()
            await self._recv_bt_handshake()

            await self._send_ext_handshake()
            if self._peer_supports_fast:
                await self._send_have_none()
            await self._send_choke()
            await self._send_not_interesetd()

            while self._metadata_piece_count is None or \
                  len(self._metadata_pieces) < self._metadata_piece_count:
                bt_msg_type, msg = await self._recv_bt_msg()
                if bt_msg_type != 20:
                    self._log.debug(
                        f'Ignoring non-extension-protocol message: '
                        f'{bt_msg_type}')
                    continue

                ext_id, msg = self._parse_ext_msg(msg)
                if not self._received_ext_handshake:
                    if ext_id == 0:
                        self._log.info(
                            'Received extension protocol handshake.')
                        self._parse_ext_handshake(msg)
                        await self._request_metadata()
                    else:
                        self._log.warning(
                            'Extended message before handshake.')
                    continue

                if ext_id != self._ut_metadata_id:
                    self._log.debug('Ignoring non-ut_metadata message.')
                    continue

                await self._process_ut_metadata_msg(msg)

        metadata = b''
        for i in range(self._metadata_piece_count):
            metadata += self._metadata_pieces[i]
        ih = hashlib.sha1(metadata).digest()
        if ih != self.infohash:
            raise _InternalBtError(BtErr.INVALID_METADATA_HASH)

        self._log.info(
            f'Metadata hash checks out for infohash '
            f'{self.infohash.hex()}.')

        self.metadata = metadata
        self.metadata_available.set()

    async def _process_ut_metadata_msg(self, msg):
        try:
            be_msg, nconsumed = bdecode(msg, allow_partial=True)
        except BDecodingError:
            raise _InternalBtError(
                BtErr.ERROR_PARSING_UT_METADATA)

        msg_type = be_msg.get(b'msg_type')
        if msg_type == 0:
            raise _InternalBtError(BtErr.PEER_ASKING_FOR_METADATA)
        elif msg_type == 2:
            raise _InternalBtError(BtErr.PEER_HAS_NO_METADATA)
        elif msg_type != 1:
            self._log.warning(
                'Invalid msg_type in ut_metadata message.')
            return

        # it's a data message (msg_type == 1)

        piece = msg[nconsumed:]
        piece_idx = be_msg.get(b'piece')

        if self.metadata_size:
            if not isinstance(piece_idx, int) or \
               piece_idx < 0 or \
               piece_idx >= self._metadata_piece_count:
                self._log.warning('Invalid piece index.')
                return
        elif piece_idx != 0:
            self._log.warning(f'Invalid piece index: {piece_idx}')
            return
        elif self.metadata_size is None:
            total_size = be_msg.get(b'total_size')
            if total_size is None:
                raise _InternalBtError(
                    BtErr.NO_METADATA_SIZE,
                    'total_size not set in ut_metadata data message, '
                    'while metadata_size was also not present in '
                    'extended handshake.')
            self._set_metadata_size(total_size)
            await self._request_metadata()

        self._metadata_pieces[piece_idx] = piece
        self._log.info(
            f'Received metadata piece: {piece_idx}')

    def _set_metadata_size(self, size):
        if size is None:
            return
        self.metadata_size = size
        piece_size = 16 * 1024
        self._metadata_piece_count = self.metadata_size // piece_size
        if self.metadata_size % piece_size != 0:
            self._metadata_piece_count += 1
        self._log.info(
            f'Expecting {self._metadata_piece_count} metadata '
            f'piece(s) (metadata_size={self.metadata_size}).')

    async def _send_bt_handshake(self):
        logger.debug('Sending bittorrent handshake...')
        bt_header = b'\x13BitTorrent protocol'

        # support: extension protocol, fast protocol
        bt_reserved = 0x0000_0000_0010_0004
        bt_reserved = bt_reserved.to_bytes(length=8, byteorder='big')

        handshake = (
            bt_header + bt_reserved + self.infohash + self.id
        )
        await self._stream.send_all(handshake)

    async def _recv_bt_handshake(self):
        logger.debug('Receiving bittorrent handshake...')

        # 20 bytes: "\x13BitTorrent protocol"
        # 8 bytes:  reserved
        # 20 bytes: infohash
        # 20 bytes: peer id
        # total: 68 bytes
        peer_handshake = await self._recv_exact(68)
        if len(peer_handshake) < 68:
            raise _InternalBtError(BtErr.BT_HANDSHAKE_INCOMPLETE)

        bt_header = b'\x13BitTorrent protocol'
        if peer_handshake[:20] != bt_header:
            raise _InternalBtError(BtErr.INVALID_BT_HANDSHAKE)

        reserved = peer_handshake[20:28]
        peer_infohash = peer_handshake[28:48]
        if peer_infohash != self.infohash:
            raise _InternalBtError(BtErr.WRONG_INFOHASH)

        self.peer_id = peer_handshake[48:]

        self._log.info(f'Connected to peer: {self.peer_id.hex()}')

        reserved = int.from_bytes(reserved, byteorder='big')
        if reserved & 0x0000_0000_0010_0000 == 0:
            raise _InternalBtError(
                BtErr.PEER_DOESNT_SUPPORT_EXT_PROTOCOL)
        if reserved & 0x0000_0000_0000_0004 == 0:
            self._peer_supports_fast = False
            logger.debug('Peer does not support fast protocol.')

    async def _send_bt_msg(self, msg_type, payload):
        length = len(payload) + 1
        data = length.to_bytes(length=4, byteorder='big')
        data += bytes([msg_type])
        data += payload
        try:
            await self._stream.send_all(data)
        except trio.BrokenResourceError as e:
            raise _InternalBtError(
                BtErr.CONN_RESET_BY_PEER,
                'Connection reset by peer while writing.')

    async def _send_choke(self):
        await self._send_bt_msg(0x00, b'')

    async def _send_not_interesetd(self):
        await self._send_bt_msg(0x03, b'')

    async def _send_have_none(self):
        await self._send_bt_msg(0x0f, b'')

    async def _send_bt_msg_ext(self, ext_id, payload):
        data = bytes([ext_id]) + payload
        await self._send_bt_msg(20, data)

    async def _send_ext_handshake(self):
        ut_metadata_id = 30
        extension_handshake = {
            b'm': {
                b'ut_metadata': self._ut_metadata_id,
            },
            b'v': 'Tridht v0.0.1'.encode('utf-8'),
            b'yourip': IPv4Address(self.peer_ip).packed,
        }
        extension_handshake = bencode(extension_handshake)
        await self._send_bt_msg_ext(0, extension_handshake)

    async def _recv_bt_msg(self):
        length = await self._recv_exact(4)
        length = int.from_bytes(length, byteorder='big')
        if length == 0:
            return b'', None

        msg_type = await self._recv_exact(1)
        msg_type = msg_type[0]

        payload_size = length - 1
        payload = await self._recv_exact(payload_size)

        return msg_type, payload

    async def _recv_exact(self, count):
        remaining = count
        data = bytearray()
        while len(data) < count:
            try:
                chunk = await self._stream.receive_some(remaining)
            except trio.BrokenResourceError as e:
                raise _InternalBtError(BtErr.CONN_RESET_BY_PEER)

            if chunk == b'':
                break

            data += chunk
            remaining -= len(chunk)

        if len(data) < count:
            raise _InternalBtError(BtErr.EOF_WHILE_READING)

        return bytes(data)

    def _parse_ext_handshake(self, handshake):
        try:
            handshake, _ = bdecode(handshake)
        except BDecodingError as e:
            raise _InternalBtError(
                BtErr.INVALID_EXT_MSG,
                f'Error parsing extension protocol handshake: {e})')

        m = handshake.get(b'm', {})
        if b'ut_metadata' not in m:
            if b'LT_metadata' in m:
                self._log.warning(
                    'Note that peer does support LT_metadata, but not '
                    'ut_metadata.')
            raise _InternalBtError(
                BtErr.PEER_DOESNT_SUPPORT_UT_METADATA)

        if b'v' in handshake:
            v = handshake[b'v'].decode("utf-8")
            self._log.debug(f'Peer version: {v}')

        self._peer_ut_metadata_id = m.get(b'ut_metadata')
        metadata_size = handshake.get(b'metadata_size')
        if metadata_size is None:
            self._log.debug(
                f'metadata_size not available in extended handshake: '
                f'{handshake}')
        else:
            self._set_metadata_size(metadata_size)

        self._received_ext_handshake = True

    async def _request_metadata(self):
        self._log.debug('Requesting metadata pieces.')
        if self._metadata_piece_count:
            count = self._metadata_piece_count
        else:
            # we didn't get metadata_size in extended handshake;
            # request just one block for now
            count = 1
        for i in range(self._already_requested_metadata_pieces, count):
            msg = bencode({
                b'msg_type': 0, # request
                b'piece': i,
            })
            logger.debug(f'Requesting metadata piece {i}.')
            await self._send_bt_msg_ext(self._peer_ut_metadata_id, msg)
        self._already_requested_metadata_pieces += count

    def _parse_ext_msg(self, msg):
        if len(msg) == 0:
            raise _InternalBtError(BtErr.INVALID_EXT_MSG)

        ext_id = msg[0]
        msg = msg[1:]
        return ext_id, msg


async def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Download bittorrent metadata from a peer.')
    parser.add_argument('peer_ip', help='Peer IP address.')
    parser.add_argument('peer_port', type=int, help='Peer port number.')
    parser.add_argument(
        'infohash', help='Infohash of the torrent to download.')

    args = parser.parse_args()

    config_logging(logging.DEBUG)

    bt = Bittorrent(args.peer_ip, args.peer_port,
                    bytes.fromhex(args.infohash))
    await bt.run()


if __name__ == '__main__':
    trio.run(main)
