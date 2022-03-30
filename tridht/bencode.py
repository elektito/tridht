class BDecodingError(Exception):
    def __init__(self, msg=None):
        if not msg:
            msg = 'Invalid bencoded message.'
        self._msg = msg

    def __str__(self):
        return self._msg


def bdecode_string(data, allow_partial=True):
    try:
        colon_pos = data.index(b':')
    except ValueError:
        raise BDecodingError('No colon found.')

    length = data[:colon_pos]
    try:
        length = length.decode('ascii')
        length = int(length)
    except UnicodeDecodeError:
        raise BDecodingError('Invalid string length prefix.')
    except ValueError:
        raise BDecodingError('Invalid string length prefix.')

    value = data[colon_pos+1:]
    if len(value) < length:
        raise BDecodingError('String is truncated.')

    value = value[:length]
    bytes_consumed = colon_pos + 1 + length
    if not allow_partial and bytes_consumed < len(data):
        raise BDecodingError('Extra data at the end.')

    return value, bytes_consumed


def bdecode_int(data, allow_partial=True):
    idx = 1 # bypass the first 'i' character
    value = bytearray()
    while idx < len(data):
        if data[idx] == ord(b'e'):
            break
        idx += 1
    else:
        raise BDecodingError('Integer is truncated.')

    if not allow_partial and idx < len(data) - 1:
        raise BDecodingError('Extra data at the end.')

    value = data[1:idx]
    try:
        value = value.decode('ascii')
        value = int(value)
    except UnicodeDecodeError:
        raise BDecodingError('Invalid integer.')
    except ValueError:
        raise BDecodingError('Invalid integer.')

    bytes_consumed = idx + 1
    return value, bytes_consumed


def bdecode_list(data, allow_partial=True):
    idx = 1 # bypass the initial 'l'
    value = []
    while idx < len(data):
        if data[idx] == ord(b'e'):
            break
        element, econsumed = bdecode(data[idx:], allow_partial=True)
        value.append(element)
        idx += econsumed
    else:
        raise BDecodingError('List is truncated.')

    if not allow_partial and idx < len(data) - 1:
        raise BDecodingError('Extra data at the end.')

    return value, idx + 1


def bdecode_dict(data, allow_partial=True):
    idx = 1 # bypass the initial 'd'
    ret = {}
    while idx < len(data):
        if data[idx] == ord(b'e'):
            break
        key, consumed = bdecode(data[idx:], allow_partial=True)
        if not isinstance(key, bytes):
            raise BDecodingError('Dictionary keys must be strings.')
        idx += consumed
        value, consumed = bdecode(data[idx:], allow_partial=True)
        ret[key] = value
        idx += consumed
    else:
        raise BDecodingError('Dictionary is truncated.')

    if not allow_partial and idx < len(data) - 1:
        raise BDecodingError('Extra data at the end.')

    return ret, idx + 1


def bdecode(data: bytes, allow_partial=False):
    if not isinstance(data, bytes):
        raise TypeError('Data to bdecode should be a bytes value.')

    if data == b'':
        raise ValueError('Cannot decode an empty value.')

    if data[0] in b'0123456789':
        return bdecode_string(data, allow_partial=allow_partial)
    else:
        func = {
            ord(b'i'): bdecode_int,
            ord(b'l'): bdecode_list,
            ord(b'd'): bdecode_dict,
        }.get(data[0])
        if not func:
            raise BDecodingError('Unknown type specifier.')
        value, nconsumed = func(data, allow_partial=allow_partial)
        return value, nconsumed


def bencode_string(value):
    length = str(len(value)).encode('ascii')
    return length + b':' + value


def bencode_int(value):
    return b'i' + str(value).encode('ascii') + b'e'


def bencode_list(value):
    return b'l' + b''.join(bencode(i) for i in value) + b'e'


def bencode_dict(value):
    if not all(isinstance(k, bytes) for k in value):
        raise TypeError('Dictionary keys should all be bytes values.')
    encoded_dict = b''.join(bencode(k) + bencode(v)
                            for k, v in value.items())
    return b'd' + encoded_dict + b'e'


def bencode(value):
    funcs = {
        bytes: bencode_string,
        int: bencode_int,
        list: bencode_list,
        dict: bencode_dict,
    }

    for t, func in funcs.items():
        if isinstance(value, t):
            return func(value)
    else:
        types_str = ', '.join(t.__name__ for t in funcs)
        raise TypeError(
            f'Data to bencode should be one of: {types_str} '
            f'(got {type(value).__name__})')
