import pytest
from tridht.bencode import bencode, bdecode, BDecodingError


def test_bdecode_invalid_type():
    with pytest.raises(TypeError):
        bdecode('4:spam')


def test_bdecode_invalid_value():
    with pytest.raises(ValueError):
        bdecode(b'')


def test_bdecode_string():
    value, nconsumed = bdecode(b'4:spam')
    assert value == b'spam'
    assert nconsumed == 6


def test_bdecode_string_empty():
    value, nconsumed = bdecode(b'0:')
    assert value == b''
    assert nconsumed == 2


def test_bdecode_string_long():
    value, nconsumed = bdecode(b'924:' + (b'x' * 924))
    assert value == (b'x' * 924)
    assert nconsumed == 4 + 924


def test_bdecode_string_extra():
    with pytest.raises(BDecodingError):
        bdecode(b'4:spamx')


def test_bdecode_string_partial():
    value, nconsumed = bdecode(b'4:spamx', allow_partial=True)
    assert value == b'spam'
    assert nconsumed == 6


def test_bdecode_string_truncated():
    with pytest.raises(BDecodingError):
        bdecode(b'4:spa')


def test_bdecode_int():
    value, nconsumed = bdecode(b'i1e')
    assert value == 1
    assert nconsumed == 3


def test_bdecode_int_zero():
    value, nconsumed = bdecode(b'i0e')
    assert value == 0
    assert nconsumed == 3


def test_bdecode_int_negative():
    value, nconsumed = bdecode(b'i-9862e')
    assert value == -9862
    assert nconsumed == 7


def test_bdecode_int_big():
    value, nconsumed = bdecode(b'i4294967295e')
    assert value == 4294967295
    assert nconsumed == 12


def test_bdecode_int_partial():
    value, nconsumed = bdecode(b'i200exyz', allow_partial=True)
    assert value == 200
    assert nconsumed == 5


def test_bdecode_int_extra():
    with pytest.raises(BDecodingError):
        bdecode(b'i200exyz')


def test_bdecode_int_truncated():
    with pytest.raises(BDecodingError):
        bdecode(b'i100')


def test_bdecode_list():
    value, nconsumed = bdecode(b'l4:spami100ee')
    assert value == [b'spam', 100]
    assert nconsumed == 13


def test_bdecode_list_empty():
    value, nconsumed = bdecode(b'le')
    assert value == []
    assert nconsumed == 2


def test_bdecode_list_truncated():
    with pytest.raises(BDecodingError):
        bdecode(b'l4:spam')


def test_bdecode_list_partial():
    value, nconsumed = bdecode(b'l4:spami100eexx', allow_partial=True)
    assert value == [b'spam', 100]
    assert nconsumed == 13


def test_bdecode_list_extra():
    with pytest.raises(BDecodingError):
        bdecode(b'l4:spami100eexx')


def test_bdecode_dict():
    value, nconsumed = bdecode(b'd4:spami100e3:foo3:bare')
    assert value == {b'spam': 100, b'foo': b'bar'}
    assert nconsumed == 23


def test_bdecode_dict_empty():
    value, nconsumed = bdecode(b'de')
    assert value == {}
    assert nconsumed == 2



def test_bdecode_dict_truncated_after_key():
    with pytest.raises(BDecodingError):
        bdecode(b'd4:spami100e3:fooe')


def test_bdecode_dict_truncated_after_value():
    with pytest.raises(BDecodingError):
        bdecode(b'd4:spami100e3:foo3:bar')


def test_bdecode_dict_partial():
    value, nconsumed = bdecode(b'dex', allow_partial=True)
    assert value == {}
    assert nconsumed == 2


def test_bdecode_dict_extra():
    with pytest.raises(BDecodingError):
        bdecode(b'dex')


def test_bdecode_mixed1():
    value, nconsumed = bdecode(b'd1:qli100ei200ee2:ttd1:a1:bee')
    assert value == {b'q': [100, 200], b'tt': {b'a': b'b'}}
    assert nconsumed == 29


def test_bdecode_mixed2():
    value, nconsumed = bdecode(b'li-9edele4:eggse')
    assert value == [-9, {}, [], b'eggs']
    assert nconsumed == 16


def test_bencode_invalid_type1():
    with pytest.raises(TypeError):
        data = bencode(1.2)


def test_bencode_invalid_type2():
    with pytest.raises(TypeError):
        data = bencode(object())


def test_bencode_string():
    data = bencode(b'spam')
    assert data == b'4:spam'


def test_bencode_string_empty():
    data = bencode(b'')
    assert data == b'0:'


def test_bencode_int():
    data = bencode(100)
    assert data == b'i100e'


def test_bencode_int_zero():
    data = bencode(0)
    assert data == b'i0e'


def test_bencode_int_negative():
    data = bencode(-9800)
    assert data == b'i-9800e'


def test_bencode_list():
    data = bencode([b'spam', 100, b'eggs', []])
    assert data == b'l4:spami100e4:eggslee'


def test_bencode_list_empty():
    data = bencode([])
    assert data == b'le'


def test_bencode_list_with_invalid_values1():
    with pytest.raises(TypeError):
        data = bencode([b'spam', 1.2])


def test_bencode_list_with_invalid_values2():
    with pytest.raises(TypeError):
        data = bencode([b'spam', [object()]])


def test_bencode_dict():
    data = bencode({b'spam': b'eggs', b'foo': 100, b'bar': []})
    assert data == b'd4:spam4:eggs3:fooi100e3:barlee'


def test_bencode_dict_empty():
    data = bencode({})
    assert data == b'de'


def test_bencode_dict_invalid_key():
    with pytest.raises(TypeError):
        data = bencode({b'spam': b'eggs', 100: 200})


def test_bencode_dict_invalid_value():
    with pytest.raises(TypeError):
        data = bencode({b'spam': 1.2})


def test_bencode_mixed1():
    data = bencode([100, b'', {b'spam': b'foo', b'eggs': [{}]}])
    assert data == b'li100e0:d4:spam3:foo4:eggsldeeee'


def test_bencode_mixed2():
    data = bencode({b'a': 1, b'b': {}, b'c': {b'x': [1]}})
    assert data == b'd1:ai1e1:bde1:cd1:xli1eeee'
