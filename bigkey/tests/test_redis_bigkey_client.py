import pytest

from bigkey.redis_bigkey_client import RedisBigKeyClient


def test_bigkey_client():
    client = RedisBigKeyClient()
    key1 = "key1"
    value1 = "1234"
    client.put_big_key(key1, value1, 4)
    assert client.get_big_key(key1) == value1

    client.put_big_key(key1, value1, 2)
    assert client.get_big_key(key1) == value1

    client.put_big_key(key1, value1, 5)
    assert client.get_big_key(key1) == value1

    key2 = "key2"
    assert client.get_big_key(key2) is None
