import math

import redis


class RedisBigKeyClient:
    def __init__(self, host='localhost', port=6379, db=0):
        self.client = redis.Redis(host=host, port=port, db=db)

    def __chunk_meta_key(self, key):
        return f'{key}_sub_keys'

    def __sub_key(self, key, idx):
        return f'{key}:chunk_{idx}'

    def put_big_key(self, key, value, chunk_count=1):
        if chunk_count > len(value):
            chunk_count = 1

        self.delete(key)
        chunk_size = math.ceil(len(value) / chunk_count)
        sub_keys = []
        with self.client.pipeline() as pipe:
            for idx in range(chunk_count):
                sub_key = f'{key}:chunk_{idx}'
                sub_val = value[idx * chunk_size:(idx + 1) * chunk_size]
                pipe.set(sub_key, sub_val)
                sub_keys.append(sub_key)
            pipe.rpush(self.__chunk_meta_key(key), *sub_keys)
            pipe.execute()

    def get_big_key(self, key):
        sub_keys = self.client.lrange(self.__chunk_meta_key(key), 0, -1)
        if not sub_keys:
            return None
        sub_values = []
        for sub_key in sub_keys:
            sub_values.append(self.client.get(sub_key))
        return b''.join(sub_values).decode() if all(sub_values) else None

    def delete(self, key):
        sub_keys = self.client.lrange(self.__chunk_meta_key(key), 0, -1)
        if not sub_keys:
            return

        with self.client.pipeline() as pipe:
            pipe.delete(*sub_keys)
            pipe.delete(self.__chunk_meta_key(key))
            pipe.execute()
