import os
from django.core.cache.backends import memcached


class BMemcached(memcached.BaseMemcachedCache):
    """
    An implementation of a cache binding using python-binary-memcached
    A.K.A BMemcached.
    """
    def __init__(self, server, params):
        import bmemcached
        params.setdefault('OPTIONS', {})

        username = params['OPTIONS'].get('username', params.get('USERNAME', os.environ.get('MEMCACHE_USERNAME')))

        if username:
            params['OPTIONS']['username'] = username

        password = params['OPTIONS'].get('password', params.get('PASSWORD', os.environ.get('MEMCACHE_PASSWORD')))

        if password:
            params['OPTIONS']['password'] = password

        if not server:
            server = tuple(os.environ.get('MEMCACHE_SERVERS', '').split(','))

        super(BMemcached, self).__init__(server, params, library=bmemcached, value_not_found_exception=ValueError)

    def get_many(self, keys, version=None):
        """
        Override this behavior and fix the keys data type.

        The code that was overriden looked like this:

        key_map = {self.make_key(key, version=version): key for key in keys}
        ret = self._cache.get_multi(key_map.keys())
        return {key_map[k]: v for k, v in ret.items()}

        (taken from a44d80f88e22eda24dacef48e368895ebea96635)

        The problem is that key_map would be a dict_keys object, whereas
        in reality, a list object is expected by self._cache.get_multi.

        This would emit stack traces like:

        File ".../python3.6/site-packages/django/core/cache/backends/memcached.py", line 88, in get_many
          ret = self._cache.get_multi(key_map.keys())
        File ".../python3.6/site-packages/bmemcached/client/replicating.py", line 82, in get_multi
          results = server.get_multi(keys)
        File ".../python3.6/site-packages/bmemcached/protocol.py", line 490, in get_multi
          keys, last = keys[:-1], str_to_bytes(keys[-1])
        """
        key_map = {self.make_key(key, version=version): key for key in keys}
        keys = list(key_map)
        ret = self._cache.get_multi(keys)
        return {key_map[k]: v for k, v in ret.items()}

    def close(self, **kwargs):
        # Override base behavior of disconnecting from memcache on every HTTP request.
        # This method is, in practice, only called by Django on the request_finished signal
        pass

    @property
    def _cache(self):
        client = getattr(self, '_client', None)
        if client:
            return client

        if self._options:
            client = self._lib.Client(
                self._servers, self._options.get('username', None),
                self._options.get('password', None)
            )
        else:
            client = self._lib.Client(self._servers,)

        self._client = client

        return client
