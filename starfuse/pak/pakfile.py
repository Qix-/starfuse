import hashlib
import io
import logging

from starfuse.pak.storage.BTreeDB4 import BTreeDB4
from starfuse.pak import SBON as sbon

log = logging.getLogger(__name__)


def encode_key(key):
    digest = hashlib.sha256(key.encode('utf-8')).digest()
    log.debug('encode key=%s digest len=%d', str(key), len(digest))
    return digest


class KeyStore(BTreeDB4):
    """A B-tree database that uses SHA-256 hashes for key lookup."""
    def __init__(self, path):
        super(KeyStore, self).__init__(path)

    def encode_key(self, key):
        return encode_key(key)


class Package(KeyStore):
    """A B-tree database representing a package of files."""
    DIGEST_KEY = '_digest'
    INDEX_KEY = '_index'

    def __init__(self, path):
        super(Package, self).__init__(path)
        self._index = None

    def encode_key(self, key):
        return super(Package, self).encode_key(key.lower())

    def get_digest(self):
        return self.get(Package.DIGEST_KEY)

    def get_index(self):
        if self._index:
            return self._index

        stream = io.BytesIO(self.get(Package.INDEX_KEY))
        if self.identifier == 'Assets1':
            self._index = sbon.read_string_list(stream)
        elif self.identifier == 'Assets2':
            self._index = sbon.read_string_digest_map(stream)

        return self._index
