"""VFS tracked, BTree package backed Pakfile

Note that this implementation supports asset pakfiles (including modpak files)
that are keyed with SHA256 digests for StarBound. It will not work with anything else.
"""

import hashlib
import io
import logging

import starfuse.pak.sbon as sbon
from starfuse.pak.btreedb4 import BTreeDB4
from starfuse.fs.vfs import VFS, FileNotFoundError, IsADirError, NotADirError

log = logging.getLogger(__name__)


def encode_key(key):
    digest = hashlib.sha256(key.encode('utf-8')).digest()
    log.debug('encode key=%s digest len=%d', str(key), len(digest))
    return digest


class KeyStore(BTreeDB4):
    """A B-tree database that uses SHA-256 hashes for key lookup."""
    def __init__(self, path, page_count):
        super(KeyStore, self).__init__(path, page_count)

    def encode_key(self, key):
        return encode_key(key)


class Package(KeyStore):
    """A B-tree database representing a package of files."""
    DIGEST_KEY = '_digest'
    INDEX_KEY = '_index'

    def __init__(self, path, page_count):
        super(Package, self).__init__(path, page_count)
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


class Pakfile(object):
    def __init__(self, path, page_count):
        self.vfs = VFS()
        self.pkg = Package(path, page_count)

        log.debug('obtaining file list')
        file_index = self.pkg.get_index()
        log.debug('registering files with virtual filesystem')
        for filepath, lookup in file_index.iteritems():
            self.vfs.add_file(filepath, lookup, mkdirs=True)
        log.info('registered %d files with virtual filesystem', len(file_index))

    def get_entry(self, abspath):
        if abspath is None or abspath == '/':
            return (self.vfs.root, None, self.vfs.root, False)

        names = self.vfs._split_path(abspath)
        direc = self.vfs._mkdirp(names[:-1], srcpath=abspath, mkdirs=False)

        fname = names[-1]

        if fname not in direc:
            raise FileNotFoundError(abspath)

        entry = direc[fname]

        # (directory dict, filename, lookup entry, True if file, False if directory)
        return (direc, fname, entry, not isinstance(entry, dict))

    def get_directory_listing(self, abspath):
        (_, _, lookup, isfile) = self.get_entry(abspath)
        if isfile:
            raise NotADirError(abspath)
        return lookup.keys()

    def get_size(self, abspath):
        (_, _, _, isfile) = self.get_entry(abspath)
        if not isfile:
            raise IsADirError(abspath)
        return self.pkg.get_size(abspath)

    def get_file_contents(self, abspath, offset=0, size=-1):
        (_, _, _, isfile) = self.get_entry(abspath)
        if not isfile:
            raise IsADirError(abspath)
        # XXX this will get more performant when TreeDB4 is refactored to use mappings
        #     instead of full-on reads.
        return self.pkg.get(abspath)[offset:offset + size]

    def readdir(self, abspath):
        (_, _, lookup, isfile) = self.get_entry(abspath)
        if isfile:
            raise NotADirError(abspath)

        results = []
        for name, lobj in lookup.iteritems():
            results.append((name, not isinstance(lobj, dict)))
        return results
