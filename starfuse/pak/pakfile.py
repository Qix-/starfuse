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


class KeyStore(BTreeDB4):
    """A B-tree database that uses SHA-256 hashes for key lookup."""
    def encode_key(self, key):
        return hashlib.sha256(key.encode('utf-8')).digest()


class Package(KeyStore):
    """A B-tree database representing a package of files."""
    DIGEST_KEY = '_digest'
    INDEX_KEY = '_index'

    def __init__(self, path, page_count, read_only=False):
        super(Package, self).__init__(path, page_count, read_only=read_only)
        self._index = None

    def encode_key(self, key):
        return super(Package, self).encode_key(key.lower())

    def get_digest(self):
        return self.get(Package.DIGEST_KEY)

    def get_index(self):
        if self._index:
            return self._index

        # TODO optimize this to use new regions system after refactoring BTreeDB
        stream = io.BytesIO(self.file_contents(Package.INDEX_KEY))
        if self.identifier == 'Assets1':
            self._index = sbon.read_string_list(stream)
        elif self.identifier == 'Assets2':
            self._index = sbon.read_string_digest_map(stream)

        return self._index


class Pakfile(object):
    def __init__(self, path, page_count, read_only=False):
        self.vfs = VFS()
        self.pkg = Package(path, page_count, read_only=False)

        log.debug('obtaining file list')
        file_index = self.pkg.get_index()
        log.debug('registering files with virtual filesystem')
        for filepath, lookup in file_index.iteritems():
            self.vfs.add_file(filepath, lookup, mkdirs=True)
        log.info('registered %d files with virtual filesystem', len(file_index))

    @property
    def read_only(self):
        return self.pkg.read_only

    @read_only.setter
    def read_only(self, val):
        self.pkg.read_only = val

    def entry(self, abspath):
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

    def directory_listing(self, abspath):
        (_, _, lookup, isfile) = self.entry(abspath)
        if isfile:
            raise NotADirError(abspath)
        return lookup.keys()

    def file_size(self, abspath):
        (_, _, _, isfile) = self.entry(abspath)
        if not isfile:
            raise IsADirError(abspath)
        return self.pkg.file_size(abspath)

    def file_contents(self, abspath, offset=0, size=-1):
        (_, _, _, isfile) = self.entry(abspath)
        if not isfile:
            raise IsADirError(abspath)
        return self.pkg.file_contents(abspath)[offset:offset + size]

    def readdir(self, abspath):
        (_, _, lookup, isfile) = self.entry(abspath)
        if isfile:
            raise NotADirError(abspath)

        results = []
        for name, lobj in lookup.iteritems():
            results.append((name, not isinstance(lobj, dict)))
        return results
