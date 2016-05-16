"""FUSE glue between the filesystem handlers and the PakVFS"""

import os
import errno
import logging
from threading import Lock
from fuse import FuseOSError, Operations, LoggingMixIn
from stat import S_IFDIR, S_IFREG
from time import time
from starfuse.pak.pakfile import Pakfile
from starfuse.fs.vfs import FileNotFoundError, IsADirError, NotADirError

log = logging.getLogger(__name__)


def fuse_op(fn):
    def handled_fn(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except FileNotFoundError as e:
            raise FuseOSError(errno.ENOENT)
        except IsADirError as e:
            raise FuseOSError(errno.EISDIR)
        except NotADirError as e:
            raise FuseOSError(errno.ENOTDIR)
        except Exception as e:
            log.exception('EIO: %s', e.message)
            raise FuseOSError(errno.EIO)
    return handled_fn


class FusePAK(LoggingMixIn, Operations):
    """FUSE operations implementation for StarBound PAK files"""
    def __init__(self, pakfile, page_count, read_only=False):
        self.pakfile = Pakfile(pakfile, page_count, read_only=read_only)
        self._lock = Lock()

    def make_file_struct(self, size, isfile=True, ctime=time(), mtime=time(), atime=time(), read_only=False):
        stats = dict()
        # TODO replace uncommented modes with commented when write ability is added
        if isfile:
            stats['st_mode'] = S_IFREG | 0o0444
        else:
            stats['st_mode'] = S_IFDIR | 0o0555

        if not self.pakfile.read_only:
            stats['st_mode'] |= 0o0200

        stats['st_uid'] = os.getuid()
        stats['st_gid'] = os.getgid()
        stats['st_nlink'] = 1
        stats['st_ctime'] = ctime
        stats['st_mtime'] = mtime
        stats['st_atime'] = atime
        stats['st_size'] = size
        return stats

    @fuse_op
    def access(self, path, mode):
        return 0

    @fuse_op
    def flush(self, path, fn):
        return 0

    @fuse_op
    def fsync(self, path, datasync, fh):
        # we don't need to worry about fsync in StarFuse. :)
        return 0

    @fuse_op
    def getattr(self, path, fh=None):
        if path == '/':
            return self.make_file_struct(0, isfile=False)
        (_, _, _, isfile) = self.pakfile.entry(path)
        if not isfile:
            return self.make_file_struct(0, isfile=False)
        size = self.pakfile.file_size(path)
        return self.make_file_struct(size)

    @fuse_op
    def readdir(self, path, fh=None):
        return self.pakfile.directory_listing(path)

    @fuse_op
    def read(self, path, size, offset, fh=None):
        return self.pakfile.file_contents(path, offset, size)
