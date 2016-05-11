"""Virtual File System, which manages paths and their lookups"""

import os
import os.path
import logging

assert os.sep == '/', 'Starfuse cannot be used on non-unix systems'

log = logging.getLogger(__name__)


class NotADirError(Exception):
    pass


class IsADirError(Exception):
    pass


class FileNotFoundError(Exception):
    pass


class VFS(object):
    def __init__(self):
        self.root = dict()

    def add_file(self, abspath, lookup=None, mkdirs=False):
        names = self._split_path(abspath)
        direc = self._mkdirp(names[:-1], srcpath=abspath, mkdirs=mkdirs)

        filename = names[-1]

        if filename in direc:
            if isinstance(direc[filename], dict):
                raise IsADirError(abspath)
            return

        direc[filename] = lookup

    def lookup_file(self, abspath):
        names = self._split_path(abspath)
        direc = self._mkdirp(names[:-1], srcpath=abspath)

        filename = names[-1]
        if filename not in direc:
            raise FileNotFoundError(Exception)

        return direc[filename]

    def _mkdirp(self, names, srcpath=None, mkdirs=False):
        cur = self.root
        for name in names:
            if name not in cur:
                if not mkdirs:
                    raise FileNotFoundError(srcpath)
                cur[name] = dict()
            elif not isinstance(cur, dict):
                raise NotADirError(srcpath)
            cur = cur[name]
        return cur

    def _split_path(self, path):
        return path.strip(os.sep).split(os.sep)
