"""Implementation of the StarBound block file v2/3 storage"""

import logging
import struct

from starfuse.fs.mapped_file import MappedFile

log = logging.getLogger(__name__)


class InvalidMagic(Exception):
    """A block file has an invalid magic string"""
    def __init__(self, path):
        super(InvalidMagic, self).__init__('a block file has an invalid magic string: %s' % path)


class SBBF03(MappedFile):
    """Implements a StarBound block file v3 store that is backed by a file

    Can also be used to read in v2.

    It's worth noting that the memory regions in this class are mapped and not
    read-in."""
    def __init__(self, path, page_count, read_only=False):
        super(SBBF03, self).__init__(path, page_count, read_only=read_only)

        self._header_size = 0
        self._block_size = 0

        self.header = None
        self.user_header = None
        self.blocks = dict()

        self.__load(path)

    def __del__(self):
        self.close()

    def block_region(self, bid):
        """Gets a block region given the block ID"""
        base_offset = self._header_size + (self._block_size * bid)
        return self.region(offset=base_offset, size=self._block_size)

    @property
    def block_count(self):
        block_region_size = len(self) - self._header_size
        return block_region_size // self._block_size

    def __load(self, path):
        log.debug('loading SBBF03 block file: %s', path)
        region = self.region(0, 32)

        # magic constant
        magic = region.read(6)
        if magic not in [b'SBBF03', b'SBBF02']:
            raise InvalidMagic(path)
        log.debug('block file has valid magic constant: %s', magic)

        # get the header and block size
        # this is all we need to actually read from the file before we start mmap-ing.
        # this is because we want to be able to mmap the header as well, and all we need to know
        # are the header sizes and block sizes.
        (self._header_size, self._block_size) = struct.unpack('>ii', region.read(8))
        log.debug('header_size=%d, block_size=%d', self._header_size, self._block_size)

        # calculate number of blocks
        log.debug('block count: %d', self.block_count)

        # map header
        self.header = self.region(offset=0, size=self._header_size)
        self.user_header = self.header.region(0x20)

        # map user header
        self.user_header = self.header.region(offset=0x20)
        log.debug('mapped headers successfully')
