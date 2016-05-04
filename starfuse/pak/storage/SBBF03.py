"""Implementation of the StarBound block file v2/3 storage"""

import mmap
import logging
import struct

log = logging.getLogger(__name__)


class InvalidBlockFileMagic(Exception):
    """A block file has an invalid magic string"""
    def __init__(self, path):
        super(InvalidBlockFileMagic, self).__init__('a block file has an invalid magic string: %s' % path)


class RegionOverflowError(Exception):
    """Data at an offset was requested but the offset was greater than the allocated size"""
    def __init__(self, offset):
        super(RegionOverflowError, self).__init__('region overflow offset: %s' % offset)


class VirtualFile(object):
    """Manages mmap()-ings of a file into vmem.

    This class prevents virtual address space from growing too large by
    re-using existing maps if the requested regions have already been mapped.
    """
    def __init__(self, path):
        # make sure we're sane here - allocation granularity needs to divide into page size!
        assert (mmap.PAGESIZE % mmap.ALLOCATIONGRANULARITY) == 0, 'page size is not a multiple of allocation granularity! you\'re on a really messed up POSIX system...'

        self._file = open(path, 'r+b')
        self.pages = dict()

    def __len__(self):
        self._file.seek(0, 2)
        return self._file.tell()

    def close(self):
        """Unmaps all mappings"""
        for i in self.pages:
            self.pages[i].close()
        self._file.close()

    def region(self, offset, size):
        """Requests a virtual region to be 'allocated'"""
        lower_page = offset - (offset % mmap.PAGESIZE)
        upper_page = ((offset + size) // mmap.PAGESIZE) * mmap.PAGESIZE
        lower_page_id = lower_page // mmap.PAGESIZE
        upper_page_id = upper_page // mmap.PAGESIZE

        # make sure we're mapped
        for i in range(lower_page_id, upper_page_id + 1):
            if i not in self.pages:
                log.debug('mapping vfile page: id=%d offset=%d', i, i * mmap.PAGESIZE)
                self.pages[i] = mmap.mmap(self._file.fileno(), offset=i * mmap.PAGESIZE, length=mmap.PAGESIZE)

        # create a region
        return VirtualRegion(self.pages, base_page=lower_page_id, base_offset=offset - lower_page, size=size)


class VirtualRegion(object):
    """A virtual region of mapped memory

    This class is a 'faked' mmap() result that allows for the finer allocation of memory mappings
    beyond/below what the filesystem really allows. It is backed by true mmap()'d pages and
    uses magic methods to achieve the appearance of being an isolated region of memory."""
    def __init__(self, pages, base_page, base_offset, size):
        self._pages = pages
        self.base_page = base_page
        self.base_offset = base_offset
        self.size = size
        self.cursor = 0

    def __len__(self):
        return self.size

    def __str__(self):
        return self.read(offset=0, length=self.size)

    def _get_offset_page(self, offset):
        abs_offset = self.base_offset + offset
        return (abs_offset // mmap.PAGESIZE) + self.base_page, abs_offset % mmap.PAGESIZE

    def __getitem__(self, offset):
        if not isinstance(offset, int):
            raise TypeError('offset is not an integer: %s' % repr(offset))

        if offset >= self.size:
            raise RegionOverflowError(offset)

        page, rel_offset = self._get_offset_page(offset)
        return self._pages[page][rel_offset]

    def __setitem__(self, offset, value):
        if not isinstance(offset, int):
            raise TypeError('offset is not an integer: %s' % repr(offset))

        if offset >= self.size:
            raise RegionOverflowError(offset)

        page, rel_offset = self._get_offset_page(offset)
        self._pages[page][rel_offset] = value
        return value

    def __enter__(self):
        return self

    def __exit__(self, tipo, value, traceback):
        return self

    def read(self, length=1, offset=-1):
        """Reads data from the virtual region"""
        if offset == -1:
            offset = self.cursor

        results = []
        length = min(length, self.size)
        log.debug('length=%d', length)
        abs_offset = offset + self.base_offset
        log.debug('abs_offset=%d', abs_offset)

        cur_page = self.base_page
        while length > 0:
            readable = mmap.PAGESIZE - abs_offset
            readable = min(readable, length)
            log.debug('read page: %d, %d', abs_offset, readable)

            results.append(self._pages[cur_page][abs_offset:abs_offset + readable])

            length -= readable
            abs_offset = 0
            cur_page += 1

        result = ''.join(results)
        self.cursor += len(result)
        return result


class SBBF03(object):
    """Implements a StarBound block file v3 store that is backed by a file

    Can also be used to read in v2.

    It's worth noting that the memory regions in this class are mapped and not
    read-in."""
    def __init__(self, path):
        self._vfile = VirtualFile(path)

        self._header_size = 0
        self._block_size = 0

        self.header = None
        self.blocks = dict()

        self.__load(path)

    def __del__(self):
        self.close()

    def close(self):
        """Closes the virtual file and the real file handles"""
        self._vfile.close()

    def __load(self, path):
        log.debug('loading SBBF03 block file: %s', path)
        region = self._vfile.region(0, 32)

        # magic constant
        magic = region.read(6)
        if magic not in [b'SBBF03', b'SBBF02']:
            raise InvalidBlockFileMagic(path)
        log.debug('block file has valid magic constant')

        # get the header and block size
        # this is all we need to actually read from the file before we start mmap-ing.
        # this is because we want to be able to mmap the header as well, and all we need to know
        # are the header sizes and block sizes.
        (self._header_size, self._block_size) = struct.unpack('>ii', region.read(8))
        log.debug('header_size=%d, block_size=%d', self._header_size, self._block_size)

        # calculate number of blocks
        block_region_size = len(self._vfile) - self._header_size
        self._block_count = block_region_size / self._block_size
        log.debug('block count: %d', self._block_count)
        if not float(float(block_region_size) / float(self._block_size)).is_integer():
            log.warning('detected trailing bytes on file; block file may be corrupt')

        # map header
        self.header = self._vfile.region(offset=0, size=self._header_size)
