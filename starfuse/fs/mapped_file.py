"""A pseudo-file mapped into memory

Provides better performance for frequent reads/writes,
and makes reading/writing easier via regions (windows)
of memory. Allows memory to be accessed via array reads/
writes as well.
"""

import mmap
import logging

log = logging.getLogger(__name__)


class ReadOnlyError(Exception):
    """The mapped file is flagged as read-only"""
    def __init__(self, path):
        super(ReadOnlyError, self).__init__('mapped file is flagged as read only: %s' % path)


class RegionOverflowError(Exception):
    """Data at an offset was requested but the offset was greater than the allocated size"""
    def __init__(self, offset):
        super(RegionOverflowError, self).__init__('region overflow offset: %d (did you allocate?)' % offset)


class Region(object):
    """A virtual region of mapped memory

    This class is a 'faked' mmap() result that allows for the finer allocation of memory mappings
    beyond/below what the filesystem really allows. It is backed by true mmap()'d pages and
    uses magic methods to achieve the appearance of being an isolated region of memory."""
    __slots__ = 'parent', 'base_offset', '__size', 'cursor'

    def __init__(self, parent, base_offset, size):
        self.parent = parent
        self.base_offset = base_offset
        self.__size = size
        self.cursor = 0

    def __len__(self):
        return self.__size

    def __str__(self):
        return str(self.read(offset=0, length=len(self)))

    def __enter__(self):
        return self

    def __exit__(self, tipo, value, traceback):
        return self

    def region(self, offset=-1, size=-1):
        (offset, size) = self._sanitize_segment(offset, size)
        return self.parent.region(self.base_offset + offset, size)

    def _sanitize_segment(self, offset, length):
        if offset >= len(self):
            raise ValueError('offset falls outside region size')
        elif offset < 0:
            offset = self.cursor

        if length == 0:
            raise ValueError('length must be at least 1')
        elif length < 0:
            length = len(self) - offset

        return (offset, length)

    def read(self, length=-1, offset=-1, advance=True):
        (offset, length) = self._sanitize_segment(offset, length)
        offset += self.base_offset
        result = self.parent.read(length, offset, advance=advance)
        if advance:
            self.cursor += len(result)
        return result

    def write(self, value, length=-1, offset=-1, advance=True):
        if length < 0:
            length = len(value)
        (offset, length) = self._sanitaize_segment(offset, length)
        offset += self.base_offset
        result = self.parent.write(value, length, offset, advance=advance)
        if advance:
            self.cursor += result
        return result


class MappedFile(Region):
    """Manages mmap()-ings of a file into vmem.

    This class prevents virtual address space from growing too large by
    re-using existing maps if the requested regions have already been mapped.
    """
    def __init__(self, path, page_count, read_only=False):
        # XXX TODO NOTE remove this line when write functionality is added.
        read_only = True

        # getting 'too many files open' error? increase the constant on the next line
        # (must be an exponent of 2)
        self._page_size = page_count * mmap.PAGESIZE

        # make sure we're sane here - allocation granularity needs to divide into page size!
        assert (self._page_size % mmap.ALLOCATIONGRANULARITY) == 0, 'page size is not a multiple of allocation granularity!'

        self._file = open(path, 'r+b')
        self._pages = dict()

        self.read_only = read_only
        self._path = path

        self.cursor = 0
        super(MappedFile, self).__init__(self, base_offset=0, size=len(self))

    def __len__(self):
        self._file.seek(0, 2)
        size = self._file.tell()
        return size

    def __del__(self):
        self.close()

    def close(self):
        """Unmaps all mappings"""
        for i in self._pages:
            self._pages[i].close()
        self._file.close()

    def region(self, offset, size):
        """Requests a virtual region be 'allocated'"""
        lower_page = offset - (offset % self._page_size)
        upper_page = ((offset + size) // self._page_size) * self._page_size
        lower_page_id = lower_page // self._page_size
        upper_page_id = upper_page // self._page_size

        # make sure we're mapped
        for i in range(lower_page_id, upper_page_id + 1):
            if i not in self._pages:
                page_offset = i * self._page_size
                page_size = min(self._page_size, len(self) - page_offset)
                log.debug('mapping vfile page: id=%d offset=%d size=%d', i, page_offset, page_size)
                self._pages[i] = mmap.mmap(self._file.fileno(), offset=page_offset, length=page_size)

        # create a region
        return Region(self, base_offset=offset, size=size)

    def read(self, length=1, offset=-1, advance=True):
        """Reads data from the virtual region"""
        (offset, length) = self._sanitize_segment(offset, length)

        results = []
        length = min(length, len(self))

        abs_offset = offset
        cur_page = abs_offset // self._page_size
        abs_offset %= self._page_size

        while length > 0:
            readable = self._page_size - abs_offset
            readable = min(readable, length)

            results.append(self._pages[cur_page][abs_offset:abs_offset + readable])

            length -= readable
            abs_offset = 0
            cur_page += 1

        result = ''.join(results)
        if advance:
            self.cursor += len(result)
        return result

    def write(self, value, offset=-1, length=-1, advance=True):
        if self.read_only:
            raise ReadOnlyError(self._path)

        # TODO
        assert False, 'not implemented'
        return 0

    def __getitem__(self, offset):
        if isinstance(offset, slice):
            (start, fin, step) = offset.indices(len(self))
            result = self.read(offset=start, length=fin - start)
            if step not in [None, 1]:
                result = result[::step]
            return result

        if not isinstance(offset, int):
            raise TypeError('offset is not an integer: %s' % repr(offset))

        if offset >= len(self):
            raise RegionOverflowError(offset)

        page = offset // self._page_size
        rel_offset = offset % self._page_size
        return self._pages[page][rel_offset]

    def __setitem__(self, offset, value):
        if self.read_only:
            raise ReadOnlyError(self._path)

        if isinstance(offset, slice):
            raise ValueError('Slice assignment not supported in mapped files; assemble your data first and then write')

        if not isinstance(offset, int):
            raise TypeError('offset is not an integer: %s' % repr(offset))

        if offset >= len(self):
            raise RegionOverflowError(offset)

        page = offset // self._page_size
        rel_offset = offset % self._page_size
        self._pages[page][rel_offset] = value
