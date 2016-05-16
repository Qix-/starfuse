"""
BTreeDB4 implementation

Heavily influenced by Blixt's starfuse-py utilities,
this implementation's sine qua non.
"""
import binascii
import bisect
import io
import struct
import logging

import starfuse.pak.sbon as sbon
from starfuse.pak.sbbf03 import SBBF03

# Override range with xrange when running Python 2.x.
try:
    range = xrange
except:
    pass

log = logging.getLogger(__name__)


class BTreeKeyError(Exception):
    def __init__(self, key):
        super(BTreeKeyError, self).__init__(key)


def keyed(fn):
    def wrapper(*args, **kwargs):
        raw_key = False
        key = None
        try:
            if 'raw_key' in kwargs:
                raw_key = kwargs['raw_key']
                del kwargs['raw_key']

            self = args[0]
            key = args[1]

            if not raw_key:
                key = self.encode_key(key)

            assert len(key) == self.key_size, 'Invalid key length'

            nargs = (self, key) + args[2:]
            return fn(*nargs, **kwargs)
        except (BTreeKeyError, KeyError):
            hex_key = binascii.hexlify(key)
            if raw_key:
                raise BTreeKeyError(hex_key)
            else:
                raise BTreeKeyError('%s (%s)' % (hex_key, args[1]))
    return wrapper


class BTreeDB4(SBBF03):
    """A B-tree database format on top of the SBBF03 block format.

    Note: The developers of this format probably intended for the underlying
    file format to be arbitrary, but this database has pretty strong
    connections to SBBF02 right now so it's been implemented as inheriting from
    that file format. In the future we may want to split away from the
    inheritance chain and instead use the SBBF02 file as an API.
    """
    def __init__(self, path, page_count, read_only=False):
        super(BTreeDB4, self).__init__(path, page_count, read_only=False)

        self.key_size = None

        # Set this attribute to True to make reading more forgiving.
        self.repair = False

        self.alternate_root_node = None
        self.root_node = None
        self.root_node_is_leaf = None

        self.__load()

    def encode_key(self, key):
        """Can be overridden to encode a key before looking for it in the
        database (for example if the key needs to be hashed)."""
        return key

    def block(self, index):
        """Gets a block object given the specified index"""
        region = self.block_region(index)

        signature = bytes(region.read(2))

        if signature in _block_types:
            return _block_types[signature](self, index, region)

        if signature is not b'\0\0':
            raise Exception('Invalid signature detected: %s', signature)

        return None

    @keyed
    def _leaf_for_key(self, key):
        """Returns the binary data for the provided key."""

        block = self.block(self.root_node)
        assert block is not None, 'Root block is None'

        # Scan down the B-tree until we reach a leaf.
        while isinstance(block, BTreeIndex):
            block_number = block.block_for_key(key)
            block = self.block(block_number)
        assert isinstance(block, BTreeLeaf), 'Did not reach a leaf'

        return block

    @keyed
    def file_contents(self, key):
        leaf = self._leaf_for_key(key, raw_key=True)
        stream = LeafReader(self, leaf)

        # The number of keys is read on-demand because only leaves pointed to
        # by an index contain this number (others just contain arbitrary data).
        num_keys, = struct.unpack('>i', stream.read(4))
        assert num_keys < 1000, 'Leaf had unexpectedly high number of keys'
        for i in range(num_keys):
            cur_key = stream.read(self.key_size)
            # TODO do some smarter parsing here (read_bytes is easy to recreate
            # using regions)
            value = sbon.read_bytes(stream)

            if cur_key == key:
                return value

        raise BTreeKeyError(key)

    @keyed
    def file_size(self, key):
        leaf = self._leaf_for_key(key, raw_key=True)
        stream = LeafReader(self, leaf)

        # The number of keys is read on-demand because only leaves pointed to
        # by an index contain this number (others just contain arbitrary data).
        num_keys, = struct.unpack('>i', stream.read(4))
        assert num_keys < 1000, 'Leaf had unexpectedly high number of keys'
        for i in range(num_keys):
            cur_key = stream.read(self.key_size)
            # TODO do much better streaming here when LeafReader is refactored
            size = sbon.read_varlen_number(stream)
            stream.read(size)

            if cur_key == key:
                return size

        raise BTreeKeyError(key)

    def __load(self):
        stream = self.user_header

        # Require that the format of the content is BTreeDB4.
        db_format = sbon.read_fixlen_string(stream, 12)
        assert db_format == 'BTreeDB4', 'Expected binary tree database'
        log.debug('found BTreeDB4 header successfully')

        # Name of the database.
        self.identifier = sbon.read_fixlen_string(stream, 12)
        log.info('database name: %s', self.identifier)

        fields = struct.unpack('>i?xi?xxxi?', stream.read(19))
        self.key_size = fields[0]
        log.debug('key size=%d', self.key_size)

        # Whether to use the alternate root node index.
        self.alternate_root_node = fields[1]
        if self.alternate_root_node:
            self.root_node, self.root_node_is_leaf = fields[4:6]
            self.other_root_node, self.other_root_node_is_leaf = fields[2:4]
        else:
            self.root_node, self.root_node_is_leaf = fields[2:4]
            self.other_root_node, self.other_root_node_is_leaf = fields[4:6]
        log.debug('loaded root nodes: root=%d isleaf=%r', self.root_node, self.root_node_is_leaf)


class BTreeBlock(object):
    def __init__(self, btree, index, region):
        self._btree = btree
        self.index = index
        self.block_region = region


class BTreeIndex(BTreeBlock):
    SIGNATURE = b'II'

    __slots__ = ['keys', 'level', 'num_keys', 'values']

    def __init__(self, btree, index, region):
        super(BTreeIndex, self).__init__(btree, index, region)
        self.level, self.num_keys, left_block = struct.unpack('>Bii', self.block_region.read(9))

        self.keys = []
        self.values = [left_block]

        for i in range(self.num_keys):
            key = self.block_region.read(btree.key_size)
            block, = struct.unpack('>i', self.block_region.read(4))

            self.keys.append(key)
            self.values.append(block)

    def __str__(self):
        return 'Index(level={}, num_keys={})'.format(self.level, self.num_keys)

    def block_for_key(self, key):
        i = bisect.bisect_right(self.keys, key)
        return self.values[i]


class BTreeLeaf(BTreeBlock):
    SIGNATURE = b'LL'

    __slots__ = ['data', 'next_block']

    def __init__(self, btree, index, region):
        super(BTreeLeaf, self).__init__(btree, index, region)
        # Substract 6 for signature and next_block.
        self.data = self.block_region.read(btree._block_size - 6)

        value, = struct.unpack('>i', self.block_region.read(4))
        self.next_block = value if value != -1 else None

    def __str__(self):
        return 'Leaf(next_block={})'.format(self.next_block)


class BTreeFree(BTreeBlock):
    SIGNATURE = b'FF'

    __slots__ = ['next_free_block']

    def __init__(self, btree, index, region):
        super(BTreeFree, self).__init__(btree, index, region)
        self.raw_data = self.block_region.region()
        value, = struct.unpack('>i', self.raw_data[:4])
        self.next_free_block = value if value != -1 else None

    def __str__(self):
        return 'Free(next_free_block={})'.format(self.next_free_block)


class BTreeRestoredLeaf(BTreeLeaf):
    def __init__(self, free_block):
        assert isinstance(free_block, BTreeFree), 'Expected free block'
        self.data = free_block.raw_data[:-4]

        value, = struct.unpack('>i', free_block.raw_data[-4:])
        self.next_block = value if value != -1 else None

    def __str__(self):
        return 'RestoredLeaf(next_block={})'.format(self.next_block)


class LeafReader(object):
    """A pseudo-reader that will cross over block boundaries if necessary."""
    __slots__ = ['_file', '_leaf', '_offset', '_visited']

    def __init__(self, file, leaf):
        assert isinstance(file, BTreeDB4), 'File is not a BTreeDB4 instance'
        assert isinstance(leaf, BTreeLeaf), 'Leaf is not a BTreeLeaf instance'

        self._file = file
        self._leaf = leaf
        self._offset = 0
        self._visited = [leaf.index]

    def read(self, length):
        offset = self._offset

        if offset + length <= len(self._leaf.data):
            self._offset += length
            return self._leaf.data[offset:offset + length]

        buffer = io.BytesIO()

        # If the file is in repair mode, make the buffer available globally.
        if self._file.repair:
            LeafReader.last_buffer = buffer

        # Exhaust current leaf.
        num_read = buffer.write(self._leaf.data[offset:])
        length -= num_read

        # Keep moving onto the next leaf until we have read the desired amount.
        while length > 0:
            next_block = self._leaf.next_block

            assert next_block is not None, 'Tried to read too far'
            assert next_block not in self._visited, 'Tried to read visited block'
            self._visited.append(next_block)

            self._leaf = self._file.block(next_block)
            if self._file.repair and isinstance(self._leaf, BTreeFree):
                self._leaf = BTreeRestoredLeaf(self._leaf)

            assert isinstance(self._leaf, BTreeLeaf), \
                'Leaf pointed to non-leaf %s after reading %d byte(s)' % (next_block, buffer.tell())

            num_read = buffer.write(self._leaf.data[:length])
            length -= num_read

        # The new offset will be how much was read from the current leaf.
        self._offset = num_read

        data = buffer.getvalue()
        buffer.close()

        return data

_block_types = {
    BTreeIndex.SIGNATURE: BTreeIndex,
    BTreeLeaf.SIGNATURE: BTreeLeaf,
    BTreeFree.SIGNATURE: BTreeFree
}
