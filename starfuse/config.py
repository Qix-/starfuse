"""StarFuse configuration/opt-parsing"""

import sys
import argparse
import logging

LOGGERFMT = '%(asctime)s [%(levelname)s] %(name)s <%(funcName)s>: %(message)s'


class Config(object):
    """StarFuse configuration class and option parser"""
    def __init__(self):
        parser = argparse.ArgumentParser(
            prog='starfuse',
            description='Mounts StarBound .pak files as FUSE filesystems')

        parser.add_argument('pakfile', type=str, help='the .pak file on which to operate')
        parser.add_argument('mount_dir', type=str, help='the directory on which to mount the PAK file')
        parser.add_argument('-v', '--verbose', help='be noisy', action='store_true')
        parser.add_argument('-w', '--write', help='allow modifications to the .pak file', action='store_true')
        parser.add_argument('--pages', type=int, help='map this number of pages at a time (default: 256)', default=256)

        self._args = parser.parse_args()

        if self._args.verbose:
            logging.basicConfig(level=logging.DEBUG, format=LOGGERFMT)
        else:
            logging.basicConfig(level=logging.INFO, format=LOGGERFMT)

    @property
    def pak_file(self):
        """Gets the .pak file path to use"""
        return self._args.pakfile

    @property
    def mount_dir(self):
        """Gets the target mount directory"""
        return self._args.mount_dir

    @property
    def page_count(self):
        """Gets the number of pages to map at once"""
        return self._args.pages

    @property
    def read_only(self):
        """Whether or not the file is read-only protected"""
        return not self._args.write

sys.modules[__name__] = Config()
