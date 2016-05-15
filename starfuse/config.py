"""StarFuse configuration/opt-parsing"""

import sys
import argparse
import logging


class Config(object):
    """StarFuse configuration class and option parser"""
    def __init__(self):
        parser = argparse.ArgumentParser(
            prog='starfuse',
            description='Mounts StarBound .pak files as FUSE filesystems')

        parser.add_argument('pakfile', type=str, help='the .pak file on which to operate')
        parser.add_argument('mount_dir', type=str, help='the directory on which to mount the PAK file')
        parser.add_argument('-v', '--verbose', help='Be noisy', action='store_true')

        self._args = parser.parse_args()

        if self._args.verbose:
            logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s <%(funcName)s>: %(message)s')

    @property
    def pak_file(self):
        """Gets the .pak file path to use"""
        return self._args.pakfile

    @property
    def mount_dir(self):
        """Gets the target mount directory"""
        return self._args.mount_dir

sys.modules[__name__] = Config()
