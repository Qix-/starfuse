"""StarFuse configuration/opt-parsing"""

import sys
import argparse


class Config(object):
    """StarFuse configuration class and option parser"""
    def __init__(self):
        parser = argparse.ArgumentParser(
            prog='starfuse',
            description='Mounts StarBound .pak files as FUSE filesystems')

        parser.add_argument('pakfile', type=str)

        self._args = parser.parse_args()

    @property
    def pak_file(self):
        """Gets the .pak file path to use"""
        return self._args.pakfile

sys.modules[__name__] = Config()
