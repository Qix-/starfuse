"""StarFuse configuration/opt-parsing"""

import sys


class Config(object):
    """StarFuse configuration class and option parser"""
    @property
    def pak_file(self):
        """Gets the .pak file path to use"""
        return 'pakfile!'

    @property
    def mount_dir(self):
        """Gets the requested mount directory path"""
        return 'mountdir!!!'

sys.modules[__name__] = Config()
