"""StarFuse, the StarBound packfile FUSE filesystem

This program allows you to mount StarBound PAK files as a directory,
granting you the ability to work directly within the PAK file instead
of having to unpack/re-pack it over and over again.
"""

import logging
import starfuse.config as config
from starfuse.pakfile import Pakfile

log = logging.getLogger(__name__)


def main():
    """StarFuse entry point"""
    log.info('starting StarFuse')
    pak = Pakfile(config.pak_file)
    log.debug('%r', pak.readdir('/'))
