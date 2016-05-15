"""StarFuse, the StarBound packfile FUSE filesystem

This program allows you to mount StarBound PAK files as a directory,
granting you the ability to work directly within the PAK file instead
of having to unpack/re-pack it over and over again.
"""

import logging
import starfuse.config as config
from fuse import FUSE
from starfuse.fusepak import FusePAK

log = logging.getLogger(__name__)


def main():
    """StarFuse entry point"""
    log.info('starting StarFuse')
    pak = FusePAK(config.pak_file)
    log.info('mounting on %s', config.mount_dir)
    FUSE(pak, config.mount_dir, foreground=True)
