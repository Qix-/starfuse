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
    log.info('mounting pakfile %s as %s', config.pak_file, ('read-only' if config.read_only else 'read/write'))
    pak = FusePAK(config.pak_file, page_count=config.page_count, read_only=config.read_only)
    log.info('mounting on %s', config.mount_dir)
    FUSE(pak, config.mount_dir, foreground=True)
