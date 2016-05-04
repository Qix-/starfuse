"""StarFuse, the StarBound packfile FUSE filesystem

This program allows you to mount StarBound PAK files as a directory,
granting you the ability to work directly within the PAK file instead
of having to unpack/re-pack it over and over again.
"""

import starfuse.config as config


def main():
    """StarFuse entry point"""
    print config.pak_file
    print config.mount_dir
