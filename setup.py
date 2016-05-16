from distutils.core import setup
setup(
    name='starfuse',
    packages=['starfuse', 'starfuse.pak', 'starfuse.pak', 'starfuse.fs'],
    version='0.3.0',
    description='Mount StarBound .pak files as FUSE filesystems',
    author='Josh Junon',
    author_email='i.am.qix@gmail.com',
    url='https://github.com/qix-/starfuse',
    download_url='https://github.com/qix-/starfuse/tarball/0.1.0',
    keywords=['starfuse', 'starbound', 'fuse', 'pak', 'development', 'mod', 'extension'],
    classifiers=[],
    install_requires=['fusepy'],
    entry_points={
        'console_scripts': [
            'starfuse = starfuse.main:main'
        ],
    },
)
