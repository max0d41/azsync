from setuptools import setup

setup(
    name='azsync',
    version='0.0.1',
    url='https://github.com/max0d41/azsync',
    description='Disturbed locking and synchronization modules based on AZRPC.',
    packages=[
        'azsync',
    ],
    install_requires=[
        'azrpc>=1.0.1',
    ],
    dependency_links=[
        'https://github.com/max0d41/azrpc/archive/master.zip#egg=azrpc-1.0.1',
    ],
)
