#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
    extra = dict(test_suite="tests.test.suite", include_package_data=True)
except ImportError:
    from distutils.core import setup
    extra = {}

import recovery

def readme():
    with open("README.md") as f:
        return f.read()

setup(
    name='graphite-recovery',
    version=recovery.__version__,
    description='Graphite recovery whisper files from remote machine replic',
    long_description = readme(),
    author='SS',
    author_email='slawomir.skowron@gmail.com',
    url='git@github.com:szibis/graphite-recovery.git',
    install_requires=['argparse','Pygtail','ConfigParser','paramiko', 'statsd-client', 'boto>=2.19.0'],
    zip_safe=False,
    scripts=[
        'bin/graphite-recovery'
    ],
    include_package_data = True,
    packages=find_packages(),
    license='GPL3',
)
