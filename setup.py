# -*- coding: utf-8 -*-


import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

requirements = [
    'gevent>=1.0',
    'pyzmq==13.1.0',
    'thrift==0.9.2',
]
if sys.version_info < (2, 7):
    requirements.append('argparse')

setup(
    name='zerothrift',
    version="0.0.1",
    description='zerothrift is a flexible RPC based on zeromq.',
    author="wfxiang08",
    url='https://github.com/wfxiang08/zerorpc-python',
    packages=['zerothrift', 'zerothrift.core'],
    install_requires=requirements,
    tests_require=['nose'],
    test_suite='nose.collector',
    zip_safe=False,
    license='MIT',
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
    ),
)
