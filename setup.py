# -*- coding: utf-8 -*-

# execfile() doesn't exist in Python 3, this way we are compatible with both.
exec(compile(open('zerorpc/version.py').read(), 'zerorpc/version.py', 'exec'))

import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


requirements = [
    'gevent>=1.0',
    'pyzmq==13.1.0'
    'thrift==0.9.2',
]
if sys.version_info < (2, 7):
    requirements.append('argparse')


setup(
    name='zerorpc',
    version=__version__,
    description='zerorpc is a flexible RPC based on zeromq.',
    author=__author__,
    url='https://github.com/dotcloud/zerorpc-python',
    packages=['zerorpc', 'zerorpc.core'],
    install_requires=requirements,
    tests_require=['nose'],
    test_suite='nose.collector',
    zip_safe=False,
    entry_points={'console_scripts': ['zerorpc = zerorpc.cli:main']},
    license='MIT',
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
    ),
)
