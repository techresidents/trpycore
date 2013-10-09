import os
from setuptools import setup, find_packages, Extension

import ez_setup
ez_setup.use_setuptools()

atomic_value = Extension('trpycore.atomic.value', ['trpycore/atomic/value.c'])

setup(
    name='trpycore',
    version = '0.14-SNAPSHOT',
    author = 'Tech Residents, Inc.',
    packages = find_packages(),
    ext_modules = [atomic_value],
    license = open('LICENSE').read(),
    description = 'Tech Residents Core Library',
    long_description = open('README').read(),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        ],
)
