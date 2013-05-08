import os
import setuptools
from distutils.core import setup, Extension

atomic_value = Extension('trpycore.atomic.value', ['trpycore/atomic/value.c'])

def find_packages():
    packages = []
    for dir,subdirs,files in os.walk('trpycore'):
        package = dir.replace(os.path.sep, '.')
        if '__init__.py' not in files:
            # not a package
            continue
        packages.append(package)
    return packages

setup(
    name='trpycore',
    version = '0.10-SNAPSHOT',
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
