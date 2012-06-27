from distutils.core import setup

import os

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
    version = '0.4.0',
    author = 'Tech Residents, Inc.',
    packages = find_packages(),
    license = 'LICENSE',
    description = 'Tech Residents Core Library',
    long_description = open('README').read(),
)
