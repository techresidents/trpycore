from distutils.core import setup

setup(
    name='trpycore',
    version='0.1-SNAPSHOT',
    author='30and30',
    packages=['trpycore',
              'trpycore.mongrel2',
              'trpycore.mongrel2_gevent',
              'trpycore.thrift_gevent',
              'trpycore.zookeeper',
              'trpycore.zookeeper_gevent',
             ],
    license='LICENSE',
    description='30and30 Python Tech Residents Core Library',
    long_description=open('README').read(),
)
