"""Makes Rackspace cloudfiles compatible with gevent by patching socket"""

import gevent
import gevent.monkey

#Unfortunately no better way to patch cloudfiles since it uses httplib.

#Note that as of 1.7.10 cloudfiles library is not properly calling
#read() on all responses which is causing the next request to 
#fail with ResponseNotReady exception. As a result, this library
#is not currently usable with gevent.
gevent.monkey.patch_socket()
gevent.monkey.patch_ssl()
gevent.monkey.patch_httplib()

import cloudfiles
from cloudfiles import *
