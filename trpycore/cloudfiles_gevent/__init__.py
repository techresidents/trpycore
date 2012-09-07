"""Makes Rackspace cloudfiles compatible with gevent by patching socket"""

import gevent
import gevent.monkey

#unfortunately no better way to patch cloudfiles since it uses httplib.
gevent.monkey.patch_socket()
gevent.monkey.patch_httplib()

import cloudfiles
from cloudfiles import *
