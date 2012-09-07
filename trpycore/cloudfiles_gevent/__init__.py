"""Makes Rackspace cloudfiles compatible with gevent by patching socket"""

import gevent
import gevent.socket

import cloudfiles
from cloudfiles import *

cloudfiles.connection.socket = gevent.socket
