"""Makes riak transports compatible with gevent by patching socket"""

import gevent
import gevent.socket

import riak
from riak import *
riak.transports.http.socket = gevent.socket
riak.transports.pbc.socket = gevent.socket
