#!/usr/bin/env python

import gevent.socket

#Patch TSocket.socket with gevent.socket
from thrift.transport import TSocket
TSocket.socket = gevent.socket 
