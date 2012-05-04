"""Import this package to patch psycopg2 for compatibility with gevent."""

import psycopg2
from psycopg2 import extensions

import gevent.socket

def gevent_wait_callback(connection, timeout=None):
    while True:
        state = connection.poll()
        if state == extensions.POLL_OK:
            break
        elif state == extensions.POLL_READ:
            gevent.socket.wait_read(connection.fileno(), timeout=timeout)
        elif state == extensions.POLL_WRITE:
            gevent.socket.wait_write(connection.fileno(), timeout=timeout)
        else:
            raise psycopg2.OperationalError("Unexcepted state: %s" % state)

extensions.set_wait_callback(gevent_wait_callback)
