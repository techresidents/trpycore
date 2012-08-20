import errno
import os
import select
import socket
import threading

from thrift.transport import TTransport
from thrift.transport import TSocket

class TransportClosedException(Exception):
    pass

class TNonBlockingServerSocket(TSocket.TSocketBase, TTransport.TServerTransportBase):
    """Non-blocking socket implementation of TServerTransport base.
    
    Server socket with a non-blocking accept call. This allows for
    graceful server termination. accept() will raise a
    TransportClosedException in the event that it is closed.
    """

    def __init__(self, host=None, port=9090, unix_socket=None):
        self.host = host
        self.port = port
        self._unix_socket = unix_socket
        self.handle = None
        self.exit_pipe = os.pipe()
        self.exit_event = threading.Event()
        self.read_descriptors = [self.exit_pipe[0]]

    def listen(self):
        res0 = self._resolveAddr()
        for res in res0:
            if res[0] is socket.AF_INET6 or res is res0[-1]:
                break

        # We need remove the old unix socket if the file exists and
        # nobody is listening on it.
        if self._unix_socket:
            tmp = socket.socket(res[0], res[1])
            try:
                tmp.connect(res[4])
            except socket.error, err:
                eno, message = err.args
                if eno == errno.ECONNREFUSED:
                    os.unlink(res[4])

        self.handle = socket.socket(res[0], res[1])
        self.read_descriptors.append(self.handle)
        self.handle.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(self.handle, 'settimeout'):
            self.handle.settimeout(None)
        self.handle.bind(res[4])
        self.handle.listen(128)

    def accept(self):
        readable, writable, errored = select.select(self.read_descriptors, [], [])
        for s in readable:
            if s is self.handle:
                client, addr = self.handle.accept()
                result = TSocket.TSocket()
                result.setHandle(client)
                return result
            elif s is self.exit_pipe[0]:
                self.exit_event.set()
                raise TransportClosedException()

    def close(self):
        os.write(self.exit_pipe[1], '\0')
        self.exit_event.wait(1)
        if self.handle is not None:
            self.handle.close()
