import logging

import gevent
import gevent.socket

from thrift import Thrift
from thrift.server import TServer
from thrift.transport import TTransport

class TGeventServer(TServer.TServer):
    """Thrift server compatible with gevent.

    For each request a new greenlet with be spawned to handle the request.
    """

    def serve(self):
        """Accept and process new server requests."""

        self.serverTransport.listen()
        while True:
            try:
                client = self.serverTransport.accept()
                gevent.spawn(self.handle, client)

            except KeyboardInterrupt:
                raise
            except Exception, x:
                logging.exception(x)
    
    def handle(self, client):
        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)

        try:
            while True:
                self.processor.process(iprot, oprot)
        except TTransport.TTransportException, tx:
            pass
        except Exception, x:
            logging.exception(x)

        itrans.close()
        otrans.close()
