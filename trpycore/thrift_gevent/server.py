import logging

import gevent
import gevent.socket

from thrift import Thrift
from thrift.server import TServer
from thrift.transport import TTransport

from trpycore.thrift_gevent.transport import TransportClosedException

class TGeventServer(TServer.TServer, object):
    """Thrift server compatible with gevent.

    For each request a new greenlet with be spawned to handle the request.
    """

    def __init__(self, *args, **kwargs):
        super(TGeventServer, self).__init__(*args, **kwargs)
        self.running = False

    def serve(self):
        """Accept and process new server requests."""
        self.running = True

        self.serverTransport.listen()
        
        errors = 0
        while self.running:
            try:
                client = self.serverTransport.accept()
                gevent.spawn(self.handle, client)
                errors = 0

            except KeyboardInterrupt:
                self.running = False
                raise
            except TransportClosedException:
                pass
            except Exception as error:
                errors += 1
                logging.exception(error)

                if errors >= 10:
                    self.running = False
                    raise
        
        self.running = False
    
    def handle(self, client):
        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)

        try:
            while True:
                self.processor.process(iprot, oprot)
        except TTransport.TTransportException:
            pass
        except Exception as error:
            logging.exception(error)

        itrans.close()
        otrans.close()

    def stop(self):
        self.running = False
        self.serverTransport.close()
