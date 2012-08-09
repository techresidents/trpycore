import logging
import threading
import Queue

from thrift.server import TServer
from thrift.transport import TTransport

from trpycore.thrift.transport import TransportClosedException

class TThreadPoolServer(TServer.TServer, object):
    """Server with a fixed size pool of threads which service requests.
    
    Based on the TThreadPoolServer supplied with thrift, with some
    minor enhancements to allow worker threads to gracefully exit
    when stop() is called.  Note that worker threads will only exit
    when they finish servicing currently open connections. This may
    take a considerable amount of time.

    Additionally, stop() will not unblock serve().
    
    In order for proper, but not graceful, termination, it is recommended
    that daemon be set to true.
    """

    STOP_ITEM = object()

    def __init__(self, *args, **kwargs):
        super(TThreadPoolServer, self).__init__(*args)
        self.clients = Queue.Queue()
        self.threads = 10
        self.daemon = kwargs.get("daemon", False)
        self.running = False

    def setNumThreads(self, num):
        """Set the number of worker threads that should be created"""
        self.threads = num

    def serveThread(self):
        """Loop around getting clients from the shared queue and process them."""
        while True:
            try:
                client = self.clients.get()
                if client is TThreadPoolServer.STOP_ITEM:
                    break

                self.serveClient(client)
            except Exception, x:
                logging.exception(x)

    def serveClient(self, client):
        """Process input/output from a client for as long as possible"""
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

    def serve(self):
        """Start a fixed number of worker threads and put client into a queue.

        This method will never return.            
        """
        self.running = True

        for i in range(self.threads):
            try:
                t = threading.Thread(target = self.serveThread)
                t.setDaemon(self.daemon)
                t.start()
            except Exception, x:
                logging.exception(x)

        # Pump the socket for clients
        self.serverTransport.listen()

        errors = 0
        while self.running:
            try:
                #blocking, non-interruptable accept call
                client = self.serverTransport.accept()
                self.clients.put(client)
                errors = 0
            except TransportClosedException:
                pass
            except Exception as error:
                errors += 1
                logging.exception(error)

                if errors >= 10:
                    self.running = False
                    raise
        
        self.running = False

    def stop(self):
        """Stop worker threads.

        Worker threads will not be stopped until they finish
        servicing the currently open connections. This may
        take a considerable amount of time.

        Additionally, calling stop will not cause the serve()
        method to unblock and return.
        """
        self.running = False
        self.serverTransport.close()
        for i in range(self.threads):
            self.clients.put(TThreadPoolServer.STOP_ITEM)
