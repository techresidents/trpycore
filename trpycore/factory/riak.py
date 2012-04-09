from __future__ import absolute_import
import riak

from trpycore.factory.base import Factory

class RiakClientFactory(Factory):
    """Factory for creating RiakClient objects."""

    def __init__(self, host, port, transport_class=None):
        """RiakClientFactory constructor.

        Args:
            host: Riak server host.
            port: Riak server port.
            transport: Transport class for connecting to Riak server.
        """
        self.host = host
        self.port = port
        self.transport_class = transport_class
    
    def create(self):
        """Return instance of riak.RiakClient."""
        return riak.RiakClient(self.host, self.port, transport_class=self.transport_class)
