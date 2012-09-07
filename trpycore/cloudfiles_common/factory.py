from trpycore.factory.base import Factory

class CloudfilesConnectionFactory(Factory):
    """Factory for creating Cloudfile's Connection objects."""

    def __init__(self, username, api_key, timeout, servicenet, connection_class=None, debuglevel=0):
        """CloudfilesConnectionFactory constructor.

        Args:
            username: Rackspace username,
            api_key: Rackspace api key
            servicenet: boolean indicating if Rackspace internal servicenet
                should be used. This should be set to true if cloudfiles
                is being accessed from a Rackspace server, since it will
                prevent charges from being incurred.
            timeout: timeout value in seconds
            debuglevel: optinoal cloudfiles integer debug level
        """
        self.username = username
        self.api_key = api_key
        self.timeout = timeout
        self.servicenet = servicenet
        self.debuglevel = debuglevel

        if connection_class is None:
            import cloudfiles
            self.connection_class = cloudfiles.connection.Connection
    
    def create(self):
        """Return instance of cloudfiles Connection object."""
        return self.connection_class(
                username=self.username,
                api_key=self.api_key,
                timeout=self.timeout,
                servicenet=self.servicenet,
                debuglevel = self.debuglevel)
