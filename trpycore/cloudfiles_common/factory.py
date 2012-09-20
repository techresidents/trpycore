from trpycore.factory.base import Factory
from trpycore.cloudfiles_common.auth import CloudfilesAuthenticator

class CloudfilesConnectionFactory(Factory):
    """Factory for creating Cloudfile's Connection objects."""

    def __init__(self,
            username,
            api_key,
            password,
            servicenet,
            timeout=5,
            connection_class=None,
            debuglevel=0):
        """CloudfilesConnectionFactory constructor.

        Args:
            username: Rackspace username,
            api_key: Rackspace api key (required if password not provided)
            password: Rackspace password (required if api_key not provided)
            servicenet: boolean indicating if Rackspace internal servicenet
                should be used. This should be set to true if cloudfiles
                is being accessed from a Rackspace server, since it will
                prevent charges from being incurred.
            timeout: timeout value in seconds
            connection_class: optional cloudfiles-like Connection class.
                If not provided it will default to cloudfiles.Connection.
            debuglevel: optinoal cloudfiles integer debug level
        """
        self.username = username
        self.api_key = api_key
        self.password = password
        self.servicenet = servicenet
        self.timeout = timeout
        self.debuglevel = debuglevel

        if connection_class is None:
            import cloudfiles
            self.connection_class = cloudfiles.Connection
        
        #cloudfiles api only support api-key base authentication.
        #Replace it with an authenticator that will do api-key
        #or password based authentication depending on which
        #is provided.
        self.authenticator = CloudfilesAuthenticator(
                username=self.username,
                api_key=self.api_key,
                password=self.password,
                timeout=self.timeout)

    def create(self):
        """Return instance of cloudfiles Connection object."""
        return self.connection_class(
                username=self.username,
                api_key=self.api_key,
                timeout=self.timeout,
                servicenet=self.servicenet,
                debuglevel = self.debuglevel,
                auth=self.authenticator)
