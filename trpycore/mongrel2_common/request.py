import cgi
import urlparse

from Cookie import Cookie

try:
    import json
except:
    import simplejson as json

from trpycore.mongrel2_common import tnetstrings


class Request(object):
    """Mongrel2 provided Request class"""

    def __init__(self, sender, conn_id, path, headers, body):
        self.sender = sender
        self.path = path
        self.conn_id = conn_id
        self.headers = headers
        self.body = body
        
        if self.headers['METHOD'] == 'JSON':
            self.data = json.loads(body)
        else:
            self.data = {}

    @staticmethod
    def parse(msg):
        sender, conn_id, path, rest = msg.split(' ', 3)
        headers, rest = tnetstrings.parse(rest)
        body, _ = tnetstrings.parse(rest)

        if type(headers) is str:
            headers = json.loads(headers)

        return Request(sender, conn_id, path, headers, body)

    def is_disconnect(self):
        if self.headers.get('METHOD') == 'JSON':
            return self.data['type'] == 'disconnect'

    def should_close(self):
        if self.headers.get('connection') == 'close':
            return True
        elif self.headers.get('VERSION') == 'HTTP/1.0':
            return True
        else:
            return False

class SafeRequest(object):
    """Wrapper for Mongrel2 Request which adds escape support"""

    def __init__(self, req):
        """SafeRequest constructor.

        Args:
            req: Mongrel2 Request object
        """
        self.req = req
        self.url_params = None
        self.post_params = None
        self.cookies = Cookie(str(self.header("cookie")))

        if self.header("QUERY"):
            self.url_params = urlparse.parse_qs(self.header("QUERY"))

        if self.header("METHOD") == "POST":
            self.post_params = urlparse.parse_qs(self.req.body)
    
    def is_disconnect(self):
        """Returns True if Mongrel2 client disconnected."""
        return self.req.is_disconnect()

    def should_close(self):
        """Returns True if Mongrel2 client requested connection to be closed.."""
        return self.req.should_close()
    
    def header(self, name):
        """Get Http header.

        Args:
            name: Http header name to return.
        
        Returns:
            Header value if present, None otherwise.
        """
        if name in self.req.headers:
            return self.req.headers[name]
        else:
            return None
    
    def cookie(self, name):
        """Get Http cookie.

        Args:
            name: Http cookie name to return.
        
        Returns:
            Cookie value if present, None otherwise.
        """
        if name in self.cookies:
            return self.cookies[name].value
        else:
            return None
    
    def param(self, name, escape=True):
        """Get Http URL parameter or POST parameter.

        Args:
            name: Http URL or POST parameter name to return.
            escape: if True parameter value will be escaped.
        
        Returns:
            Paramater value if present, None otherwise.
        """
        if self.method() == "GET":
            return self.url_param(name, escape)
        else:
            return self.post_param(name, escape)
    
    def url_param(self, name, escape=True):
        """Get Http URL parameter.

        Args:
            name: Http URL parameter name to return.
            escape: if True parameter value will be escaped.
        
        Returns:
            Paramater value if present, None otherwise.
        """
        if name not in self.url_params:
            return None

        value = self.url_params[name][0]
        if escape:
            return self._escape(value)
        else:
            return value

    def post_param(self, name, escape=True):
        """Get Http POST parameter.

        Args:
            name: Http POST parameter name to return.
            escape: if True parameter value will be escaped.
        
        Returns:
            Paramater value if present, None otherwise.
        """
        if name not in self.post_params:
            return None

        value = self.post_params[name][0]
        if escape:
            return self._escape(value)
        else:
            return value

    def method(self):
        """Get Http method (GET, POST, etc...).

        Returns:
            Http method value.
        """
        return self.header("METHOD")

    def _escape(self, input):
        return cgi.escape(input)
