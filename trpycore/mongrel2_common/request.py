import cgi
import urlparse

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
    """Wrapper for Request which adds escape support"""

    def __init__(self, req):
        self.req = req
        self.url_params = None
        self.post_params = None
        
        if self.header("QUERY"):
            self.url_params = urlparse.parse_qs(self.header("QUERY"))

        if self.header("METHOD") == "POST":
            self.post_params = urlparse.parse_qs(self.req.body)
    
    def header(self, name):
        if name in self.req.headers:
            return self.req.headers[name]
        else:
            return None
    
    def param(self, name, escape=True):
        if self.method() == "GET":
            return self.url_param(name, escape)
        else:
            return self.post_param(name, escape)
    
    def url_param(self, name, escape=True):
        value = self.url_params[name][0]
        if escape:
            return self._escape(value)
        else:
            return value

    def post_param(self, name, escape=True):
        value = self.post_params[name][0]
        if escape:
            return self._escape(value)
        else:
            return value

    def method(self):
        return self.header("METHOD")

    def _escape(self, input):
        return cgi.escape(input)
