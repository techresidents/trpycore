import json
import re
import urlparse

from cloudfiles.errors import AuthenticationError, AuthenticationFailed, ResponseError

def parse_url(url):
    (scheme, netloc, path, params, query, frag) = urlparse.urlparse(url)

    # We only support web services
    if not scheme in ('http', 'https'):
        raise ValueError('Scheme must be one of http or https')

    is_ssl = scheme == 'https' and True or False

    # Verify hostnames are valid and parse optional port
    match = re.match(r"([a-zA-Z0-9\-\.]+):?([0-9]{2,5})?", netloc)

    if match:
        (host, port) = match.groups()
        if not port:
            port = is_ssl and '443' or '80'
    else:
        raise ValueError('Invalid host and/or port: %s' % netloc)

    return (host, int(port), path.strip('/'), is_ssl)


class CloudfilesAuthenticator(object):
    def __init__(self,
            username,
            api_key=None,
            password=None,
            timeout=5,
            auth_endpoint=None,
            connection_class=None):

        self.username = username
        self.api_key = api_key
        self.password = password
        self.timeout = timeout
        self.endpoint = auth_endpoint or "https://identity.api.rackspacecloud.com/v2.0"
        self.connection_class = connection_class

        if self.connection_class is None:
            import httplib
            if self.endpoint.startswith("https:"):
                self.connection_class = httplib.HTTPSConnection
            else:
                self.connection_class = httplib.HTTPConnection
        
        #parse endpoint
        self.host, self.port, self.path, self.is_ssl = \
                parse_url(self.endpoint)
     
        #connection
        self.connection = self.connection_class(
                host=self.host,
                port=self.port,
                timeout=self.timeout)


    def authenticate(self):
        if self.api_key is not None:
            response = self.authenticate_api_key(
                    username=self.username,
                    api_key=self.api_key)
        elif self.password is not None:
            response = self.authenticate_password(
                    username=self.username,
                    password=self.password)
        else:
            raise ValueError("api_key or password required.")
        
        
        try:
            default_region = response["access"]["user"]["RAX-AUTH:defaultRegion"]
            auth_token = response["access"]["token"]["id"]
            for service in response["access"]["serviceCatalog"]:
                if service["name"] == "cloudFiles":
                    for endpoint in service["endpoints"]:
                        if endpoint["region"] == default_region:
                            cloudfiles_url = endpoint["publicURL"]
                if service["name"] == "cloudFilesCDN":
                    for endpoint in service["endpoints"]:
                        if endpoint["region"] == default_region:
                            cloudfiles_cdn_url = endpoint["publicURL"]
            
            self.connection.close()

            return cloudfiles_url, cloudfiles_cdn_url, auth_token

        except Exception as error:
            raise AuthenticationError("Invalid response from authentication service: %s" \
                    % str(error))
        

    def authenticate_api_key(self, username, api_key):
        data = """
        {
            "auth": {
            "RAX-KSKEY:apiKeyCredentials": {
                "username": "%s",
                "apiKey": "%s"
                }
            }
        }
        """ % (username, api_key)

        headers = {
            "Content-type": "application/json"
        }
        
        response = self.send_request("POST", "/tokens", data, headers)
        result = json.loads(response.read())
        self.connection.close()
        return result

    def authenticate_password(self, username=None, password=None):
        data = """
        {
            "auth": {
            "passwordCredentials": {
                "username": "%s",
                "password": "%s"
                }
            }
        }
        """ % (username, password)
        headers = {
            "Content-type": "application/json"
        }

        response = self.send_request("POST", "/tokens", data, headers)
        result = json.loads(response.read())
        self.connection.close()
        return result

    def default_headers(self, method, path, data):
        headers = {
            'Content-Length': str(len(data)),
        }
        return headers
    
    def send_request(self, method, path, data=None, headers=None):
        data = data or ""
        user_headers = headers
        headers = self.default_headers(method, path, data)
        if user_headers is not None:
            headers.update(user_headers)

        path = "/%s/%s" % (self.path, path.strip("/"))

        try:
            self.connection.request(method, path, data, headers)
            response = self.connection.getresponse()
            self.validate_response(response)
        except ResponseError as error:
            if error.status == 401:
                raise AuthenticationFailed(error.reason)
            else:
                raise
        return response

    def validate_response(self, response):
        if response.status < 200 or response.status > 299:
            response.read()
            raise ResponseError(response.status, response.reason)
