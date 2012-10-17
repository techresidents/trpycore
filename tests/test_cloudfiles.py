import unittest
import time

import testbase
from trpycore.cloudfiles_common.auth import CloudfilesAuthenticator
from trpycore.cloudfiles_common.factory import CloudfilesConnectionFactory

class TestCloudfilesConnectionFactory(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass

    
    def test_factory(self):
        factory = CloudfilesConnectionFactory(
                username="trdev",
                api_key=None,
                password="B88mMJqh",
                timeout=5,
                servicenet=False)

        connection = factory.create()
        connection.list_containers()

class TestCloudfilesAuthenticator(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass

    
    def test_factory(self):
        authenticator = CloudfilesAuthenticator(
                username="trdev",
                api_key=None,
                password="B88mMJqh",
                timeout=5)

        authenticator.authenticate()
        time.sleep(300)
        authenticator.authenticate()
    
if __name__ == "__main__":
    unittest.main()
