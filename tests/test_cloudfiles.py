import unittest

import testbase
from trpycore.cloudfiles_common.factory import CloudfilesConnectionFactory

class TestTrie(unittest.TestCase):
    
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
    
if __name__ == "__main__":
    unittest.main()
