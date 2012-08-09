import logging
import os
import unittest
import time


from trpycore.zookeeper.client import ZookeeperClient
from trpycore.zookeeper.watch import ChildrenWatch, DataWatch, HashringWatch
from trpycore.zookeeper.util import expire_zookeeper_client_session

class TestDataWatch(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)

        cls.watch_path = "/unittest/datawatch"
        cls.watch_data = "unittest_data"
        cls.zookeeper_client.create(cls.watch_path, cls.watch_data)
        time.sleep(1)
        
        cls.watch_observer_count = 0
        cls.watch_observer_data = None
        def watch_observer(watch):
            cls.watch_observer_count += 1
            cls.watch_observer_data = cls.watch.get_data()
        
        cls.watch = DataWatch(
                cls.zookeeper_client,
                cls.watch_path,
                watch_observer)
        cls.watch.start()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.watch.stop()
        cls.zookeeper_client.delete(cls.watch_path)
        cls.zookeeper_client.stop()

    def test_get_data(self):
        pass
        #data = self.watch.get_data()
        #self.assertEqual(data, self.watch_data)
        #self.assertEqual(self.watch_observer_data, self.watch_data)

    def test_data_change(self):
        data = self.watch.get_data()
        self.assertEqual(data, self.watch_data)
        self.assertEqual(self.watch_observer_data, self.watch_data)
        
        new_data = "test_data_change"
        count = self.watch_observer_count
        self.zookeeper_client.set_data(self.watch_path, new_data)
        time.sleep(1)

        data = self.watch.get_data()
        self.assertEqual(data, new_data)
        self.assertEqual(self.watch_observer_data, new_data)
        self.assertEqual(self.watch_observer_count, count+1)

        count = self.watch_observer_count
        self.zookeeper_client.set_data(self.watch_path, self.watch_data)
        time.sleep(1)

        data = self.watch.get_data()
        self.assertEqual(data, self.watch_data)
        self.assertEqual(self.watch_observer_data, self.watch_data)
        self.assertEqual(self.watch_observer_count, count+1)

    def test_session_expiration(self):
        expired_session_data = []

        def observer(event):
            if event.state_name == "EXPIRED_SESSION_STATE":
                expired_session_data.append(self.watch.get_data())
        #add session observer for testing
        self.zookeeper_client.add_session_observer(observer)
        
        count = self.watch_observer_count
        expired_result = expire_zookeeper_client_session(self.zookeeper_client, 10)
        self.assertEqual(expired_result, True)
        time.sleep(1)

        self.assertEqual(expired_session_data[0], None)
        self.assertEqual(self.watch_observer_count, count+1)
        data = self.watch.get_data()
        self.assertEqual(data, self.watch_data)
        self.assertEqual(self.watch_observer_data, self.watch_data)

class TestDataWatchNoNode(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)

        cls.watch_path = "/unittest/datawatch"
        cls.watch_data = "unittest_data"
        
        if cls.zookeeper_client.exists(cls.watch_path):
            cls.zookeeper_client.delete(cls.watch_path)
        time.sleep(1)

        cls.watch_observer_count = 0
        cls.watch_observer_data = None
        def watch_observer(watch):
            cls.watch_observer_count += 1
            cls.watch_observer_data = cls.watch.get_data()
        
        cls.watch = DataWatch(
                cls.zookeeper_client,
                cls.watch_path,
                watch_observer)
        cls.watch.start()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.watch.stop()
        cls.zookeeper_client.stop()
        cls.zookeeper_client.join()

    def test_get_data(self):
        data = self.watch.get_data()
        self.assertEqual(data, None)

        count = self.watch_observer_count

        #create node
        self.zookeeper_client.create(self.watch_path, self.watch_data)
        time.sleep(1)
        data = self.watch.get_data()
        self.assertEqual(self.watch_observer_count, count+1)
        self.assertEqual(data, self.watch_data)

        #delete node
        self.zookeeper_client.delete(self.watch_path)
        time.sleep(1)
        data = self.watch.get_data()
        self.assertEqual(self.watch_observer_count, count+2)
        self.assertEqual(data, None)

class TestChildrenWatch(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)

        cls.watch_path = "/unittest/childrenwatch"
        cls.children =[("child1", "child1data"), ("child2", "child2data")]
        cls.zookeeper_client.create(cls.watch_path, "parentdata")
        for child, child_data in cls.children:
            cls.zookeeper_client.create(os.path.join(cls.watch_path, child), child_data)
        time.sleep(1)
        
        cls.watch_observer_count = 0
        cls.watch_observer_children = None
        def watch_observer(watch):
            cls.watch_observer_count += 1
            cls.watch_observer_children = cls.watch.get_children()
        
        cls.watch = ChildrenWatch(
                cls.zookeeper_client,
                cls.watch_path,
                watch_observer)
        cls.watch.start()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.watch.stop()
        for child, child_data in cls.children:
            cls.zookeeper_client.delete(os.path.join(cls.watch_path, child))
        cls.zookeeper_client.delete(cls.watch_path)
        cls.zookeeper_client.stop()

    def test_get_children(self):
        children = self.watch.get_children()
        self.assertEqual(len(children), len(self.children))
        for child, child_data in self.children:
            self.assertEqual(children[child][0], child_data)

        for child, child_data in self.children:
            self.assertEqual(self.watch_observer_children[child][0], child_data)
    
    def test_children_change(self):
        children = self.watch.get_children()
        self.assertEqual(len(children), len(self.children))
        for child, child_data in self.children:
            self.assertEqual(children[child][0], child_data)
       
        count = self.watch_observer_count

        #add new child
        self.zookeeper_client.create(os.path.join(self.watch_path, "child100"), "child100data")
        time.sleep(1)
        children = self.watch.get_children()
        self.assertEqual(children, self.watch_observer_children)
        self.assertEqual(self.watch_observer_count, count+1)
        self.assertEqual(len(children), len(self.children)+1)
        for child, child_data in self.children:
            self.assertEqual(children[child][0], child_data)
        self.assertEqual(children["child100"][0], "child100data")

        #remove child
        self.zookeeper_client.delete(os.path.join(self.watch_path, "child100"))
        time.sleep(1)
        children = self.watch.get_children()
        self.assertEqual(children, self.watch_observer_children)
        self.assertEqual(len(children), len(self.children))
        self.assertEqual(self.watch_observer_count, count+2)
        for child, child_data in self.children:
            self.assertEqual(children[child][0], child_data)

    def test_session_expiration(self):
        expired_session_children = {}

        def observer(event):
            if event.state_name == "EXPIRED_SESSION_STATE":
                expired_session_children.update(self.watch.get_children())
        #add session observer for testing
        self.zookeeper_client.add_session_observer(observer)
        
        count = self.watch_observer_count

        expired_result = expire_zookeeper_client_session(self.zookeeper_client, 10)
        self.assertEqual(expired_result, True)
        time.sleep(1)

        self.assertEqual(len(expired_session_children), 0)
        self.assertEqual(self.watch_observer_count, count+1)
        children = self.watch.get_children()
        self.assertEqual(children, self.watch_observer_children)
        self.assertEqual(len(children), len(self.children))
        for child, child_data in self.children:
            self.assertEqual(children[child][0], child_data)

class TestChildrenWatchNoNode(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)

        cls.watch_path = "/unittest/childrenwatch"
        cls.children =[("child1", "child1data"), ("child2", "child2data")]

        if cls.zookeeper_client.exists(cls.watch_path):
            for child, child_data in cls.children:
                cls.zookeeper_client.delete(os.path.join(cls.watch_path, child))
            cls.zookeeper_client.delete(cls.watch_path)
        time.sleep(1)
        
        cls.watch_observer_count = 0
        cls.watch_observer_children = None
        def watch_observer(watch):
            cls.watch_observer_count += 1
            cls.watch_observer_children = cls.watch.get_children()
        
        cls.watch = ChildrenWatch(
                cls.zookeeper_client,
                cls.watch_path,
                watch_observer)
        cls.watch.start()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.watch.stop()
        cls.zookeeper_client.stop()
        cls.zookeeper_client.join()

    def test_get_children(self):
        children = self.watch.get_children()
        self.assertEqual(len(children), 0)
        
        #create nodes
        count = self.watch_observer_count
        self.zookeeper_client.create(self.watch_path, "parentdata")
        time.sleep(1)
        for child, child_data in self.children:
            self.zookeeper_client.create(os.path.join(self.watch_path, child), child_data)
            time.sleep(1)
        self.assertEqual(self.watch_observer_count, count+3)

        children = self.watch.get_children()
        self.assertEqual(children, self.watch_observer_children)
        for child, child_data in self.children:
            self.assertEqual(children[child][0], child_data)
        
        #delete nodes
        count = self.watch_observer_count
        for child, child_data in self.children:
            self.zookeeper_client.delete(os.path.join(self.watch_path, child))
            time.sleep(1)
        self.zookeeper_client.delete(self.watch_path)
        time.sleep(1)
        self.assertEqual(self.watch_observer_count, count+3)
        children = self.watch.get_children()
        self.assertEqual(children, self.watch_observer_children)
        self.assertEqual(len(children), 0)

class TestHashringWatch(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)
        
        cls.hashring_path = "/unittest/hashring"
        cls.hashring_data = "unittest_data"
        cls.hashring_positions = [0xcfcd208495d565ef66e7dff9f98764da, 0xf899139df5e1059396431415e770c6dd, 0x0]
        cls.hashring_watch = HashringWatch(
                cls.zookeeper_client,
                cls.hashring_path,
                positions=cls.hashring_positions,
                position_data=cls.hashring_data)
        cls.hashring_watch.start()
        time.sleep(1)

        logging.basicConfig(level=logging.DEBUG)

    @classmethod
    def tearDownClass(cls):
        cls.hashring_watch.stop()
        cls.zookeeper_client.stop()

    def test_hashring_length(self):
        hashring = self.hashring_watch.hashring()
        self.assertEqual(len(hashring), 3)

    def test_hashring_order(self):
        hashring = self.hashring_watch.hashring()
        self.assertEqual(hashring[0].token, 0x0)
        self.assertEqual(hashring[1].token, 0xcfcd208495d565ef66e7dff9f98764da)
        self.assertEqual(hashring[2].token, 0xf899139df5e1059396431415e770c6dd)

    def test_hashring_data(self):
        hashring = self.hashring_watch.hashring()
        for node in hashring:
            self.assertEqual(node.data, "unittest_data")
    
    def test_preflist_length(self):
        preference_list = self.hashring_watch.preference_list('0')
        self.assertEqual(len(preference_list), 3)

    def test_preflist_order(self):
        #md5 of '0' is cfcd208495d565ef66e7dff9f98764da,
        #so the first node in the preference list should
        #be 0xf899139df5e1059396431415e770c6dd.
        preference_list = self.hashring_watch.preference_list('0')
        self.assertEqual(preference_list[0].token, 0xf899139df5e1059396431415e770c6dd)
        self.assertEqual(preference_list[1].token, 0x0)
        self.assertEqual(preference_list[2].token, 0xcfcd208495d565ef66e7dff9f98764da)

    def test_find_hashring_node(self):
        #md5 of '0' is cfcd208495d565ef66e7dff9f98764da,
        #so the node should be 0xf899139df5e1059396431415e770c6dd.
        node = self.hashring_watch.find_hashring_node('0')
        self.assertEqual(node.token, 0xf899139df5e1059396431415e770c6dd)

        #md5 of '1' is c4ca4238a0b923820dcc509a6f75849b,
        #so the should be cfcd208495d565ef66e7dff9f98764da.
        node = self.hashring_watch.find_hashring_node('1')
        self.assertEqual(node.token, 0xcfcd208495d565ef66e7dff9f98764da)

    def test_hashring_node_join(self):
        hashring_watch = HashringWatch(
                self.zookeeper_client,
                self.hashring_path,
                positions=[0xdfcd208495d565ef66e7dff9f98764da],
                position_data=self.hashring_data)
        hashring_watch.start()
        time.sleep(1)

        hashring = self.hashring_watch.hashring()
        self.assertEqual(len(hashring), 4)
        self.assertEqual(hashring[0].token, 0x0)
        self.assertEqual(hashring[1].token, 0xcfcd208495d565ef66e7dff9f98764da)
        self.assertEqual(hashring[2].token, 0xdfcd208495d565ef66e7dff9f98764da)
        self.assertEqual(hashring[3].token, 0xf899139df5e1059396431415e770c6dd)

        preference_list = self.hashring_watch.preference_list('0')
        self.assertEqual(len(preference_list), 4)
        self.assertEqual(preference_list[0].token, 0xdfcd208495d565ef66e7dff9f98764da)
        self.assertEqual(preference_list[1].token, 0xf899139df5e1059396431415e770c6dd)
        self.assertEqual(preference_list[2].token, 0x0)
        self.assertEqual(preference_list[3].token, 0xcfcd208495d565ef66e7dff9f98764da)
        hashring_watch.stop()

        time.sleep(1)
        hashring = self.hashring_watch.hashring()
        self.assertEqual(len(hashring), 3)
        self.assertEqual(hashring[0].token, 0x0)
        self.assertEqual(hashring[1].token, 0xcfcd208495d565ef66e7dff9f98764da)
        self.assertEqual(hashring[2].token, 0xf899139df5e1059396431415e770c6dd)

        preference_list = self.hashring_watch.preference_list('0')
        self.assertEqual(preference_list[0].token, 0xf899139df5e1059396431415e770c6dd)
        self.assertEqual(preference_list[1].token, 0x0)
        self.assertEqual(preference_list[2].token, 0xcfcd208495d565ef66e7dff9f98764da)

    def test_session_expiration(self):
        expired_session_hashring = []

        def observer(event):
            if event.state_name == "EXPIRED_SESSION_STATE":
                hashring = self.hashring_watch.hashring()
                expired_session_hashring.append(hashring)
        #add session observer for testing
        self.zookeeper_client.add_session_observer(observer)

        expired_result = expire_zookeeper_client_session(self.zookeeper_client, 10)
        self.assertEqual(expired_result, True)
        time.sleep(1)
        
        self.assertEqual(len(expired_session_hashring[0]), 0)
        hashring = self.hashring_watch.hashring()
        self.assertEqual(len(hashring), 3)
        self.assertEqual(hashring[0].token, 0x0)
        self.assertEqual(hashring[1].token, 0xcfcd208495d565ef66e7dff9f98764da)
        self.assertEqual(hashring[2].token, 0xf899139df5e1059396431415e770c6dd)

        preference_list = self.hashring_watch.preference_list('0')
        self.assertEqual(preference_list[0].token, 0xf899139df5e1059396431415e770c6dd)
        self.assertEqual(preference_list[1].token, 0x0)
        self.assertEqual(preference_list[2].token, 0xcfcd208495d565ef66e7dff9f98764da)
        self.zookeeper_client.remove_session_observer(observer)

if __name__ == "__main__":
    unittest.main()
