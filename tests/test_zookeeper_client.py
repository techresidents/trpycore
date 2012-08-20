import logging
import os
import unittest
import time

import zookeeper

from trpycore.zookeeper.client import ZookeeperClient
from trpycore.zookeeper.util import expire_zookeeper_client_session

class TestZookeeperClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)
        
    @classmethod
    def tearDownClass(cls):
        cls.zookeeper_client.stop()
        cls.zookeeper_client.join()

    def test_create(self):
        path = "/unittest_create"
        data = "unittest_data"
        created_path = self.zookeeper_client.create(path, data, ephemeral=True)
        self.assertEqual(created_path, path)
        
        #verify node creation
        rdata, rstat = self.zookeeper_client.get_data(path)
        self.assertEqual(rdata, data)
        
        #verify recreation fails
        with self.assertRaises(zookeeper.NodeExistsException):
            self.zookeeper_client.create(path)

        self.zookeeper_client.delete(path)

    def test_create_path(self):
        path = "/unittest_create_path/path/path"
        data = "unittest_data"
        
        created_path = self.zookeeper_client.create_path(path, data)
        self.assertEqual(created_path, path)
        
        #verify node creation
        rdata, rstat = self.zookeeper_client.get_data(path)
        self.assertEqual(rdata, data)
        
        #verify creation returns None
        self.assertEqual(self.zookeeper_client.create_path(path, data), None)
        
        while path:
            self.zookeeper_client.delete(path)
            path = path.rsplit('/', 1)[0]

    def test_exists(self):
        path = "/unittest_exists"

        stat = self.zookeeper_client.exists(path)
        self.assertEqual(stat, None)

        self.zookeeper_client.create(path, ephemeral=True)
        stat = self.zookeeper_client.exists(path)
        self.assertIsInstance(stat, dict)

        self.zookeeper_client.delete(path)

    def test_get_children(self):
        path = "/unittest_get_children"
        children = ['child1', 'child2']

        with self.assertRaises(zookeeper.NoNodeException):
            self.zookeeper_client.get_children(path)
        
        #create node and children
        self.zookeeper_client.create(path)
        for child in children: 
            child_path = os.path.join(path, child)
            self.zookeeper_client.create(child_path, ephemeral=True)

        #very child creation
        new_children = self.zookeeper_client.get_children(path)
        self.assertEqual(children, new_children)
        
        #delete nodes
        for child in children: 
            child_path = os.path.join(path, child)
            self.zookeeper_client.delete(child_path)
        self.zookeeper_client.delete(path)

    def test_get_data(self):
        path = "/unittest_get_data"
        data = "unitest_data"

        with self.assertRaises(zookeeper.NoNodeException):
            self.zookeeper_client.get_data(path)

        self.zookeeper_client.create(path, data, ephemeral=True)
        rdata, rstat = self.zookeeper_client.get_data(path)

        self.assertEqual(rdata, data)
        self.assertIsInstance(rstat, dict)

        self.zookeeper_client.delete(path)

    def test_set_data(self):
        path = "/unittest_get_data"
        data = "unitest_data"

        with self.assertRaises(zookeeper.NoNodeException):
            self.zookeeper_client.get_data(path)

        self.zookeeper_client.create(path, data, ephemeral=True)

        rdata, rstat = self.zookeeper_client.get_data(path)
        self.assertEqual(rdata, data)
        self.assertIsInstance(rstat, dict)

        new_data = "new_unittest_data"
        self.zookeeper_client.set_data(path, new_data)
        rdata, rstat = self.zookeeper_client.get_data(path)
        self.assertEqual(rdata, new_data)
        self.assertIsInstance(rstat, dict)

        self.zookeeper_client.delete(path)

    def test_delete(self):
        path = "/unittest_delete"

        with self.assertRaises(zookeeper.NoNodeException):
            self.zookeeper_client.async_delete(path).get()

        self.zookeeper_client.create(path, ephemeral=True)
        stat = self.zookeeper_client.exists(path)
        self.assertIsInstance(stat, dict)

        self.zookeeper_client.delete(path)

        stat = self.zookeeper_client.exists(path)
        self.assertEqual(stat, None)


class TestZookeeperClientAsync(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)
        
    @classmethod
    def tearDownClass(cls):
        cls.zookeeper_client.stop()
        cls.zookeeper_client.join()

    def test_async_create(self):
        path = "/unittest_async_create"
        data = "unittest_data"
        created_path = self.zookeeper_client.async_create(path, data, ephemeral=True).get()
        self.assertEqual(created_path, path)
        
        #verify node creation
        rdata, rstat = self.zookeeper_client.get_data(path)
        self.assertEqual(rdata, data)
        
        #verify recreation fails
        with self.assertRaises(zookeeper.NodeExistsException):
            self.zookeeper_client.create(path)

        self.zookeeper_client.delete(path)

    def test_async_exists(self):
        path = "/unittest_async_exists"

        stat = self.zookeeper_client.async_exists(path).get()
        self.assertEqual(stat, None)

        self.zookeeper_client.create(path, ephemeral=True)
        stat = self.zookeeper_client.exists(path)
        self.assertIsInstance(stat, dict)

        self.zookeeper_client.delete(path)

    def test_async_get_children(self):
        path = "/unittest_async_get_children"
        children = ['child1', 'child2']

        with self.assertRaises(zookeeper.NoNodeException):
            self.zookeeper_client.async_get_children(path).get()
        
        #create node and children
        self.zookeeper_client.create(path)
        for child in children: 
            child_path = os.path.join(path, child)
            self.zookeeper_client.create(child_path, ephemeral=True)

        #very child creation
        new_children = self.zookeeper_client.async_get_children(path).get()
        self.assertEqual(children, new_children)
        
        #delete nodes
        for child in children: 
            child_path = os.path.join(path, child)
            self.zookeeper_client.delete(child_path)
        self.zookeeper_client.delete(path)

    def test_async_get_data(self):
        path = "/unittest_async_get_data"
        data = "unitest_data"

        with self.assertRaises(zookeeper.NoNodeException):
            self.zookeeper_client.async_get_data(path).get()

        self.zookeeper_client.create(path, data, ephemeral=True)
        rdata, rstat = self.zookeeper_client.async_get_data(path).get()

        self.assertEqual(rdata, data)
        self.assertIsInstance(rstat, dict)

        self.zookeeper_client.delete(path)

    def test_async_set_data(self):
        path = "/unittest_async_get_data"
        data = "unitest_data"

        with self.assertRaises(zookeeper.NoNodeException):
            self.zookeeper_client.async_get_data(path).get()

        self.zookeeper_client.create(path, data, ephemeral=True)

        rdata, rstat = self.zookeeper_client.get_data(path)
        self.assertEqual(rdata, data)
        self.assertIsInstance(rstat, dict)

        new_data = "new_unittest_data"
        self.zookeeper_client.async_set_data(path, new_data).get()
        rdata, rstat = self.zookeeper_client.get_data(path)
        self.assertEqual(rdata, new_data)
        self.assertIsInstance(rstat, dict)

        self.zookeeper_client.delete(path)

    def test_async_delete(self):
        path = "/unittest_async_delete"

        with self.assertRaises(zookeeper.NoNodeException):
            self.zookeeper_client.async_delete(path).get()

        self.zookeeper_client.create(path, ephemeral=True)
        stat = self.zookeeper_client.exists(path)
        self.assertIsInstance(stat, dict)

        self.zookeeper_client.async_delete(path).get()

        stat = self.zookeeper_client.exists(path)
        self.assertEqual(stat, None)



class TestZookeeperClientWatch(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)
        
    @classmethod
    def tearDownClass(cls):
        cls.zookeeper_client.stop()
        cls.zookeeper_client.join()

    def test_exists_watch(self):
        path = "/unittest_exists_watch"

        watch_events = []
        def watcher(event):
            watch_events.append(event)

        stat = self.zookeeper_client.exists(path, watcher)
        self.assertEqual(stat, None)
        self.assertEqual(len(watch_events), 0)
        
        #create node
        self.zookeeper_client.create(path, ephemeral=True)
        time.sleep(1)
        self.assertEqual(len(watch_events), 1)
        self.assertEqual(watch_events[0].type_name, "CREATED_EVENT")
        self.assertEqual(watch_events[0].path, path)
        
        watch_events = []
        stat = self.zookeeper_client.exists(path, watcher)
        self.assertIsInstance(stat, dict)
        self.assertEqual(len(watch_events), 0)
        
        #delete node
        self.zookeeper_client.delete(path)
        time.sleep(1)
        self.assertEqual(len(watch_events), 1)
        self.assertEqual(watch_events[0].type_name, "DELETED_EVENT")
        self.assertEqual(watch_events[0].path, path)

    def test_get_children_watch(self):
        path = "/unittest_get_children_watch"
        children = ['child1', 'child2']

        watch_events = []
        def watcher(event):
            watch_events.append(event)

        #create node and children
        self.zookeeper_client.create(path)
        self.zookeeper_client.get_children(path, watcher)

        for child in children: 
            child_path = os.path.join(path, child)
            self.zookeeper_client.create(child_path, ephemeral=True)
        time.sleep(1)

        self.assertEqual(len(watch_events), 1)
        for event in watch_events:
            self.assertEqual(event.type_name, "CHILD_EVENT")
            self.assertEqual(event.path, path)
        
        watch_events = []
        new_children = self.zookeeper_client.get_children(path, watcher)
        self.assertEqual(children, new_children)
        
        #delete child nodes
        for child in children: 
            child_path = os.path.join(path, child)
            self.zookeeper_client.delete(child_path)
        time.sleep(1)

        self.assertEqual(len(watch_events), 1)
        for event in watch_events:
            self.assertEqual(event.type_name, "CHILD_EVENT")
            self.assertEqual(event.path, path)
        
        #delete parent node
        watch_events = []
        new_children = self.zookeeper_client.get_children(path, watcher)
        self.assertEqual(len(new_children), 0)
        self.zookeeper_client.delete(path)
        time.sleep(1)
        self.assertEqual(len(watch_events), 1)
        self.assertEqual(watch_events[0].type_name, "DELETED_EVENT")
        self.assertEqual(watch_events[0].path, path)

    def test_get_data_watch(self):
        path = "/unittest_get_data_watch"
        data = "unitest_data"

        watch_events = []
        def watcher(event):
            watch_events.append(event)

        #create node
        self.zookeeper_client.create(path, data, ephemeral=True)
        rdata, rstat = self.zookeeper_client.get_data(path, watcher)
        self.assertEqual(rdata, data)
        self.assertIsInstance(rstat, dict)
        
        #change data
        new_data = "new_unittest_data"
        self.zookeeper_client.set_data(path, new_data)
        time.sleep(1)
        self.assertEqual(len(watch_events), 1)
        self.assertEqual(watch_events[0].type_name, "CHANGED_EVENT")
        self.assertEqual(watch_events[0].path, path)
        
        watch_events = []
        rdata, rstat = self.zookeeper_client.get_data(path, watcher)
        self.assertEqual(rdata, new_data)
        self.assertIsInstance(rstat, dict)

        #delete node
        self.zookeeper_client.delete(path)
        time.sleep(1)
        self.assertEqual(len(watch_events), 1)
        self.assertEqual(watch_events[0].type_name, "DELETED_EVENT")
        self.assertEqual(watch_events[0].path, path)


class TestZookeeperClientWatchAsync(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)
        
    @classmethod
    def tearDownClass(cls):
        cls.zookeeper_client.stop()
        cls.zookeeper_client.join()

    def test_async_exists_watch(self):
        path = "/unittest_async_exists_watch"

        watch_events = []
        def watcher(event):
            watch_events.append(event)

        stat = self.zookeeper_client.async_exists(path, watcher).get()
        self.assertEqual(stat, None)
        self.assertEqual(len(watch_events), 0)
        
        #create node
        self.zookeeper_client.create(path, ephemeral=True)
        time.sleep(1)
        self.assertEqual(len(watch_events), 1)
        self.assertEqual(watch_events[0].type_name, "CREATED_EVENT")
        self.assertEqual(watch_events[0].path, path)
        
        watch_events = []
        stat = self.zookeeper_client.async_exists(path, watcher).get()
        self.assertIsInstance(stat, dict)
        self.assertEqual(len(watch_events), 0)
        
        #delete node
        self.zookeeper_client.delete(path)
        time.sleep(1)
        self.assertEqual(len(watch_events), 1)
        self.assertEqual(watch_events[0].type_name, "DELETED_EVENT")
        self.assertEqual(watch_events[0].path, path)

    def test_async_get_children_watch(self):
        path = "/unittest_async_get_children_watch"
        children = ['child1', 'child2']

        watch_events = []
        def watcher(event):
            watch_events.append(event)

        #create node and children
        self.zookeeper_client.create(path)
        self.zookeeper_client.async_get_children(path, watcher).get()

        for child in children: 
            child_path = os.path.join(path, child)
            self.zookeeper_client.create(child_path, ephemeral=True)
        time.sleep(1)

        self.assertEqual(len(watch_events), 1)
        for event in watch_events:
            self.assertEqual(event.type_name, "CHILD_EVENT")
            self.assertEqual(event.path, path)
        
        watch_events = []
        new_children = self.zookeeper_client.async_get_children(path, watcher).get()
        self.assertEqual(children, new_children)
        
        #delete child nodes
        for child in children: 
            child_path = os.path.join(path, child)
            self.zookeeper_client.delete(child_path)
        time.sleep(1)

        self.assertEqual(len(watch_events), 1)
        for event in watch_events:
            self.assertEqual(event.type_name, "CHILD_EVENT")
            self.assertEqual(event.path, path)
        
        #delete parent node
        watch_events = []
        new_children = self.zookeeper_client.async_get_children(path, watcher).get()
        self.assertEqual(len(new_children), 0)
        self.zookeeper_client.delete(path)
        time.sleep(1)
        self.assertEqual(len(watch_events), 1)
        self.assertEqual(watch_events[0].type_name, "DELETED_EVENT")
        self.assertEqual(watch_events[0].path, path)

    def test_async_get_data_watch(self):
        path = "/unittest_async_get_data_watch"
        data = "unitest_data"

        watch_events = []
        def watcher(event):
            watch_events.append(event)

        #create node
        self.zookeeper_client.create(path, data, ephemeral=True)
        rdata, rstat = self.zookeeper_client.async_get_data(path, watcher).get()
        self.assertEqual(rdata, data)
        self.assertIsInstance(rstat, dict)
        
        #change data
        new_data = "new_unittest_data"
        self.zookeeper_client.set_data(path, new_data)
        time.sleep(1)
        self.assertEqual(len(watch_events), 1)
        self.assertEqual(watch_events[0].type_name, "CHANGED_EVENT")
        self.assertEqual(watch_events[0].path, path)
        
        watch_events = []
        rdata, rstat = self.zookeeper_client.async_get_data(path, watcher).get()
        self.assertEqual(rdata, new_data)
        self.assertIsInstance(rstat, dict)

        #delete node
        self.zookeeper_client.delete(path)
        time.sleep(1)
        self.assertEqual(len(watch_events), 1)
        self.assertEqual(watch_events[0].type_name, "DELETED_EVENT")
        self.assertEqual(watch_events[0].path, path)



class TestZookeeperClientSessionExpiration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)

        cls.zookeeper_client = ZookeeperClient(["localdev:2181"])
        cls.zookeeper_client.start()
        time.sleep(1)
        
    @classmethod
    def tearDownClass(cls):
        cls.zookeeper_client.stop()
        cls.zookeeper_client.join()


    def test_session_expiration(self):
        path = "/unittest_expiration"
        ephemeral_path = "/unittest_expiration_ephemeral"
        data = "unittest_data"
        expired = []

        if self.zookeeper_client.exists(path):
            self.zookeeper_client.delete(path)

        self.zookeeper_client.create(path, data, ephemeral=False)
        self.zookeeper_client.create(ephemeral_path, data, ephemeral=True)
        rdata, rstat = self.zookeeper_client.get_data(path)
        self.assertEqual(rdata, data)

        def observer(event):
            if event.state_name == "EXPIRED_SESSION_STATE":
                expired.append(True)

        #add session observer for testing
        self.zookeeper_client.add_session_observer(observer)
        
        expired_result = expire_zookeeper_client_session(self.zookeeper_client, 10)
        self.assertEqual(expired_result, True)
        time.sleep(1)

        #verify expiration took place
        self.assertEqual(len(expired), 1)

        #verify ephemeral node no longer exists
        with self.assertRaises(zookeeper.NoNodeException):
            self.zookeeper_client.get_data(ephemeral_path)

        #verify non-ephemeral node still exists
        rdata, rstat = self.zookeeper_client.get_data(path)
        self.assertEqual(rdata, data)
        self.zookeeper_client.delete(path)

