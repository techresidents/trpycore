import bisect
import hashlib
import logging
import os
import uuid

import gevent
import gevent.event
import gevent.queue
import zookeeper

class GDataWatch(object):
    """GDataWatch provides a convenient wrapper for monitoring the value of a Zookeeper node.

    watch_observer method, if provided, will be invoked in the context of the 
    the DataWatch greenlet (with this object as the sole parameter)
    when the node's data is modified.

    session_observer method, if provied, will be invoked in the context of the
    GZookeeperClient greenlet (with Zookeeper.Event as the sole parameter)
    when session events occur.
   """
    
    #Event to stop the watcher
    _STOP_EVENT = object()

    def __init__(self, client, path, watch_observer=None, session_observer=None):
        """DataWatch constructor.

        Args:
            client: GZooKeeperClient instance
            path: zookeeper node path to watch
            watch_observer: optional method to be invoked upon data change
                with this object as the sole parameter. Method will be invoked
                in the context of GDataWatch's greenlet.
            session_observer: optional method to be invoked upon zookeeper
                session event with Zookeeper.Event as the sole argument.
                Method will be invoked in the context of the 
                GZookeeperClient greenlet.
        """
        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._queue = gevent.queue.Queue()
        self._watching = False
        self._running = False
        self._log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

        self._data = None
        self._stat = None

        def session_observer(event):
            """Internal session observer to handle disconnections."""
            #Restart the watcher upon reconnection if we're watching and not running
            if self._watching and (not self._running) and event.state == zookeeper.CONNECTED_STATE:
                self.start()
            
            if self._session_observer:
                self._session_observer(event)
        
        #Monitor session for disconnections and reconnections to restart watcher
        self._client.add_session_observer(session_observer)
    
    def start(self):
        """Start watching node."""
        if not self._running:
            self._watching = True
            gevent.spawn(self._run)
    
    def _run(self):
        """Method will run in GDataWatch greenlet."""
        if not self._client.connected:
            return
        
        self._running = True

        def watcher(event):
            self._queue.put(event)

        while self._watching:
            try:
                self._data, self._stat = self._client.get_data(self._path, watcher)
                if self._observer:
                    self._observer(self._data, self._stat)
                
                if self._watch_observer:
                    self._watch_observer(self)

                event = self._queue.get()
                if event is self._STOP_EVENT:
                    break

            except zookeeper.ConnectionLossException:
                break
            except Exception as error:
                self._log.exception(error)
        
        self._running = False
    
    def stop(self):
        """Stop watching node."""
        if self._running:
            self._watching = False
            self._queue.put(self._STOP_EVENT)
    
    def get_data(self):
        """Get zookeeper node data.

        Returns:
            zookeeper node data (string)
        """
        return self._data

    def get_stat(self):
        """Return zookeeper node stat data.
        
        Returns:
            zookeeper node stat data.
            i.e.
            {'pzxid': 4522L, 'ctime': 1333654407863L, 'aversion': 0, 'mzxid': 4522L,
            'numChildren': 0, 'ephemeralOwner': 0L, 'version': 0, 'dataLength': 0,
            'mtime': 1333654407863L, 'cversion': 0, 'czxid': 4522L}
        """
        return self._stat


class GChildrenWatch(object):
    """GChildrenWatch provides a convenient wrapper for monitoring the existence
       of a zookeeper node's children and their INITIAL data.

    watch_observer method, if provided, will be invoked in the context of the 
    the GChildrenWatch greenlet (with this object as the sole parameter)
    when the node's data is modified.

    session_observer method, if provied, will be invoked in the context of the
    GZookeeperClient greenlet (with Zookeeper.Event as the sole parameter)
    when session events occur.
   """

    _STOP_EVENT = object()

    def __init__(self, client, path, watch_observer=None, session_observer=None):
        """GChildrenWatch constructor.

        Args:
            client: GZookeeperClient instance
            path: zookeeper node path to watch
            watch_observer: optional method to be invoked upon child change
                with this object as the sole parameter. Method will be invoked
                in the context of the GChildrenWatch greenlet.
            session_observer: optional method to be invoked upon zookeeper
                session event with Zookeeper.Event as the sole argument.
                Method will be invoked in the context of the GZookeeperClient
                greenlet.
        """
        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._queue = gevent.queue.Queue()
        self._watching = False
        self._running = False
        self._log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

        self._children = {}

        def session_observer(event):
            """Internal zookeeper session observer to handle disconnections."""
            #Restart the watcher upon reconnection if we're watching and not running
            if self._watching and (not self._running) and event.state == zookeeper.CONNECTED_STATE:
                self.start()
            
            if self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)
    
    def start(self):
        """Start watching node's children."""
        if not self._running:
            self._watching = True
            gevent.spawn(self._run)
    
    def _run(self):
        """Method to be run in GChildrenWatch greenlet."""
        if not self._client.connected:
            return

        self._running = True

        def watcher(event):
            """Internal node watcher method.

            watcher will be invoked in the context of GZookeeperClient
            greenlet. It will put the event on the _queue so that it
            can be handle in _run method and update the children
            accordingly.
            """
            self._queue.put(event)

        while self._watching:
            try:
                children = self._client.get_children(self._path, watcher)
                
                #Add new children
                for child in children:
                    if child not in self._children:
                        self._children[child] = self._client.get_data(os.path.join(self._path, child))
                
                #Remove old children
                for child in self._children.keys():
                    if child not in children:
                        del self._children[child]

                if self._watch_observer:
                    self._watch_observer(self)
                
                event = self._queue.get()
                if event is self._STOP_EVENT:
                    break

            except zookeeper.ConnectionLossException:
                break
            except Exception as error:
                self._log.exception(error)

        self._running = False

    def stop(self):
        """Stop watching children."""
        if self._running:
            self._watching = False
            self._queue.put(self._STOP_EVENT)
    
    def get_children(self):
        """Return a node's children.
        
        Returns:
            dict with the node name as the key and its data as its value.
        """
        return self._children

class GHashringWatch(object):
    """GHashringWatch provides a convenient wrapper for using a zookeeper
       node to represent a consistent hash ring.

    The zookeeper node's children will represent positions on a consistent
    hash ring. The data associated with each node will be application
    specific, but should contain the necessary data to identify the
    selected (service, machine, url) associated with the hash ring
    position. 
    
    The associated data MUST be static, since the children data will
    not be monitored for updates.

    watch_observer method, if provided, will be invoked in the context of the 
    the GHashringWatch greenlet (with this object as the sole parameter)
    when positions are added or removed from the hash ring.

    session_observer method, if provied, will be invoked in the context of the
    GZookeeperClient greenlet (with Zookeeper.Event as the sole parameter)
    when session events occur.
    """

    _STOP_EVENT = object()

    def __init__(self, client, path, num_positions=0, position_data=None,
            watch_observer=None, session_observer=None, hash_function=None):
        """GHashringWatch constructor.

        Args:
            client: GZookeeperClient instance
            path: zookeeper node path to watch
            num_positions: optional number of positions to occupy on
                the hashring (nodes to create).
            position_data: data to associate with the occupied positions (nodes)
            watch_observer: optional method to be invoked upon hashring change
                with this object as the sole parameter. Method will be invoked
                in the context of the GHashringWatch greenlet.
            session_observer: optional method to be invoked upon zookeeper
                session event with Zookeeper.Event as the sole argument.
                Method will be invoked in the context of the GZookeeperClient
                greenlet.
        """

        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._hash_function = hash_function or hashlib.md5
        self._queue = gevent.queue.Queue()
        self._num_positions = num_positions
        self._position_data = position_data
        self._watching = False
        self._running = False
        self._log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))
        
        self._positions = []
        self._hashring = []
        self._children = {}

        def session_observer(event):
            """Internal zookeeper session observer to handle disconnections."""
            #Restart the watcher upon reconnection if we're watching and not running
            if self._watching and (not self._running) and event.state == zookeeper.CONNECTED_STATE:
                self.start()
            
            if self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)
    
    def start(self):
        """Start watching hashring node."""
        if not self._running:
            self._watching = True
            gevent.spawn(self._run)

    def _add_hashring_positions(self):
        """Add positions to hashring (create nodes)."""

        #If positions have already been added return
        if self._positions:
            return
        
        #Add our postions to the hashring
        for i in range(0, self._num_positions):
            while True:
                try:
                    position = self._hash_function(uuid.uuid4().hex).hexdigest()
                    self._client.create_path(self._path)
                    data = self._position_data or position
                    self._client.create(os.path.join(self._path, position), data, ephemeral=True)
                    self._positions.append(position)
                    break
                except zookeeper.NodeExistsException:
                    #Position collision, keep trying.
                    pass
    
    def _run(self):
        """Method to run in GHashringWatch greenlet."""
        if not self._client.connected:
            return
        
        self._running = True
        
        self._add_hashring_positions()

        def watcher(event):
            """Internal watcher to be invoked when hashring changes.

            Method will be invoked in the context of GZookeeperClient
            greenlet. It will put an event on the _queue to signal
            the _run method to awaken and update the hashring data
            in the context of the GHashringWatch greenlet.
            """
            self._queue.put(event)

        while self._running:
            try:
                children = self._client.get_children(self._path, watcher)

                #Add new children
                for child in children:
                    if child not in self._children:
                        self._children[child] = self._client.get_data(os.path.join(self._path, child))

                        #Insort child into the sorted _hashring
                        bisect.insort_left(self._hashring, child)
                
                #Remove old children
                for child in self._children.keys():
                    if child not in children:
                        del self._children[child]
                        self._hashring.remove(child)

                if self._watch_observer:
                    self._watch_observer(self)

                event = self._queue.get()
                if event is self._STOP_EVENT:
                    break

            except zookeeper.ConnectionLossException:
                break
            except Exception as error:
                self._log.exception(error)
        
        self._running = False
    
    def stop(self):
        """Stop watching the hashring."""
        if self._running:
            self._watching = False
            self._queue.put(self._STOP_EVENT)
    

    def get_children(self):
        """Return hashring node's children.
        
        The children node names represent positions on the hashring.

        Returns:
            dict with the node name as the key and its data as its value.
        """
        return self._children

    def get_hashring(self):
        return self._hashring
    
    def get_hashchild(self, data):
        """Return the selected hashring positions's node data.

        The selected node is determined based on the hash
        of the user passed "data". The first node to the
        right of the data hash on the hash ring
        will be selected.
        
        Args:
            data: string to hash to find appropriate hashring position.
        Returns:
            data (string) associated with the selected child.
        """
        data_hash = self._hash_function(data).hexdigest()
        index = bisect.bisect(self._hashring, data_hash)

        #If we're at the end of the hash ring, loop to the start
        if index == len(self._hashring):
            index = 0

        position = self._hashring[index]
        return self._children[position]
