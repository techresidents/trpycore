import bisect
import hashlib
import logging
import os
import uuid
from collections import deque

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
                session event with GZookeeperClient.Event as the sole argument.
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
            self._log.info("Starting %s(path=%s) ..." % 
                    (self.__class__.__name__, self._path))
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
            self._log.info("Stopping %s(path=%s) ..." % 
                    (self.__class__.__name__, self._path))

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
                session event with GZookeeperClient.Event as the sole argument.
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
            self._log.info("Starting %s(path=%s) ..." % 
                    (self.__class__.__name__, self._path))

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
            self._log.info("Stopping %s(path=%s) ..." % 
                    (self.__class__.__name__, self._path))

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

    class HashringNode(object):
        """Hashring node class."""

        def __init__(self, token, data=None, stat=None):
            """HashringNode constructor.

            Args:
                token: 128-bit integer token identifying the node's
                    position on the hashring.
                data: Optional string data associated with the node.
                stat: Zookeeper stat dict.
            """
            self.token = token
            self.data = data
            self.stat = stat

        def __cmp__(self, other):
            if self.token < other.token:
                return -1
            elif self.token > other.token:
                return 1
            else:
                return 0
        
        def __hash__(self):
            return self.token.__hash__()

        def __repr__(self):
            return "%s(%032x)" % (self.__class__.__name__, self.token)


    _STOP_EVENT = object()

    def __init__(self, client, path, positions=None, position_data=None,
            watch_observer=None, session_observer=None, hash_function=None):
        """GHashringWatch constructor.

        Args:
            client: GZookeeperClient instance
            path: zookeeper node path to watch
            positions: optional list of positions to occupy on the
                hashring (nodes to create). Each position
                must be a 128-bit integer in integer or hex string format.
                If None, a randomly generated position will be used.
                Note that in the case of a position collision, a randomly
                generated position will also be used.
            position_data: data to associate with the occupied positions (nodes)
            watch_observer: optional method to be invoked upon hashring change.
                Method will be invoked in the context of the GHashringWatch greenlet
                with the following paramaters:
                    watch: GHashringWatch object,
                    previous_hashring: hashring prior to changes
                    current_hashring: hashring after changes
                    added_nodes: list of added HashRingNode's
                    removed_nodes: list of removed HashRingNode's 
            session_observer: optional method to be invoked upon zookeeper
                session event with GZookeeperClient.Event as the sole argument.
                Method will be invoked in the context of the GZookeeperClient
                greenlet.
        """

        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._hash_function = hash_function or hashlib.md5
        self._queue = gevent.queue.Queue()
        self._positions =[]
        self._position_data = position_data
        self._greenlet = None
        self._watching = False
        self._running = False
        self._log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))
        
        self._occupied_positions = 0
        self._hashring = []
        self._children = {}
        self._num_positions = len(positions)

        #Remove None values from positions, since
        #this indicates that a randomly chosen
        #position should be used. This is no longer
        #needed, since we've already calculated
        #the number of positions needed.
        for position in positions:
            if position is not None:
                if isinstance(position, basestring):
                    self._positions.append(long(position, 16))
                else:
                    self._positions.append(long(position))

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
            self._log.info("Starting %s(path=%s) ..." % 
                    (self.__class__.__name__, self._path))

            self._watching = True
            self._greenlet = gevent.spawn(self._run)

    def _add_hashring_positions(self):
        """Add positions to hashring (create nodes)."""

        #If positions have already been added return
        if self._occupied_positions != 0:
            return
        
        #Allocate a position queue with requested hashring
        #positions. These position values will be 
        #tried first, before backing off to randomly
        #generated positions.
        position_queue = deque(self._positions)
        self._positions = []

        #Add our postions to the hashring
        for i in range(0, self._num_positions):
            while True:
                try:
                    if position_queue:
                        position = position_queue.popleft()
                    else:
                        position = uuid.uuid4().int

                    self._client.create_path(self._path)
                    data = self._position_data or position
                    hex_position = "%032x" % position
                    self._client.create(os.path.join(self._path, hex_position), data, ephemeral=True)
                    self._positions.append(position)
                    break
                except zookeeper.NodeExistsException:
                    #Potential collision.
                    #Check to see if this is our data. This can happen
                    #in the event of a brief disconnection.
                    #If this is the case, continue as usual.
                    #Otherwise, this is an actual collision,
                    #so try again.
                    node_data, stat = self._client.get_data(os.path.join(self._path, hex_position))
                    if node_data == data:
                        #False alarm (brief disconnection)
                        self._positions.append(position)
                        break

    def _remove_hashring_positions(self):
        """Remove positions from hashring (delete nodes)."""

        for position in self._positions:
            try:
                hex_position = "%032x" % position
                self._client.delete(os.path.join(self._path, hex_position))
            except zookeeper.NoNodeException as error:
                self._log.exception(error)
    
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
        
        #Keep track of consecutive errors
        #If total gets too high we will break out.
        errors = 0

        while self._running:
            try:
                children = self._client.get_children(self._path, watcher)

                previous_hashring = list(self._hashring)

                #Add new children
                added_nodes = []
                for child in children:
                    if child not in self._children:
                        data, stat = self._client.get_data(os.path.join(self._path, child))
                        self._children[child] = (data, stat)

                        #Insort hash ring node into the sorted _hashring
                        position = long(child, 16)
                        node = self.HashringNode(position, data, stat)
                        added_nodes.append(node)
                        bisect.insort(self._hashring, node)
                        print self._hashring
                
                #Remove old children
                removed_nodes = []
                for child in self._children.keys():
                    if child not in children:
                        position = long(child, 16)
                        del self._children[child]
                        node = self._hashring[self._hashring.index(self.HashringNode(position))]
                        removed_nodes.append(node)
                        self._hashring.remove(node)

                if self._watch_observer:
                    self._watch_observer(
                            self,
                            previous_hashring=previous_hashring,
                            current_hashring=list(self._hashring),
                            added_nodes=added_nodes,
                            removed_nodes=removed_nodes)

                event = self._queue.get()
                if event is self._STOP_EVENT:
                    break

                errors = 0

            except zookeeper.ConnectionLossException:
                self._on_disconnected()
                break
            except Exception as error:
                errors = errors + 1
                self._log.exception(error)
                if errors > 10:
                    break
        
        self._remove_hashring_positions()
        self._running = False
    
    def _on_disconnected(self):
        self._occupied_positions = 0
        self._hashring = []
        self._children = {}
    
    def stop(self):
        """Stop watching the hashring."""
        if self._running:
            self._log.info("Stopping %s(path=%s) ..." % 
                    (self.__class__.__name__, self._path))

            self._watching = False
            self._queue.put(self._STOP_EVENT)

    def join(self, timeout):
        """Join the hashring."""
        if self._greenlet:
            self._greenlet.join(timeout)

    def children(self):
        """Return hashring node's children.
        
        The children node names represent positions on the hashring.

        Returns:
            dict with the node name as the key and its (data, stat) as its value.
        """
        return self._children

    def hashring(self):
        """Return hashring as ordered list of HashringNode's.
        
        Hashring is represented as an ordered list of HashringNode's.
        The list is ordered by hashring position (HashringNode.token).

        Returns:
            Ordered list of HashringNode's.
        """
        return self._hashring

    def preference_list(self, data, hashring=None):
        """Return a preference list of HashringNode's for the given data.
        
        Generates an ordered list of HashringNode's responsible for
        the data. The list is ordered by node preference, where the
        first node in the list is the most preferred node to process
        the data. Upon failure, lower preference nodes in the list
        should be tried.
        
        Args:
            data: string to hash to find appropriate hashring position.
            hashring: Optional list of HashringNode's for which
                to calculate the preference list. If None, the current
                hashring will be used.
        Returns:
            Preference ordered list of HashringNode's responsible
            for the given data.
        """
        hashring = hashring or self._hashring
        data_hash = self._hash_function(data).hexdigest()
        data_token = long(data_hash, 16)
        index = bisect.bisect(hashring, self.HashringNode(data_token))

        #If we're at the end of the hash ring, loop to the start
        if index == len(hashring):
            index = 0

        result = hashring[index:]
        result.extend(hashring[0:index])
        return result

    def find_hashring_node(self, data):
        """Find the hashring node responsible for the given data.

        The selected hashring node is determined based on the hash
        of the user passed "data". The first node to the
        right of the data hash on the hash ring
        will be selected.
        
        Args:
            data: string to hash to find appropriate hashring position.
        Returns:
            HashringNode responsible for the given data.
        Raises:
            RuntimeError if no nodes are available.
        """
        preference_list = self.preference_list()
        if len(preference_list):
            return self.preference_list[0]
        else:
            raise RuntimeError("no nodes available")
