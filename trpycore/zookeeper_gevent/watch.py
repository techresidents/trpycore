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
    """GDataWatch provides a robust data watcher for a Zookeeper node.
    
    GDataWatch properly handles node creation and deletion, as well
    as zookeeper session expirations.

    watch_observer method, if provided, will be invoked in the context of the 
    the DataWatch greenlet (with this object as the sole parameter)
    when the node's data is modified.

    session_observer method, if provied, will be invoked in the context of the
    GZookeeperClient greenlet (with Zookeeper.Event as the sole parameter)
    when session events occur.
   """
    
    #Event to stop the watcher
    _START_EVENT = object()
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
            if not self._watching:
                return

            if event.state == zookeeper.CONNECTED_STATE:
                #Restart the watcher upon reconnection if we're not running
                if not self._running:
                    self.start()
            elif event.state == zookeeper.EXPIRED_SESSION_STATE:
                self._on_session_expiration()
            
            self._queue.put(event)

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
            self._queue.put(self._START_EVENT)
            gevent.spawn(self._run)
    
    def _watcher(self, event):
        """Internal watcher to be invoked when data/exists changes.
    
        Method will be invoked in the context of GZookeeperClient
        greenlet. It will put an event on the _queue to signal
        the _run method to awaken and update the data
        in the context of the GDataWatch greenlet.
        """
        self._queue.put(event)
    
    def _update(self):
        """Helper method to be invoked upon data change.

        This method will update the object's local data
        with data retrieved from zookeeper.  Upon success,
        all watch observers will be invoked.

        Raises:
            Zookeeper exceptions.
        """
        try:
            self._data, self._stat = self._client.get_data(self._path, self._watcher)
        except zookeeper.NoNodeException:
            stat = self._client.exists(self._path, self._watcher)

            #Double check to make sure the node still does not exist.
            #If it exists, we had bad timing, so try again.
            if stat is not None:
                self._data, self._stat = self._client.get_data(self._path, self._watcher)
            else:
                self._log.warning("watch node '%s' does not exist." % self._path)
                self._log.warning("monitoring node '%s' for creation." % self._path)
                self._data = None
                self._stat = None

        if self._watch_observer:
            self._watch_observer(self)
    
    def _run(self):
        """Method will run in GDataWatch greenlet."""

        self._running = True
        errors = 0

        while self._running:
            try:
                event = self._queue.get()

                if event is self._STOP_EVENT:
                    break
                elif event is self._START_EVENT:
                    if self._client.connected:
                        self._update()
                elif event.state == zookeeper.CONNECTING_STATE:
                    #In the event of a disconnection we do not reset state.
                    #Zookeeper should be running in a cluster, so we're assuming
                    #that even in the event of servers failures or a network
                    #partition we will be able to reconnect to at least
                    #one of the zookeeper servers.
                    pass
                elif event.state == zookeeper.EXPIRED_SESSION_STATE:
                    self._on_session_expiration()
                elif event.state == zookeeper.CONNECTED_STATE:
                    self._update()
                    errors = 0

            except zookeeper.ConnectionLossException:
                continue
            except zookeeper.SessionExpiredException:
                continue
            except zookeeper.ClosingException:
                continue
            except Exception as error:
                errors = errors + 1
                self._log.exception(error)
                if errors > 10:
                    self.log.error("max errors exceeded: exiting watch.")
                    break
        
        self._running = False

    def _on_session_expiration(self):
        """Helper to reset state in the event of zookeeper session expiration"""
        self._data = None
        self._stat = None
    
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
    """Provides a robust watcher for a Zookeeper node's children.

    GChildrenWatch properly handles node creation and deletion, as well
    as zookeeper session expirations. Note that changes to a child's
    data will not be watched.

    watch_observer method, if provided, will be invoked in the context of the 
    the GChildrenWatch greenlet (with this object as the sole parameter)
    when the node's data is modified.

    session_observer method, if provied, will be invoked in the context of the
    GZookeeperClient greenlet (with Zookeeper.Event as the sole parameter)
    when session events occur.
   """

    _START_EVENT = object()
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
            if not self._watching:
                return

            if event.state == zookeeper.CONNECTED_STATE:
                #Restart the watcher upon reconnection if we're not running
                if not self._running:
                    self.start()
            elif event.state == zookeeper.EXPIRED_SESSION_STATE:
                self._on_session_expiration()
            
            self._queue.put(event)

            if self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)
    
    def start(self):
        """Start watching node's children."""
        if not self._running:
            self._log.info("Starting %s(path=%s) ..." % 
                    (self.__class__.__name__, self._path))

            self._watching = True
            self._queue.put(self._START_EVENT)
            gevent.spawn(self._run)

    def _watcher(self, event):
        """Internal node watcher method.
    
        watcher will be invoked in the context of GZookeeperClient
        greenlet. It will put the event on the _queue so that it
        can be handle in _run method and update the children
        accordingly.
        """
        self._queue.put(event)
    
    def _update(self):
        """Helper method to be invoked upon children change.

        This method will update the object's local data
        with data retrieved from zookeeper. Upon success,
        all watch observers will be invoked.

        Raises:
            Zookeeper exceptions.
        """
        try:
            children = self._client.get_children(self._path, self._watcher)
            
            #Add new children
            for child in children:
                if child not in self._children:
                    self._children[child] = self._client.get_data(os.path.join(self._path, child))
            
            #Remove old children
            for child in self._children.keys():
                if child not in children:
                    del self._children[child]

        except zookeeper.NoNodeException:
            stat = self._client.exists(self._path, self._watcher)

            #Double check to make sure the node still does not exist.
            #If it exists, we had bad timing, so try again.
            if stat is not None:
                self._update()
            else:
                self._log.warning("watch node '%s' does not exist." % self._path)
                self._log.warning("monitoring node '%s' for creation." % self._path)
                self._children = {}

        if self._watch_observer:
            self._watch_observer(self)

    def _run(self):
        """Method will run in GChildrenWatch greenlet."""

        self._running = True
        errors = 0

        while self._running:
            try:
                event = self._queue.get()

                if event is self._STOP_EVENT:
                    break
                elif event is self._START_EVENT:
                    if self._client.connected:
                        self._update()
                elif event.state == zookeeper.CONNECTING_STATE:
                    #In the event of a disconnection we do not reset state.
                    #Zookeeper should be running in a cluster, so we're assuming
                    #that even in the event of servers failures or a network
                    #partition we will be able to reconnect to at least
                    #one of the zookeeper servers.
                    pass
                elif event.state == zookeeper.EXPIRED_SESSION_STATE:
                    self._on_session_expiration()
                elif event.state == zookeeper.CONNECTED_STATE:
                    self._update()
                    errors = 0

            except zookeeper.ConnectionLossException:
                continue
            except zookeeper.SessionExpiredException:
                continue
            except zookeeper.ClosingException:
                continue
            except Exception as error:
                errors = errors + 1
                self._log.exception(error)
                if errors > 10:
                    self.log.error("max errors exceeded: exiting watch.")
                    break
        
        self._running = False

    def _on_session_expiration(self):
        """Helper to reset state in the event of zookeeper session expiration."""
        self._children = {}

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


    _START_EVENT = object()
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
        positions = positions or []

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

            if not self._watching:
                return

            if event.state == zookeeper.CONNECTED_STATE:
                #Restart the watcher upon reconnection if we're not running
                if not self._running:
                    self.start()
            elif event.state == zookeeper.EXPIRED_SESSION_STATE:
                self._on_session_expiration()
            
            self._queue.put(event)

            if self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)
    
    def start(self):
        """Start watching hashring node."""
        if not self._running:
            self._log.info("Starting %s(path=%s) ..." % 
                    (self.__class__.__name__, self._path))

            self._watching = True
            self._queue.put(self._START_EVENT)
            self._greenlet = gevent.spawn(self._run)

    def _add_hashring_positions(self):
        """Add positions to hashring (create nodes).

        Ensure hashring positions are present. If positions
        already exist on the hashring, this method will
        not do anything.
        """

        #If positions have already been added return
        if self._occupied_positions != 0:
            return

        try:
            self._client.create_path(self._path)
        except zookeeper.NodeExistsException:
            pass
        
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
            except Exception as error:
                message = "error removing hashring position %s: %s" % (hex_position, str(error))
                self._log.error(message)

    def _watcher(self, event):
        """Internal watcher to be invoked when hashring changes.
    
        Method will be invoked in the context of GZookeeperClient
        greenlet. It will put an event on the _queue to signal
        the _run method to awaken and update the hashring data
        in the context of the GHashringWatch greenlet.
        """
        self._queue.put(event)

    def _update(self):
        """Helper method to update the hashring ring.

        This method attempts to update the object's local hashring
        with data retrieved from zookeeper. Following the update,
        registered watch observers will be invoked.

        Raises:
            Zookeeper exceptions.
        """
        self._add_hashring_positions()
    
        children = self._client.get_children(self._path, self._watcher)
    
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
    
    def _run(self):
        """Method to run in GHashringWatch greenlet."""

        self._running = True
        errors = 0

        while self._running:
            try:
                event = self._queue.get()

                if event is self._STOP_EVENT:
                    break
                elif event is self._START_EVENT:
                    if self._client.connected:
                        self._update()
                elif event.state == zookeeper.CONNECTING_STATE:
                    #In the event of a disconnection we do not reset state.
                    #Zookeeper should be running in a cluster, so we're assuming
                    #that even in the event of servers failures or a network
                    #partition we will be able to reconnect to at least
                    #one of the zookeeper servers.
                    pass
                elif event.state == zookeeper.EXPIRED_SESSION_STATE:
                    self._on_session_expiration()
                elif event.state == zookeeper.CONNECTED_STATE:
                    self._update()

            except zookeeper.ConnectionLossException:
                continue
            except zookeeper.SessionExpiredException:
                continue
            except zookeeper.ClosingException:
                continue
            except Exception as error:
                errors = errors + 1
                self._log.exception(error)
                if errors > 10:
                    self.log.error("max errors exceeded: exiting watch.")
                    break
        
        self._remove_hashring_positions()
        self._running = False

    def _on_session_expiration(self):
        """Helper to reset state in the event of zookeeper session_expiration."""
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
        preference_list = self.preference_list(data)
        if len(preference_list):
            return preference_list[0]
        else:
            raise RuntimeError("no nodes available")
