import bisect
import hashlib
import logging
import os
import threading
import uuid
from collections import deque

import zookeeper

class DataWatch(object):
    """DataWatch provides a robust watcher for a Zookeeper node's data.

    GDataWatch properly handles node creation and deletion, as well
    as zookeeper session expirations.

    watch_observer method, if provided, will be invoked in the context of the 
    underlying zookeeper API thread (with this object as the sole parameter)
    when the node's data is modified.

    session_observer method, if provied, will be invoked in the context of the
    underlying zookeeper API thread (with Zookeeper.Event as the sole parameter)
    when session events occur.
    """
    def __init__(self, client, path, watch_observer=None, session_observer=None):
        """DataWatch constructor.

        Args:
            client: zookeeper client instance
            path: zookeeper node path to watch
            watch_observer: optional method to be invoked upon data change
                with this object as the sole parameter. Method will be invoked
                in the context of the underly zookeeper API thread.
            session_observer: optional method to be invoked upon zookeeper
                session event with ZookeeperClient.Event as the sole argument.
                Method will be invoked in the context of the underlying
                zookeeper API thread.
        """
        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._lock = threading.Lock()
        self._watching = False
        self._running = False
        self._data = None
        self._stat = None
        self._log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

        def session_observer(event):
            """Internal zookeeper session observer to handle disconnections."""
            if not self._watching:
                return

            if event.state == zookeeper.CONNECTED_STATE:
                #In the event that the watch was started before the client is connected
                #we will start watching as soon as we do connect. 
                if not self._running:
                    self.start()
            elif event.state == zookeeper.CONNECTING_STATE:
                #In the event of a disconnection we do not reset state.
                #Zookeeper should be running in a cluster, so we're assuming
                #that even in the event of servers failures or a network
                #partition we will be able to reconnect to at least
                #one of the zookeeper servers.
                pass
            elif event.state == zookeeper.EXPIRED_SESSION_STATE:
                self._on_session_expiration()
            
            #notify ession observer
            if self._watching and self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)

    def _callback(self, handle, return_code, data, stat):
        """Zookeeper async_get_data callback.

        This method will be invoked in the context of the
        Zookeeper API thread upon completion of the
        async get data request.

        Args:
            handle: zookeeper handle
            return_code: zookeeper return code
            data: zookeeper node data
            stat: zookeeper node stat dict
        """
        try:
            if return_code != zookeeper.OK:
                raise self._client.error_to_exception(return_code)
            with self._lock:
                self._data = data
                self._stat = stat
        except zookeeper.NoNodeException:
            #Setup an exists watch so we'll be notified when this
            #node is created. Note that for now this is a blocking
            #operation to simplify the logic. This is excuting
            #in a borrowed thread, but this should not happen
            #often and should be a quick operation.
            stat = self._client.exists(self._path, self._watcher)

            #Double check if node exists. If it now exists, it was bad timing,
            #so schedule another async_get_data.
            if stat is not None:
                self._client.async_get_data(self._path, self._watcher, self._callback)
            else:
                self._log.warning("watch node '%s' does not exist." % self._path)
                self._log.warning("monitoring node '%s' for creation." % self._path)
                with self._lock:
                    self._data = None
                    self._stat = None
        except zookeeper.ClosingException:
            pass
        except Exception as error:
            if self._watching:
                self._log.exception(error)
            else:
                self._log.warning(str(error))
    
        if self._watching and self._watch_observer:
            self._watch_observer(self)
    
    def _watcher(self, event):
        """Internal watcher to handle changes to data.
    
        This method will invoke an async_get_data to get the updated
        data. The result will be received in callback() which
        will update our internal data.
    
        Args:
            event: ZookeeperClient.Event object
        """
        if event.state == zookeeper.CONNECTED_STATE:
            #Only invoke the watcher observer if watching
            watcher_callback = self._watcher if self._watching else None
            self._client.async_get_data(self._path, watcher_callback, self._callback)
    
    def start(self):
        """Start watching the zookeeper node."""


        self._watching = True

        if not self._client.connected:
            return

        self._log.info("Starting %s(path=%s) ..." % 
                (self.__class__.__name__, self._path))
        
        self._running = True

        self._client.async_get_data(self._path, self._watcher, self._callback)

    def stop(self):
        """Stop watching the node."""
        self._log.info("Stopping %s(path=%s) ..." % 
                (self.__class__.__name__, self._path))

        self._watching = False
        self._running = False

    def join(self, timeout=None):
        """Join watch."""
        return

    def _on_session_expiration(self):
        """Helper to reset state in the event of zookeeper session expiration."""
        #Set running to false, so that the watch will be
        #reestablished upon reconnection.
        self._running = False
        with self._lock:
            self._data = None
            self._stat = None
    
    def get_data(self):
        """Get zookeeper node data.
        
        Since this is a string which is immutable, this does not
        require a lock and copy.

        Returns:
            zookeeper node data (string)
        """
        return self._data

    def get_stat(self):
        """Obtain the lock and return a copy of the stat data.
        
        Returns:
            zookeeper node stat data.
            i.e.
            {'pzxid': 4522L, 'ctime': 1333654407863L, 'aversion': 0, 'mzxid': 4522L,
            'numChildren': 0, 'ephemeralOwner': 0L, 'version': 0, 'dataLength': 0,
            'mtime': 1333654407863L, 'cversion': 0, 'czxid': 4522L}
        """
        with self._lock:
            return dict(self._stat)

class ChildrenWatch(object):
    """Provides a robust watcher for a Zookeeper node's children.

    ChildrenWatch properly handles node creation and deletion, as well
    as zookeeper session expirations. Note that changes to a child's
    data will not be watched.

    watch_observer method, if provided, will be invoked in the context of the 
    underlying zookeeper API thread (with this object as the sole parameter)
    when the node's children are added or removed.

    session_observer method, if provied, will be invoked in the context of the
    underlying zookeeper API thread (with ZookeeperClient.Event as teh sole parameter)
    when session events occur.
    """
 
    def __init__(self, client, path, watch_observer=None, session_observer=None):
        """ChildrenWatch constructor.

        Args:
            client: zookeeper client instance
            path: zookeeper node path to watch
            watch_observer: optional method to be invoked upon child change
                with this object as the sole parameter. Method will be invoked
                in the context of the underly zookeeper API thread.
            session_observer: optional method to be invoked upon zookeeper
                session event with ZookeeperClient.Event as the sole argument.
                Method will be invoked in the context of the underlying
                zookeeper API thread.
        """
        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._lock = threading.Lock()
        self._watching = False
        self._running = False
        self._children = {}
        self._log = logging.getLogger("%s.%s" % (__name__, self.__class__.__name__))

        def session_observer(event):
            """Internal zookeeper session observer to handle disconnections."""
            if event.state == zookeeper.CONNECTED_STATE:
                #In the event that the watch was started before the client is connected
                #we will start watching as soon as we do connect. 
                if self._watching and (not self._running):
                    self.start()
            elif event.state == zookeeper.CONNECTING_STATE:
                #In the event of a disconnection we do not reset state.
                #Zookeeper should be running in a cluster, so we're assuming
                #that even in the event of servers failures or a network
                #partition we will be able to reconnect to at least
                #one of the zookeeper servers.
                pass
            elif event.state == zookeeper.EXPIRED_SESSION_STATE:
                self._on_session_expiration()
            
            #notify ession observer
            if self._watching and self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)

    def _callback(self, handle, return_code, children):
        """Zookeeper async_get_data callback.

        This method will be invoked in the context of the
        Zookeeper API thread upon completion of the
        async get data request.

        Args:
            handle: zookeeper handle
            return_code: zookeeper return code
            children: list of children node paths
        """
        try:
            if return_code != zookeeper.OK:
                raise self._client.error_to_exception(return_code)

            #Get the initial node data for new children (without lock)
            #Reading of _children without the lock should be safe since
            #we're the only writer. This is assuming that the underlying
            #zookeeper api is only dispatching events in a single thread.
            #This should be the case since zookeeper goes through great
            #lengths to ensure strict ordering.
            new_children = {}
            for child in children:
                if child not in self._children:
                    new_children[child] = self._client.get_data(os.path.join(self._path, child))
            
            #Acquire lock and update with new children and remove old children
            with self._lock:
                self._children.update(new_children)
    
                for child in self._children.keys():
                    if child not in children:
                        del self._children[child]
            
        except zookeeper.NoNodeException:
            #Setup an exists watch so we'll be notified when this
            #node is created. Note that for now this is a blocking
            #operation to simplify the logic. This is excuting
            #in a borrowed thread, but this should not happen
            #often and should be a quick operation.
            stat = self._client.exists(self._path, self._watcher)

            #Double check if node exists. If it now exists, it was bad timing,
            #so schedule another async_get_data.
            if stat is not None:
                self._client.async_get_children(self._path, self._watcher, self._callback)
            else:
                self._log.warning("watch node '%s' does not exist." % self._path)
                self._log.warning("monitoring node '%s' for creation." % self._path)
                with self._lock:
                    self._children = {}
        except zookeeper.ClosingException:
            pass
        except Exception as error:
            if self._watching:
                self._log.exception(error)
            else:
                self._log.warning(str(error))

        #Notify watch observers (without lock)
        if self._watching and self._watch_observer:
            self._watch_observer(self)
        
    
    def _watcher(self, event):
        """Internal watcher to handle changes to children / node creation.
    
        This method will invoke an async_get_children to get the updated
        data. The result will be received in callback() which
        will update our internal data.
    
        Args:
            event: ZookeeperClient.Event object
        """
        if event.state == zookeeper.CONNECTED_STATE:
            #only invoke the watcher_callback if watching
            watcher_callback = self._watcher if self._watching else None
            self._client.async_get_children(self._path, watcher_callback, self._callback)
    
    def start(self):
        """Start watching the node."""
        self._watching = True

        if not self._client.connected:
            return
        
        self._log.info("Starting %s(path=%s) ..." % 
                (self.__class__.__name__, self._path))

        self._running = True

        self._client.async_get_children(self._path, self._watcher, self._callback)
    
    def stop(self):
        """Stop watching the node."""
        self._log.info("Stopping %s(path=%s) ..." % 
                (self.__class__.__name__, self._path))

        self._watching = False
        self._running = False

    def join(self, timeout=None):
        """Join watch."""
        return

    def _on_session_expiration(self):
        """Helper to reset state in the event of zookeeper session expiration."""
        #Set running to false, so that the watch will be
        #reestablished upon reconnection.
        self._running = False
        with self._lock:
            self._children = {}

    def get_children(self):
        """Obtain the lock and return a copy of the children.
        
        Returns:
            dict with the node name as the key and its data as its value.
        """
        with self._lock:
            return dict(self._children)


class HashringWatch(object):
    """HashRingWatch provides a convenient wrapper for using a zookeeper
       node to represent a consistent hash ring.

    The zookeeper node's children will represent positions on a consistent
    hash ring. The data associated with each node will be application
    specific, but should contain the necessary data to identify the
    selected (service, machine, url) associated with the hash ring
    position. 
    
    The associated data MUST be static, since the children data will
    not be monitored for updates.

    watch_observer method, if provided, will be invoked in the context of the 
    the zookeeper api thread (with this object as the sole parameter)
    when positions are added or removed from the hash ring.

    session_observer method, if provied, will be invoked in the context of the
    zookeeper api thread (with Zookeeper.Event as the sole parameter)
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


    def __init__(self, client, path, positions=None, position_data=None,
            watch_observer=None, session_observer=None, hash_function=None):
        """HashringWatch constructor.

        Args:
            client: zookeeper client instance
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
            watch_observer: optional method to be invoked upon hashring change.
                Method will be invoked in the context of the underlying
                zookeeper API thread with the following parameters:
                    watch: HashringWatch object,
                    previous_hashring: hashring prior to changes
                    current_hashring: hashring after changes
                    added_nodes: list of added HashRingNode's
                    removed_nodes: list of removed HashRingNode's 
            session_observer: optional method to be invoked upon zookeeper
                session event with Zookeeper.Event as the sole argument.
                Method will be invoked in the context of the underlying
                zookeeper API thread.
        """
        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._hash_function = hash_function or hashlib.md5
        self._lock = threading.Lock()
        self._positions = []
        self._position_data = position_data
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
            if event.state == zookeeper.CONNECTED_STATE:
                #In the event that the watch was started before the client is connected
                #we will start watching as soon as we do connect. 
                if self._watching and (not self._running):
                    self.start()
            elif event.state == zookeeper.CONNECTING_STATE:
                #In the event of a disconnection we do not reset state.
                #Zookeeper should be running in a cluster, so we're assuming
                #that even in the event of servers failures or a network
                #partition we will be able to reconnect to at least
                #one of the zookeeper servers.
                pass
            elif event.state == zookeeper.EXPIRED_SESSION_STATE:
                self._on_session_expiration()
            
            #notify ession observer
            if self._watching and self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)

    def _add_hashring_positions(self):
        """Add positions to hashring (create nodes)."""

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
            except zookeeper.NoNodeException as error:
                self._log.exception(error)

    def _callback(self, handle, return_code, children):
        """Internal callback for async_get_children.
    
        This is invoked when positions are added or removed
        from the hashring.
        
        Args:
            handle: zookeeper api handle
            return_code: zookeeper return code
            children: list of zookeeper children node names
        """
        if return_code != zookeeper.OK:
            return
        
        #Get the initial node data for new children (without lock)
        #Reading of _children without the lock should be safe since
        #we're the only writer. This is assuming that the underlying
        #zookeeper api is only dispatching events in a single thread.
        #This should be the case since zookeeper goes through great
        #lengths to ensure strict ordering.
        new_children = {}
        for child in children:
            if child not in self._children:
                new_children[child] = self._client.get_data(os.path.join(self._path, child))
        
        #Acquire lock and update with new children and remove old children
        with self._lock:
            
            #hashring prior to changes
            previous_hashring = list(self._hashring)
    
            #Insert new children
            added_nodes = []
            self._children.update(new_children)
            #Insert new children into the sorted _hashring
            for child, (data, stat) in new_children.items():
                position = long(child, 16)
                node = self.HashringNode(position, data, stat)
                added_nodes.append(node)
                bisect.insort_left(self._hashring, node)
            
            #Remove stale children
            removed_nodes = []
            for child, (data, stat) in self._children.items():
                if child not in children:
                    position = long(child, 16)
                    del self._children[child]
                    node = self._hashring[self._hashring.index(self.HashringNode(position))]
                    removed_nodes.append(node)
                    self._hashring.remove(node)
            
            #hashring following changes
            current_hashring = list(self._hashring)
        
        #Notify watch observers (without lock)
        if self._watching and self._watch_observer:
            self._watch_observer(
                    self,
                    previous_hashring=previous_hashring,
                    current_hashring=current_hashring,
                    added_nodes=added_nodes,
                    removed_nodes=removed_nodes)
    
    def _watcher(self, event):
        """Internal watcher to handle changes to hashring.
    
        This method will invoke an async_get_children to get the updated
        hashring positions. The result will be received in callback() which
        will update our internal data.
    
        Args:
            event: ZookeeperClient.Event object
        """
        if event.state == zookeeper.CONNECTED_STATE:
            #only invoke the watcher_callback if watching
            watcher_callback = self._watcher if self._watching else None
            self._client.async_get_children(self._path, watcher_callback, self._callback)
    
    def start(self):
        """Start watching the hashring node."""
        self._watching = True

        if not self._client.connected:
            return
        
        self._log.info("Starting %s(path=%s) ..." % 
                (self.__class__.__name__, self._path))

        self._running = True
        self._add_hashring_positions()

        self._client.async_get_children(self._path, self._watcher, self._callback)
    
    def stop(self):
        """Stop watching the node."""
        self._log.info("Stopping %s(path=%s) ..." % 
                (self.__class__.__name__, self._path))
        
        self._remove_hashring_positions()
        self._watching = False
        self._running = False
    
    def join(self, timeout=None):
        """Join watch."""
        return

    def _on_session_expiration(self):
        """Helper to reset state in the event of zookeeper session expiration."""
        #Set running to false, so that the watch will be
        #reestablished upon reconnection.
        self._running = False
        with self._lock:
            self._occupied_positions = 0
            self._hashring = []
            self._children = {}
    
    def children(self):
        """Obtain the lock and return a copy of the node children.
        
        The children node names represent positions on the hashring.

        Returns:
            dict with the node name as the key and its data as its value.
        """
        with self._lock:
            return dict(self._children)
    
    def hashring(self):
        """Return hashring as ordered list of HashringNode's.
        
        Hashring is represented as an ordered list of HashringNode's.
        The list is ordered by hashring position (HashringNode.token).

        Returns:
            Ordered list of HashringNode's.
        """
        with self._lock:
            return list(self._hashring)

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
        if hashring is None:
            with self._lock:
                hashring = list(self._hashring)

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
