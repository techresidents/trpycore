import bisect
import hashlib
import os
import threading
import uuid

import zookeeper

class DataWatch(object):
    """DataWatch provides a convenient wrapper for monitoring the value of a Zookeeper node.

       watch_observer method, if provided, will be invoked in the context of the 
       underlying zookeeper API thread (with this object as the sole parameter)
       when the node's data is modified.

       session_observer method, if provied, will be invoked in the context of the
       underlying zookeeper API thread (with Zookeeper.Event as teh sole parameter)
       when session events occur.
    """
    def __init__(self, client, path, watch_observer=None, session_observer=None):
        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._lock = threading.Lock()
        self._watching = False
        self._running = False
        self._data = None
        self._stat = None

        def session_observer(event):
            #In the event that the watch was started before the client is connected
            #we will start watching as soon as we do connect. 
            if self._watching and (not self._running) and event.state == zookeeper.CONNECTED_STATE:
                self.start()
            
            if self._watching and self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)
    
    def start(self):
        """Start watching the zookeeper node"""

        self._watching = True

        if not self._client.connected:
            return
        
        self._running = True

        def callback(handle, return_code, data, stat):
            with self._lock:
                self._data = data
                self._stat = stat

            if self._watching and self._watch_observer:
                self._watch_observer(self)

        def watcher(handle, type, state, path):
            if state == zookeeper.CONNECTED_STATE:
                #Only invoke the watcher observer if the event type is CHILD_EVENT and we're wating
                watcher_callback = watcher if (self._watching and type == zookeeper.CHILD_EVENT) else None
                self._client.async_get_children(self._path, watcher_callback, callback)
        
        self._client.async_get_data(self._path, watcher, callback)

    def stop(self):
        """Stop watching the node"""
        self._watching = False
        self._running = False
    
    def get_data(self):
        """Return node data. Since this is a string 
           which is immutable, this does not require a lock
           and copy.
        """
        return self._data

    def get_stat(self):
        """Obtain the lock and return a copy of the data to the user"""
        with self._lock:
            return dict(self._stat)

class ChildrenWatch(object):
    """ChildrenWatch provides a convenient wrapper for monitoring the existence
       of Zookeeper child nodes and their INITIAL data.

       watch_observer method, if provided, will be invoked in the context of the 
       underlying zookeeper API thread (with this object as the sole parameter)
       when the node's children are added or removed.

       session_observer method, if provied, will be invoked in the context of the
       underlying zookeeper API thread (with Zookeeper.Event as teh sole parameter)
       when session events occur.
    """
 
    def __init__(self, client, path, watch_observer=None, session_observer=None):
        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._lock = threading.Lock()
        self._watching = False
        self._running = False
        self._children = {}

        def session_observer(event):
            #In the event that the watch was started before the client is connected
            #we will start watching as soon as we do connect. 
            if self._watching and (not self._running) and event.state == zookeeper.CONNECTED_STATE:
                self.start()

            if self._watching and self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)
    
    def start(self):
        """Start watching the node"""
        self._watching = True

        if not self._client.connected:
            return
        
        self._running = True

        def callback(handle, return_code, children):
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
                self._children.update(new_children)

                for child in self._children.keys():
                    if child not in children:
                        del self._children[child]
            
            #Notify watch observers (without lock)
            if self._watching and self._watch_observer:
                self._watch_observer(self)

        def watcher(handle, type, state, path):
            if state == zookeeper.CONNECTED_STATE:
                #only invoke the watcher_callback if the event type is CHILD_EVENT
                watcher_callback = watcher if (self._watching and type == zookeeper.CHILD_EVENT) else None
                self._client.async_get_children(self._path, watcher_callback, callback)

        self._client.async_get_children(self._path, watcher, callback)
    
    def stop(self):
        """Stop watching the node"""
        self._watching = False
        self._running = False

    def get_children(self):
        """Obtain the lock and return a copy of the children"""
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

    def __init__(self, client, path, num_positions=0, position_data=None,
            watch_observer=None, session_observer=None, hash_function=None):

        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._hash_function = hash_function or hashlib.md5
        self._lock = threading.Lock()
        self._num_positions = num_positions
        self._position_data = position_data

        self._positions = []
        self._watching = False
        self._running = False

        def session_observer(event):
            #In the event that the watch was started before the client is connected
            #we will start watching as soon as we do connect. 
            if self._watching and (not self._running) and event.state == zookeeper.CONNECTED_STATE:
                self.start()

            if self._watching and self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)

    def _add_hashring_positions(self):
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
    
    def start(self):
        """Start watching the hashring node"""
        self._watching = True

        if not self._client.connected:
            return
        
        self._running = True
        self._add_hashring_positions()

        def callback(handle, return_code, children):
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

                #Insert new children
                self._children.update(new_children)
                #Insert new children into the sorted _hashring
                for child in new_children.keys():
                    bisect.insort_left(self._hashring, child)
                
                #Remove stale children
                for child in self._children.keys():
                    if child not in children:
                        del self._children[child]
                        self._hashring.remove(child)
            
            #Notify watch observers (without lock)
            if self._watching and self._watch_observer:
                self._watch_observer(self)

        def watcher(handle, type, state, path):
            if state == zookeeper.CONNECTED_STATE:
                #only invoke the watcher_callback if the event type is CHILD_EVENT
                watcher_callback = watcher if (self._watching and type == zookeeper.CHILD_EVENT) else None
                self._client.async_get_children(self._path, watcher_callback, callback)

        self._client.async_get_children(self._path, watcher, callback)
    
    def stop(self):
        """Stop watching the node"""
        self._watching = False
        self._running = False
    
    def get_children(self):
        """Obtain the lock and return a copy of the children"""
        with self._lock:
            return dict(self._children)
    
    def get_hashring(self):
        """Obtain the lock and return a copy of the hashring list"""
        with self._lock:
            return list(self._hashring)

    def get_hashchild(self, data):
        """Return the selected zookeeper node's data.

           The selected node is determined based on the hash
           of the user passed "data". The first node to the
           right of the data hash  on the hash ring
           will be selected.
        """
        with self._lock:
            data_hash = self._hash_function(data).hexdigest()
            index = bisect.bisect(self._hashring, data_hash)

            #If we're at the end of the hash ring, loop to the start
            if index == len(self._hashring):
                index = 0

            position = self._hashring[index]
            return self._children[position]
