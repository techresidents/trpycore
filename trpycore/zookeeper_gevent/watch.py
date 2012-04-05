import bisect
import hashlib
import os
import uuid

import gevent
import gevent.event
import gevent.queue
import zookeeper

class GDataWatch(object):
    """DataWatch provides a convenient wrapper for monitoring the value of a Zookeeper node.

       watch_observer method, if provided, will be invoked in the context of the 
       the DataWatch greenlet (with this object as the sole parameter)
       when the node's data is modified.

       session_observer method, if provied, will be invoked in the context of the
       ZookeeperClient greenlet (with Zookeeper.Event as the sole parameter)
       when session events occur.
   """
    
    #Event to stop the watcher
    _STOP_EVENT = None

    def __init__(self, client, path, watch_observer=None, session_observer=None):
        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._queue = gevent.queue.Queue()
        self._watching = False
        self._running = False

        self._data = None
        self._stat = None

        def session_observer(event):
            #Restart the watcher upon reconnection if we're watching and not running
            if self._watching and (not self._running) and event.state == zookeeper.CONNECTED_STATE:
                self.start()
            
            if self._session_observer:
                self._session_observer(event)
        
        #Monitor session for disconnections and reconnections to restart watcher
        self._client.add_session_observer(session_observer)
    
    def start(self):
        self._watching = True
        gevent.spawn(self._run)
    
    def _run(self):
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

                self._queue.get()

            except zookeeper.ConnectionLossException:
                break
            except Exception as error:
                break
        
        self._running = False
    
    def stop(self):
        self._watching = False
        self._queue.put(self._STOP_EVENT)
    
    def get_data(self):
        return self._data

    def get_stat(self):
        return self._stat


class GChildrenWatch(object):
    """ChildrenWatch provides a convenient wrapper for monitoring the existence
       of a zookeeper node's children and their INITIAL data.

       watch_observer method, if provided, will be invoked in the context of the 
       the ChildrenWatch greenlet (with this object as the sole parameter)
       when the node's data is modified.

       session_observer method, if provied, will be invoked in the context of the
       ZookeeperClient greenlet (with Zookeeper.Event as the sole parameter)
       when session events occur.
   """

    _STOP_EVENT = None

    def __init__(self, client, path, watch_observer=None, session_observer=None):
        self._client = client
        self._path = path
        self._watch_observer = watch_observer
        self._session_observer = session_observer
        self._queue = gevent.queue.Queue()
        self._watching = False
        self._running = False

        self._children = {}

        def session_observer(event):
            #Restart the watcher upon reconnection if we're watching and not running
            if self._watching and (not self._running) and event.state == zookeeper.CONNECTED_STATE:
                self.start()
            
            if self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)
    
    def start(self):
        self._watching = True
        gevent.spawn(self._run)
    
    def _run(self):
        if not self._client.connected:
            return

        self._running = True

        def watcher(event):
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

            except zookeeper.ConnectionLossException:
                break
            except Exception as error:
                break

        self._running = False

    def stop(self):
        self._watching = False
        self._queue.put(self._STOP_EVENT)
    
    def get_children(self):
        return self._children

class GHashringWatch(object):
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
       the HashRingWatch greenlet (with this object as the sole parameter)
       when positions are added or removed from the hash ring.

       session_observer method, if provied, will be invoked in the context of the
       ZookeeperClient greenlet (with Zookeeper.Event as the sole parameter)
       when session events occur.
   """

    _STOP_EVENT = None

    def __init__(self, client, path, num_positions=0, position_data=None,
            watch_observer=None, session_observer=None, hash_function=None):

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
        
        self._positions = []
        self._hashring = []
        self._children = {}

        def session_observer(event):
            #Restart the watcher upon reconnection if we're watching and not running
            if self._watching and (not self._running) and event.state == zookeeper.CONNECTED_STATE:
                self.start()
            
            if self._session_observer:
                self._session_observer(event)
        
        self._client.add_session_observer(session_observer)
    
    def start(self):
        self._watching = True
        gevent.spawn(self._run)

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
    
    def _run(self):
        if not self._client.connected:
            return
        
        self._running = True
        
        self._add_hashring_positions()

        def watcher(event):
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

                self._queue.get()

            except zookeeper.ConnectionLossException:
                break
            except Exception as error:
                break
        
        self._running = False
    
    def stop(self):
        self._watching = False
        self._queue.put(self._STOP_EVENT)
    

    def get_children(self):
        return self._children

    def get_hashring(self):
        return self._hashring
    
    def get_hashchild(self, data):
        """Return the selected zookeeper node's data.

           The selected node is determined based on the hash
           of the user passed "data". The first node to the
           right of the data hash  on the hash ring
           will be selected.
        """
        data_hash = self._hash_function(data).hexdigest()
        index = bisect.bisect(self._hashring, data_hash)

        #If we're at the end of the hash ring, loop to the start
        if index == len(self._hashring):
            index = 0

        position = self._hashring[index]
        return self._children[position]
