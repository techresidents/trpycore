#/usr/bin/env python

import Queue
import threading

import zookeeper

TYPE_NAME_MAP = {
    zookeeper.CHANGED_EVENT: "CHANGED_EVENT",
    zookeeper.CHILD_EVENT: "CHILD_EVENT",
    zookeeper.CREATED_EVENT: "CREATED_EVENT",
    zookeeper.DELETED_EVENT: "DELETED_EVENT",
    zookeeper.NOTWATCHING_EVENT: "NOTWATCHING_EVENT",
    zookeeper.SESSION_EVENT: "SESSION_EVENT",
}

STATE_NAME_MAP = {
    zookeeper.ASSOCIATING_STATE: "ASSOCIATING_STATE",
    zookeeper.AUTH_FAILED_STATE: "AUTH_FAILED_STATE",
    zookeeper.CONNECTED_STATE: "CONNECTED_STATE",
    zookeeper.CONNECTING_STATE: "CONNECTING_STATE",
    zookeeper.EXPIRED_SESSION_STATE: "EXPIRED_SESSION_STATE",
}

ERROR_CODE_EXCEPTION_MAP = {
    zookeeper.APIERROR: zookeeper.ApiErrorException,
    zookeeper.AUTHFAILED: zookeeper.AuthFailedException,
    zookeeper.BADARGUMENTS: zookeeper.BadArgumentsException,
    zookeeper.BADVERSION: zookeeper.BadVersionException,
    zookeeper.CONNECTIONLOSS: zookeeper.ConnectionLossException,
    zookeeper.DATAINCONSISTENCY: zookeeper.DataInconsistencyException,
    zookeeper.INVALIDACL: zookeeper.InvalidACLException,
    zookeeper.INVALIDCALLBACK: zookeeper.InvalidCallbackException,
    zookeeper.MARSHALLINGERROR: zookeeper.MarshallingErrorException,
    zookeeper.NOAUTH: zookeeper.NoAuthException,
    zookeeper.NOCHILDRENFOREPHEMERALS: zookeeper.NoChildrenForEphemeralsException,
    zookeeper.NODEEXISTS: zookeeper.NodeExistsException,
    zookeeper.NONODE: zookeeper.NoNodeException,
    zookeeper.NOTEMPTY: zookeeper.NotEmptyException,
    zookeeper.OPERATIONTIMEOUT: zookeeper.OperationTimeoutException,
    zookeeper.RUNTIMEINCONSISTENCY: zookeeper.RuntimeInconsistencyException,
    zookeeper.SESSIONEXPIRED: zookeeper.SessionExpiredException,
    zookeeper.SESSIONMOVED: zookeeper.SESSIONMOVED,
    zookeeper.SYSTEMERROR: zookeeper.SystemErrorException,
    zookeeper.UNIMPLEMENTED: zookeeper.UnimplementedException,
}

class ZookeeperClient(threading.Thread):
    """Client for Apache Zookeeper
       
       ZookeeperClient will run in it's own thread once started.
       
       Session observer callbacks will also be dispatched in the context 
       of the ZookeeperClient thread.

       All other callbacks, (watcher, async callbacks), will be dispatched in the
       context of the underlying zookeeper API threads. Care should be taken
       not to do too much processing in the contexts of these threads.
    """
    
    #Adding _STOP_EVENT to the event queue will cause the Zookeeper client
    #thread to exit.
    _STOP_EVENT = None

    class Event(object):
        """Represent zookeeper events"""

        def __init__(self, type, state, path):
            self.type = type
            self.state = state
            self.path = path
            self.type_name = TYPE_NAME_MAP.get(type)
            self.state_name = STATE_NAME_MAP.get(state)
    
    class AsyncResult(object):
        """AsyncResult is returned by async methods if a callback is not passsed.
           This provides a convenient way for the invoker to receive the result
           in a non-busy-waiting manner.
        """

        class TimeoutException(Exception):
            pass

        def __init__(self):
            self.event = threading.Event()
            self.result = None
            self.exception = None
        
        def ready(self):
            return self.event.is_set()
        
        def get(self, block=True, timeout=None):
            if not self.ready() and block:
                self.event.wait(timeout)
            
            if not self.ready():
                raise self.TimeoutException("Timeout: result not ready")
            
            if self.result:
                return self.result
            else:
                raise self.exception

        def set(self, value=None):
            self.result = value
            self.event.set()
    
        def set_exception(self, exception):
            self.exception = exception
            self.event.set()

    def __init__(self, servers):
        super(ZookeeperClient, self).__init__()

        self.servers = servers
        self.acl = [{"perms": 0x1f, "scheme": "world", "id": "anyone"}]
        self.handle = None
        self.session_observers = []
        self.connected = False
        self.running = False
        self._queue = Queue.Queue()

    def error_to_exception(self, return_code, message=None):
        """Convert zookeeper error code to the appropriate exception"""

        try:
            error_message = zookeeper.zerror(return_code)
        except:
            error_message = "Unknown error code - %s" % return_code
        
        if message:
            message = "%s: %s" % (message, error_message)
        else:
            message = error_message

        exception_class = ERROR_CODE_EXCEPTION_MAP.get(return_code) or Exception
        
        return exception_class(message)

    def start(self):
        self.running = True
        super(ZookeeperClient, self).start()

    def run(self):
        def session_watcher(handle, type, state, path):
            """session_watcher callback will put the event in the queue_
               where it will be consumed in the ZookeeperClient thread.
               In this thread the event will be processed and passed
               on to session observers.
            """
            self._queue.put(self.Event(type, state, path))
        
        #start up underlying zookeeper client
        self.handle = zookeeper.init(",".join(self.servers), session_watcher, 10000)

        while(self.running):
            try:
                event = self._queue.get()

                if event == self._STOP_EVENT:
                    break

                if event.state == zookeeper.CONNECTED_STATE:
                    self.connected = True
                elif event.state in [zookeeper.CONNECTED_STATE, zookeeper.EXPIRED_SESSION_STATE]:
                    self.connected = False

                for observer in self.session_observers:
                    observer(event)

            except Exception as error:
                print str(error)
        
        #ZookeeperClient has been stopped, so close underlying zookeeper connections.
        self.close()


    def stop(self):
        """Stop Zookeeper Client by putting STOP_EVENT in queue.
           Callers wishing to wait for the client to disconnect
           and stop should call join().
        """
        self.running = False
        self._queue.put(self._STOP_EVENT)

    def state(self):
        return zookeeper.state(self.handle)

    def session(self):
        """Returns (session, session_password) tuple"""
        return zookeeper.client_id(self.handle)

    def session_timeout(self):
        return zookeeper.recv_timeout(self.handle)

    def close(self):
        """Close underlying zookeeper connections"""
        zookeeper.close(self.handle)
        self.handle = None
        self.connected = False

    def add_session_observer(self, observer):
        self.session_observers.append(observer)
    
    def create(self, path, data=None, acl=None, sequence=False, ephemeral=False):
        """Blocking call to create Zookeeper nodes"""

        data = data or ""
        acl = acl or self.acl
        flags = (zookeeper.SEQUENCE if sequence else 0) | (zookeeper.EPHEMERAL if ephemeral else 0)
        
        return zookeeper.create(self.handle, path, data, acl, flags)

    def create_path(self, path, acl=None, sequence=False, ephemeral=False):
        """Blocking call to create Zookeeper node (including any subnodes that do not exist)"""
        data = None
        
        current_path = ['']
        for node in path.split("/")[1:]:
            current_path.append(node)
            try:
                self.create("/".join(current_path), data, acl, sequence, ephemeral)
            except zookeeper.NodeExistsException:
                pass

    def exists(self, path, watcher=None):
        """Blocking call to exists"""
        return zookeeper.exists(self.handle, path, watcher)

    def get_children(self, path, watcher=None):
        """Blocking call to get_children"""
        return zookeeper.get_children(self.handle, path, watcher)

    def get_data(self, path, watcher=None):
        """Blocking call to get data"""
        return zookeeper.get(self.handle, path, watcher)

    def set_data(self, path, data, return_data=False):
        """Blocking call to set data"""
        if return_data:
            return zookeeper.set2(self.handle, path, data)
        else:
            return zookeeper.set(self.handle, path, data)

    def delete(self, path, version=None):
        """Blocking call to delete data"""
        version = version if version is not None else -1
        return zookeeper.delete(self.handle, path, version)

    def async_create(self, path, data=None, acl=None, sequence=False, ephemeral=False, callback=None):
        """Async call to create Zookeeper node.
           If callback is None, an AsyncResult object will be returned.
           Otherwise, callback will be invoked in the context of the
           underlying zookeeper API thread.
        """

        data = data or ""
        acl = acl or self.acl
        flags = (zookeeper.SEQUENCE if sequence else 0) | (zookeeper.EPHEMERAL if ephemeral else 0)
        
        async_result = None

        if callback is None:
            async_result = self.AsyncResult()

            def async_callback(handle, return_code, path):
                if return_code == zookeeper.OK:
                    async_result.set(path)
                else:
                    async_result.set_exception(self.error_to_exception(return_code))

            callback = async_callback

        zookeeper.acreate(self.handle, path, data, acl, flags, callback)

        return async_result

    def async_exists(self, path, watcher=None, callback=None):
        """Async call to exists.
           If callback is None, an AsyncResult object will be returned.
           Otherwise, callback will be invoked in the context of the
           underlying zookeeper API thread.
        """
        async_result = None

        if callback is None:
            async_result = self.AsyncResult()

            def async_callback(handle, return_code, stat):
                if return_code == zookeeper.OK:
                    async_result.set(stat)
                else:
                    async_result.set_exception(self.error_to_exception(return_code))

            callback = async_callback

        zookeeper.aexists(self.handle, path, watcher, callback)

        return async_result

    def async_get_children(self, path, watcher=None, callback=None):
        """Async call to get_children.
           If callback is None, an AsyncResult object will be returned.
           Otherwise, callback will be invoked in the context of the
           underlying zookeeper API thread.
        """
        async_result = None

        if callback is None:
            async_result = self.AsyncResult()

            def async_callback(handle, return_code, children):
                if return_code == zookeeper.OK:
                    async_result.set(children)
                else:
                    async_result.set_exception(self.error_to_exception(return_code))

            callback = async_callback

        zookeeper.aget_children(self.handle, path, watcher, callback)

        return async_result

    def async_get_data(self, path, watcher=None, callback=None):
        """Async call to get_data.
           If callback is None, an AsyncResult object will be returned.
           Otherwise, callback will be invoked in the context of the
           underlying zookeeper API thread.
        """
        async_result = None

        if callback is None:
            async_result = self.AsyncResult()

            def async_callback(handle, return_code, data, stat):
                if return_code == zookeeper.OK:
                    async_result.set((data, stat))
                else:
                    async_result.set_exception(self.error_to_exception(return_code))

            callback = async_callback

        zookeeper.aget(self.handle, path, watcher, callback)

        return async_result

    def async_set_data(self, path, data, callback=None):
        """Async call to set_data.
           If callback is None, an AsyncResult object will be returned.
           Otherwise, callback will be invoked in the context of the
           underlying zookeeper API thread.
        """
        async_result = None

        if callback is None:
            async_result = self.AsyncResult()

            def async_callback(handle, return_code, stat):
                if return_code == zookeeper.OK:
                    async_result.set(stat)
                else:
                    async_result.set_exception(self.error_to_exception(return_code))

            callback = async_callback

        zookeeper.aset(self.handle, path, data, callback)

        return async_result

    def async_delete(self, path, version=None, callback=None):
        """Async call to delete.
           If callback is None, an AsyncResult object will be returned.
           Otherwise, callback will be invoked in the context of the
           underlying zookeeper API thread.
        """
        version = version if version is not None else -1
        async_result = None

        if callback is None:
            async_result = self.AsyncResult()

            def async_callback(handle, return_code):
                if return_code == zookeeper.OK:
                    async_result.set(None)
                else:
                    async_result.set_exception(self.error_to_exception(return_code))

            callback = async_callback

        zookeeper.adelete(self.handle, path, version, callback)

        return async_result


