import logging
import Queue
import threading

import zookeeper

#Map zookeeper event types to names
TYPE_NAME_MAP = {
    zookeeper.CHANGED_EVENT: "CHANGED_EVENT",
    zookeeper.CHILD_EVENT: "CHILD_EVENT",
    zookeeper.CREATED_EVENT: "CREATED_EVENT",
    zookeeper.DELETED_EVENT: "DELETED_EVENT",
    zookeeper.NOTWATCHING_EVENT: "NOTWATCHING_EVENT",
    zookeeper.SESSION_EVENT: "SESSION_EVENT",
}

#Map zookeeper states to names
STATE_NAME_MAP = {
    zookeeper.ASSOCIATING_STATE: "ASSOCIATING_STATE",
    zookeeper.AUTH_FAILED_STATE: "AUTH_FAILED_STATE",
    zookeeper.CONNECTED_STATE: "CONNECTED_STATE",
    zookeeper.CONNECTING_STATE: "CONNECTING_STATE",
    zookeeper.EXPIRED_SESSION_STATE: "EXPIRED_SESSION_STATE",
}

#Map zookeeper error codes to exceptions
ERROR_CODE_EXCEPTION_MAP = {
    zookeeper.APIERROR: zookeeper.ApiErrorException,
    zookeeper.AUTHFAILED: zookeeper.AuthFailedException,
    zookeeper.BADARGUMENTS: zookeeper.BadArgumentsException,
    zookeeper.BADVERSION: zookeeper.BadVersionException,
    zookeeper.CLOSING: zookeeper.ClosingException,
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
    """Thread safe client for Apache Zookeeper
       
    ZookeeperClient will run in it's own thread once started.
    
    Session observer callbacks will also be dispatched in the context 
    of the ZookeeperClient thread.

    All other callbacks, (watcher, async callbacks), will be dispatched in the
    context of the underlying zookeeper API threads. Care should be taken
    not to do too much processing in the contexts of these threads.
    """
    
    #Adding _STOP_EVENT to the event queue will cause the Zookeeper client
    #thread to exit.
    _STOP_EVENT = object()

    class Event(object):
        """Represent zookeeper events"""

        def __init__(self, type, state, path):
            """Event constructor.
            Args:
                type: zookeeper api event type
                state: zookeeper api state
                path: zookeeper node path
            """
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
            
            if self.exception is not None:
                raise self.exception
            else:
                return self.result

        def set(self, value=None):
            self.result = value
            self.event.set()
    
        def set_exception(self, exception):
            self.exception = exception
            self.event.set()

    def __init__(self, servers, session_id=None, session_password=None):
        """Zookeeper constructor.
        
        Args:
            servers: list of zookeeper servers, i.e., ["localhost:2181", localdev:2181"]
            session_id: optional zookeeper session id. If not provided, a new
                zookeeper session will be created.
            session_password: optional zookeeper session password, which is
                required if session_id  is not None.
        """
        super(ZookeeperClient, self).__init__()

        self.servers = servers
        self.session_id = session_id or -1
        self.session_password = session_password or ""
        self.acl = [{"perms": 0x1f, "scheme": "world", "id": "anyone"}]
        self.handle = None
        self.session_observers = []
        self.session_timeout_ms = 10000
        self.connected = False
        self.running = False
        self._queue = Queue.Queue()
        self.log = logging.getLogger(__name__)

    def _session_watcher(self, handle, type, state, path):
        """zookeeper session  callback method.
        
        This method will put the session  event in the queue_
        where it will be consumed in the ZookeeperClient thread.
        In this thread the event will be processed and passed
        on to session observers.
    
        Args:
            handle: zookeeper api handle
            type: zookeeper api event type
            state: zookeeper api state
            path: zookeeper api node path
        """
        self._queue.put(self.Event(type, state, path))
    
    def _establish_session(self):
        """Establish a session with zookeeper."""
        servers = ",".join(self.servers)

        self.handle = zookeeper.init(
                servers,
                self._session_watcher,
                self.session_timeout_ms,
                (self.session_id, self.session_password))

    def _watcher_proxy(self, watcher):
        """Proxy to invoke user passed watcher callback.
        
        Proxy wraps type, state, path in ZookeeperClient.Event
        and invokes the user's watcher callback.
        """
        if watcher is None:
            return None

        def watcher_proxy_callback(handle, type, state, path):
            try:
                watcher(self.Event(type, state, path))
            except Exception as error:
                self.log.error("watcher exception from %s" % watcher)
                self.log.exception(error)
        return watcher_proxy_callback

    def error_to_exception(self, return_code, message=None):
        """Convert zookeeper error code to the appropriate exception.

        Args:
            return_code: zookeeper return code.
            message: exception message.
        
        Returns:
            zookeeper exception object which should be raised.
        """

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
        """Starts ZookeeperClient thread if not arleady running."""
        if not self.running:
            self.log.info("Starting ZookeeperClient ...")
            self.running = True
            super(ZookeeperClient, self).start()

    def run(self):
        """ZookeeperClient thread run method."""

        def session_watcher(handle, type, state, path):
            """zookeeper session  callback method.
            
            This method will put the session  event in the queue_
            where it will be consumed in the ZookeeperClient thread.
            In this thread the event will be processed and passed
            on to session observers.

            Args:
                handle: zookeeper api handle
                type: zookeeper api event type
                state: zookeeper api state
                path: zookeeper api node path
            """
            self._queue.put(self.Event(type, state, path))
        
        self.log.info("ZookeeperClient started.")

        self._establish_session()

        while(self.running):
            try:
                event = self._queue.get()

                if event is self._STOP_EVENT:
                    break

                if event.state == zookeeper.CONNECTED_STATE:
                    self.connected = True
                    self.session_id, self.session_password = self.session()
                    self.log.info("Zookeeper connected: (session_id=%x, passwd=%r)" % (
                        self.session_id, self.session_password))
                elif event.state == zookeeper.CONNECTING_STATE:
                    self.connected = False
                elif event.state == zookeeper.EXPIRED_SESSION_STATE:
                    self.log.warning("Zookeeper session (%x) expired." % self.session_id)
                    self.connected = False
                    self.handle = None
                    self.session_id = -1
                    self.session_password = ""
                    
                    self.log.info("Attempting to establish a new session...")
                    self._establish_session()

                for observer in self.session_observers:
                    try:
                        observer(event)
                    except Exception as error:
                        self.log.error("Session observer exception from %s:" % observer)
                        self.log.exception(error)

            except Exception as error:
                self.log.error("Unhandled ZookeeperClient exception:")
                self.log.exception(error)
        
        #ZookeeperClient has been stopped, so close underlying zookeeper connections.
        self.close()

        self.log.info("ZookeeperClient stopped.")


    def stop(self):
        """Stop Zookeeper Client by putting STOP_EVENT in queue.

        Callers wishing to wait for the client to disconnect
        and stop should call join().
        """
        if self.running:
            self.log.info("Stopping ZookeeperClient ...")
            self.running = False
            self._queue.put(self._STOP_EVENT)

    def state(self):
        """Returns zookeeper api state."""
        return zookeeper.state(self.handle)

    def session(self):
        """Returns zookeeper api (session, session_password) tuple."""
        return zookeeper.client_id(self.handle)

    def session_timeout(self):
        """Returns zookeepe session timeout."""
        return zookeeper.recv_timeout(self.handle)

    def close(self):
        """Close underlying zookeeper connections."""
        zookeeper.close(self.handle)
        self.handle = None
        self.session_id = -1
        self.session_password = ""
        self.connected = False

    def add_session_observer(self, observer):
        """Add zookeeper session observer method.
            
        Observer method will be invoked whenever a zookeeper session event occurs.
        The method will be invoked in the context of the ZookeeperClient thread
        with the ZookeeperClient.Event as its sole argument.
        """
        self.session_observers.append(observer)

    def remove_session_observer(self, observer):
        """Remove zookeeper api session observer."""
        self.session_observers.remove(observer)
    
    def create(self, path, data=None, acl=None, sequence=False, ephemeral=False):
        """Blocking call to create Zookeeper node.

        Args:
            path: zookeeper node path, i.e. /my/zookeeper/node/path
            data: optional zookeeper node data (string)
            acl: optional zookeeper access control list (default is insecure)
            sequence: if True node will be created by adding a unique number
                the supplied path.
            ephemeral: if True, node will automatically be deleted when client exists.
        
        Returns:
            path of created node
    
        Raises:
            zookeeper.NodeExistsException if node already exists. 
            zookeeper.*Exception for other failure scenarios.
        """

        data = data or ""
        acl = acl or self.acl
        flags = (zookeeper.SEQUENCE if sequence else 0) | (zookeeper.EPHEMERAL if ephemeral else 0)
        
        return zookeeper.create(self.handle, path, data, acl, flags)

    def create_path(self, path, data=None, acl=None, sequence=False, ephemeral=False):
        """Blocking call to create Zookeeper node (including any subnodes that do not exist).
            
        Args:
            path: zookeeper node path, i.e. /my/zookeeper/node/path
            acl: optional zookeeper access control list (default is insecure)
                to be applied to all created nodes.
            data: Optional data to be set for leaf node.
            sequence: if True leaf node will be created by adding a unique number
                the supplied path.
            ephemeral: if True, leaf node will automatically be deleted when client exists.
        Returns:
            path if node created, or None if it already exists.
        Raises:
            zookeeper.*Exception for other failure scenarios.
        """
        result = None
        
        current_path_list = ['']
        for node in path.split("/")[1:]:
            current_path_list.append(node)
            current_path = "/".join(current_path_list)
            try:
                if current_path == path:
                    result = self.create(current_path, data, acl, sequence, ephemeral)
                else:
                    self.create(current_path, data=None, acl=acl)
            except zookeeper.NodeExistsException:
                if current_path == path:
                    raise
        
        return result


    def exists(self, path, watcher=None):
        """Blocking call to check if zookeeper node  exists.
        
        Args:
            watcher: watcher method to be invoked upon node creation
                or removal with Zookeeper.Event as its sole argument.
                watcher will be invoked in the context of the underlying
                zookeeper API threads.

        Returns:
            zookeeper state dict if node exists, otherwise None.
            i.e.
            {'pzxid': 4522L, 'ctime': 1333654407863L, 'aversion': 0, 'mzxid': 4522L,
            'numChildren': 0, 'ephemeralOwner': 0L, 'version': 0, 'dataLength': 0,
            'mtime': 1333654407863L, 'cversion': 0, 'czxid': 4522L}

        Raises:
            zookeeper.*Exception for other failure scenarios.
        """
        return zookeeper.exists(self.handle, path, self._watcher_proxy(watcher))

    def get_children(self, path, watcher=None):
        """Blocking call to retreive a  zookeeper node's children.

        Args:
            path: zookeeper node path
            watcher: callback method to be invoked upon addition or removal
                of node's children with ZookeeperClient.Event as its sole
                argument.  watcher will be invoked in the context of the underlying
                zookeeper API threads.
        
        Returns:
            list of zookeeper node paths

        Raises:
            zookeeper.NoNodeException if node already exists. 
            zookeeper.*Exception for other failure scenarios.
        """
        return zookeeper.get_children(self.handle, path, self._watcher_proxy(watcher))

    def get_data(self, path, watcher=None):
        """Blocking call to get zookeeper node's data.

        Args:
            path: zookeeper node path
            watcher: callback method to be invoked upon data change.
                watcher will be invoked in the context of the underlying
                zookeeper API threads.
        
        Returns:
            (data, stat) tuple upon success.

        Raises:
            zookeeper.NoNodeException if node already exists. 
            zookeeper.*Exception for other failure scenarios.
        """
        return zookeeper.get(self.handle, path, self._watcher_proxy(watcher))

    def set_data(self, path, data, version=None, return_data=False):
        """Blocking call to set zookeeper node's data.

        Args:
            path: zookeeper node path
            data: zookeeper node data (string)
            version = version if version is not None else -1
            return_data: if True return data

        Raises:
            zookeeper.NoNodeException if node already exists. 
            zookeeper.*Exception for other failure scenarios.
        """
        version = version if version is not None else -1

        if return_data:
            return zookeeper.set2(self.handle, path, data, version)
        else:
            return zookeeper.set(self.handle, path, data, version)

    def delete(self, path, version=None):
        """Blocking call to delete zookeeper node.

        Args:
            path: zookeeper node path
            version: expected node version (optional)

        Raises:
            zookeeper.NoNodeException if node already exists. 
            zookeeper.NotEmptyException if node has children. 
            zookeeper.BadVersionException if version does not match node's version.
            zookeeper.*Exception for other failure scenarios.
        """
        version = version if version is not None else -1
        return zookeeper.delete(self.handle, path, version)

    def async_create(self, path, data=None, acl=None, sequence=False, ephemeral=False, callback=None):
        """Async call to create Zookeeper node.

        Args:
            path: zookeeper node path, i.e. /my/zookeeper/node/path
            data: optional zookeeper node data (string)
            acl: optional zookeeper access control list (default is insecure)
            sequence: if True node will be created by adding a unique number
                the supplied path.
            ephemeral: if True, node will automatically be deleted when client exists.
            callback: callback method to be invoked upon operation completion with
                zookeeper api handle, return_code, and path arguments. callback
                will be invoked in the context of the underlying zookeeper API
                thread.
        
        Returns:
            Zookeeper.AsyncResult if callback is None, otherwise None.
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
        """Async call to check if zookeeper node  exists.
        
        Args:
            path: zookeeper node path
            watcher: watcher method to be invoked upon node creation
                or removal with Zookeeper.Event as its sole argument.
                watcher will be invoked in the context of the underlying
                zookeeper API threads.
            callback: callback method to be invoked upon operation completion with
                zookeeper api handle, return_code, and stat arguments. callback
                will be invoked in the context of the underlying zookeeper API
                thread.

        Returns:
            Zookeeper.AsyncResult if callback is None, otherwise None.
        """
        async_result = None

        if callback is None:
            async_result = self.AsyncResult()

            def async_callback(handle, return_code, stat):
                if return_code == zookeeper.OK:
                    async_result.set(stat)
                elif return_code == zookeeper.NONODE:
                    async_result.set(None)
                else:
                    async_result.set_exception(self.error_to_exception(return_code))

            callback = async_callback

        zookeeper.aexists(self.handle, path, self._watcher_proxy(watcher), callback)

        return async_result

    def async_get_children(self, path, watcher=None, callback=None):
        """Async call to get zookeeper node's children.
        
        Args:
            path: zookeeper node path
            watcher: watcher method to be invoked upon node creation
                or removal with Zookeeper.Event as its sole argument.
                watcher will be invoked in the context of the underlying
                zookeeper API threads.
            callback: callback method to be invoked upon operation completion with
                zookeeper api handle, return_code, and list of children nodes.
                callback will be invoked in the context of the underlying
                zookeeper API thread.

        Returns:
            Zookeeper.AsyncResult if callback is None, otherwise None.
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

        zookeeper.aget_children(self.handle, path, self._watcher_proxy(watcher), callback)

        return async_result

    def async_get_data(self, path, watcher=None, callback=None):
        """Async call to get zookeeper node's data.

        Args:
            path: zookeeper node path
            watcher: watcher method to be invoked upon node creation
                or removal with Zookeeper.Event as its sole argument.
                watcher will be invoked in the context of the underlying
                zookeeper API threads.
            callback: callback method to be invoked upon operation completion with
                zookeeper api handle, return_code, data, and stat.
                callback will be invoked in the context of the underlying
                zookeeper API thread.

        Returns:
            Zookeeper.AsyncResult if callback is None, otherwise None.
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

        zookeeper.aget(self.handle, path, self._watcher_proxy(watcher), callback)

        return async_result

    def async_set_data(self, path, data, version=None, callback=None):
        """Async call to set zookeeper node's data.

        Args:
            path: zookeeper node path
            data: zookeeper node data (string)
            version: expected node version
            callback: callback method to be invoked upon operation completion with
                zookeeper api handle, return_code, data, and stat.
                callback will be invoked in the context of the underlying
                zookeeper API thread.

        Returns:
            Zookeeper.AsyncResult if callback is None, otherwise None.
        """
        async_result = None
        version = version if version is not None else -1

        if callback is None:
            async_result = self.AsyncResult()

            def async_callback(handle, return_code, stat):
                if return_code == zookeeper.OK:
                    async_result.set(stat)
                else:
                    async_result.set_exception(self.error_to_exception(return_code))

            callback = async_callback

        zookeeper.aset(self.handle, path, data, version, callback)

        return async_result

    def async_delete(self, path, version=None, callback=None):
        """Async call to delete zookeeper node's data.

        Args:
            path: zookeeper node path
            version: expected node version
            callback: callback method to be invoked upon operation completion with
                zookeeper api handle, return_code.
                callback will be invoked in the context of the underlying
                zookeeper API thread.

        Returns:
            Zookeeper.AsyncResult if callback is None, otherwise None.
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
