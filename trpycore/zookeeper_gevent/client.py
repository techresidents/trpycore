import fcntl
import os
import Queue

import gevent
import gevent.event
import gevent.queue

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


class ZookeeperClient(object):
    """Gevent Compatible Client for Apache Zookeeper
       
       ZookeeperClient will run in it's own greenlet once started.
       
       Session observer callbacks will be dispatched in the context 
       of the ZookeeperClient greenlet.

       All other callbacks (watcher, async callbacks), will be dispatched in
       the context of newly spawned greenlets.
    """
    
    #STOP_EVENT, when pushed onto event queue, will cause the ZookeeperClient to stop.
    _STOP_EVENT = None

    class Event(object):
        """Event class to represent Zookeeper events."""
        def __init__(self, type, state, path):
            self.type = type
            self.state = state
            self.path = path
            self.type_name = TYPE_NAME_MAP.get(type)
            self.state_name = STATE_NAME_MAP.get(state)
    
    class AsyncResult(gevent.event.AsyncResult):
        """AsyncResult class which supports cross-thread signaling.

           The underlying zookeeper API works through callbacks which
           are invoked in the context of zookeeper API managed threads.
           
           Currently we use AsyncResult to pass data between the zookeeper
           API threads and the gevent main thread. While this works to pass
           data the gevent main thread will not detect changes made to the
           AsyncResult from another thread until it receives another event.
           
           To work around this, gevent should be configured to monitor
           the read end of the pipe passed to AsyncResult's constructor. 
           When data is set on AsyncResult from an outside thread, 
           AsyncResult will also write a single byte to this pipe 
           which will cause gevent to wake up and also detect
           the changes to the AsyncResult and wake up the waiting greenlet.
        """
        def __init__(self, pipe):
            self._pipe = pipe
            super(ZookeeperClient.AsyncResult, self).__init__()
        
        def set(self, value=None):
            super(ZookeeperClient.AsyncResult, self).set(value)
            os.write(self._pipe[1], '\0')
    
        def set_exception(self, exception):
            super(ZookeeperClient.AsyncResult, self).set_exception(exception)
            os.write(self._pipe[1], '\0')


    class AsyncQueue(Queue.Queue, object):
        """AsyncQueue is a threadsafe queue with cross-thread signaling support"""

        def __init__(self, *args, **kwargs):
            super(ZookeeperClient.AsyncQueue, self).__init__(*args, **kwargs)
            
            #event to signal between the readpipe_callback greenlet
            #and the get() invoking greenlet
            self._event = gevent.event.Event()
            
            #non-blocking pipe for cross-thread signaling of _event
            self._pipe = os.pipe()
            fcntl.fcntl(self._pipe[0], fcntl.F_SETFL, os.O_NONBLOCK)
            fcntl.fcntl(self._pipe[1], fcntl.F_SETFL, os.O_NONBLOCK)
            
            def readpipe_callback(event, eventtype):
                """readpipe_callback will be invoked by main greenlet 
                   when data is available on the pipe for reading.
                   Each byte on the pipe represents an item on
                   the queue and will result in _event.set()
                """
                try:
                    os.read(event.fd, 1)
                    self._event.set()
                except Exception:
                    pass
            
            #Invoke readpipe_callback when data is available for
            #reading on _pipe.
            self._pipe_event = gevent.core.event(
                    gevent.core.EV_READ | gevent.core.EV_PERSIST,
                    self._pipe[0],
                    readpipe_callback
                    )
            self._pipe_event.add()
        
        def put(self, item, block=False, timeout=None):
            """Put item on queue and signal cross-thread event"""
            super(ZookeeperClient.AsyncQueue, self).put(item, block, timeout)
            os.write(self._pipe[1], '\0')
        
        def get(self, block=True, timeout=None):
            """Get item off the queue and clear _event"""
            self._event.wait()
            self._event.clear()
            return super(ZookeeperClient.AsyncQueue, self).get(block=False)
       

    def __init__(self, servers):
        self.servers = servers
        self.running = False
        self.connected = False
        self.acl = [{"perms": 0x1f, "scheme": "world", "id": "anyone"}]
        self.pipe = self._nonblocking_pipe()
        self.handle = None
        self.greenlet = None
        self.session_observers = []
        #self._queue = gevent.queue.Queue()
        self._queue = self.AsyncQueue()

       
        def readpipe_callback(event, eventtype):
            """Callback to read AsyncResult cross-thread signaling byte"""
            try:
                os.read(event.fd, 1)
            except Exception:
                pass

        #Setup events to be triggered when data is written
        #to pipe to work around cross-thread AsyncResult issues.
        #See AsyncResult for more details.
        self._event = gevent.core.event(
                gevent.core.EV_READ | gevent.core.EV_PERSIST,
                self.pipe[0],
                readpipe_callback
                )
        self._event.add()
   
    def _nonblocking_pipe(self):
        """Create non-blocking pipe for AsyncResult cross-thread signaling"""
        read, write = os.pipe()
        fcntl.fcntl(read, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(write, fcntl.F_SETFL, os.O_NONBLOCK)
        return (read, write)
    
    def _async_result(self):
        """Create a new AsyncResult attached to the pipe"""
        return self.AsyncResult(self.pipe)
    
    def _spawn_watcher(self, watcher):
        """Spawn a new greenlet for the Zookeeper watcher.

           The newly spawned greenlet will run _watcher_greenlet()
           which is a proxy to the user passed watcher. 

           _watcher_greenlet will wait (block) on the async_result
           until the zookeeper watch is signaled.
            
           Once the zookeeper watch is signaled the below
           method will be invoked by the underlying
           zookeeper API (in its own zookeeper thread) and
           set the result on the async_result. This will awaken
           _watcher_greenlet which will finally invoke the 
           user passed watcher.

        """
        if watcher is None:
            return (None, None)
        
        #Callback to returned and ultimately passed to the zookeeper
        #API to be invoked when the watch is signaled.
        async_result = self._async_result()
        def callback(handle, *args):
            async_result.set(args)
        
        #Spawn new greenlet to wait on AsyncResult for watch.
        greenlet = gevent.spawn(self._watcher_greenlet, async_result, watcher)

        return (callback, greenlet)

    def _watcher_greenlet(self, async_result, watcher):
        """Proxy to invoke user passed watcher callback
           This method will wait on async_result for watch event
           and then invoke the user's watcher method.
        """
        type, state, path =  async_result.get()
        watcher(self.Event(type, state, path))
    
    def error_to_exception(self, return_code, message=None):
        """Convert zookeeper error code to exceptions."""
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
        """Start main ZookeeperClient greenlet"""
        if not self.running:
            self.running = True
            self.greenlet = gevent.spawn(self.run)

    def run(self):
        def session_watcher(handle, type, state, path):
            """sesession_watcher callback will be invoked by the underlying zookeeper
               API (in zookeeper API thread) when session events occur.

               Events will be passed to the ZookeeperClient greenlet through
               the gevent queue. 

               The gevent queue was not thread safe. There is a potential race
               so it's replaced with a AsyncQueue which is thread safe and
               supports cross-thread signaling through pipe events.
            """
            self._queue.put(self.Event(type, state, path))
        
        #Connect to the zookeeper server
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
                    try:
                        observer(event)
                    except Exception as error:
                        print str(error)

            except Exception as error:
                print str(error)
        
        self.close()
    
    def join(self):
        if self.greenlet:
            self.greenlet.join()


    def stop(self):
        """Stop the ZookeeperClient by putting the STOP_EVENT in queue.
           To wait for the client to stop, you should call join().
        """
        if self.running:
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
        """Close underlying zookeeper API connections"""
        zookeeper.close(self.handle)
        self.handle = None
        self.connected = False
    
    def add_session_observer(self, observer):
        self.session_observers.append(observer)
    
    def create(self, path, data=None, acl=None, sequence=False, ephemeral=False):
        data = data or ""
        acl = acl or self.acl
        
        return self.async_create(path, data, acl, sequence, ephemeral).get()

    def create_path(self, path, acl=None, sequence=False, ephemeral=False):
        if self.exists(path):
            return

        data = None
        
        current_path = ['']
        for node in path.split("/")[1:]:
            current_path.append(node)
            try:
                self.async_create("/".join(current_path), data, acl, sequence, ephemeral).get()
            except zookeeper.NodeExistsException:
                pass

    def exists(self, path, watcher=None):
        return self.async_exists(path, watcher).get()

    def get_children(self, path, watcher=None):
        return self.async_get_children(path, watcher).get()

    def get_data(self, path, watcher=None):
        return self.async_get_data(path, watcher).get()

    def set_data(self, path, data):
        return self.async_set_data(path, data).get()

    def delete(self, path, version=None):
        return self.async_delete(path, version).get()


    def async_create(self, path, data=None, acl=None, sequence=False, ephemeral=False):
        data = data or ""
        acl = acl or self.acl
        flags = (zookeeper.SEQUENCE if sequence else 0) | (zookeeper.EPHEMERAL if ephemeral else 0)

        async_result = self._async_result()

        def callback(handle, return_code, path):
            if return_code == zookeeper.OK:
                async_result.set(path)
            else:
                async_result.set_exception(self.error_to_exception(return_code))

        zookeeper.acreate(self.handle, path, data, acl, flags, callback)

        return async_result

    def async_exists(self, path, watcher=None):
        async_result = self._async_result()

        def callback(handle, return_code, stat):
            if return_code == zookeeper.OK:
                async_result.set(stat)
            elif return_code == zookeeper.NONODE:
                return None
            else:
                async_result.set_exception(self.error_to_exception(return_code))

        watcher_callback, greenlet = self._spawn_watcher(watcher)

        zookeeper.aexists(self.handle, path, watcher_callback, callback)

        return async_result

    def async_get_children(self, path, watcher=None):
        async_result = self._async_result()

        def callback(handle, return_code, children):
            if return_code == zookeeper.OK:
                async_result.set(children)
            else:
                async_result.set_exception(self.error_to_exception(return_code))

        watcher_callback, greenlet = self._spawn_watcher(watcher)

        zookeeper.aget_children(self.handle, path, watcher_callback , callback)

        return async_result

    def async_get_data(self, path, watcher=None):
        async_result = self._async_result()

        def callback(handle, return_code, data, stat):
            if return_code == zookeeper.OK:
                async_result.set((data, stat))
            else:
                async_result.set_exception(self.error_to_exception(return_code))

        watcher_callback, greenlet = self._spawn_watcher(watcher)

        zookeeper.aget(self.handle, path, watcher_callback, callback)

        return async_result

    def async_set_data(self, path, data):
        async_result = self._async_result()

        def callback(handle, return_code, stat):
            if return_code == zookeeper.OK:
                async_result.set(stat)
            else:
                async_result.set_exception(self.error_to_exception(return_code))

        zookeeper.aset(self.handle, path, data, callback)

        return async_result

    def async_delete(self, path, version=None):
        version = version if version is not None else -1

        async_result = self._async_result()

        def callback(handle, return_code):
            if return_code == zookeeper.OK:
                async_result.set(None)
            else:
                async_result.set_exception(self.error_to_exception(return_code))

        zookeeper.adelete(self.handle, path, version, callback)

        return async_result


