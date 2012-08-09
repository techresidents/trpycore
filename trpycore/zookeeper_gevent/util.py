from gevent.event import Event

from trpycore.zookeeper_gevent.client import GZookeeperClient

def expire_zookeeper_client_session(client, timeout=10):
    """Expire zookeeper session for the given client.

    This method should only be used for testing purposed.
    It will induce an EXPIRED_SESSION_STATE event in
    given client.

    Args:
        client: GZookeeperClient object
        timeout: optional timeout in seconds
            to wait for the session to expire.
            If None, this call will block.
            This is not recommended.
    
    Returns:
        True if session exipration occured within
        timeout seconds, False otherwise.
    """
    #session expiration event to wait on
    session_expiration_event = Event()

    def observer(event):
        if event.state_name == "EXPIRED_SESSION_STATE":
            session_expiration_event.set()
    client.add_session_observer(observer)

    #construct new client with same session_id
    #so we can cause a session expiration event
    #in our other client.
    zookeeper_client = GZookeeperClient(
            client.servers,
            client.session_id,
            client.session_password)
    
    def zookeeper_observer(event):
        #Upon connection, immediately stop the client
        #which will cause a session expiration in
        #self.zookeeper_client.
        if event.state_name == "CONNECTED_STATE":
            zookeeper_client.stop()
    
    zookeeper_client.add_session_observer(zookeeper_observer)
    zookeeper_client.start()
    zookeeper_client.join()
    
    session_expiration_event.wait(timeout)

    client.remove_session_observer(observer)
    return session_expiration_event.is_set()
