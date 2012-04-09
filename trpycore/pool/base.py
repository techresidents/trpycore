import abc

class PoolEmptyException(Exception):
    pass

class PoolContextManager(object):
    """PoolContextManager stand alone or base class.

    A simple context manager for use with Pool that can
    be extended or used as is. As is, it simply wraps
    and instance object in a context manager.
    """

    def __init__(self, pool, instance):
        """PoolContextManager constructor.

        Args:
            pool: Pool object.
            instance: Pooled object instance to be returned
                by the context manager.
        """
        self.pool = pool
        self.instance = instance
    
    def get(self):
        """Returns the pool object."""
        return self.instance

    def __enter__(self):
        """Context manager entry point.

        Returns:
            Pooled object instance.
        """
        return self.get()

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """Exit context manager without suppressing exceptions."""
        self.pool.put(self.instance)
        return False


class Pool(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def get(block=True, timeout=None):
        """Get a pooled object instance.
        
        Returns a PoolContextManager to manager the pool object
        resource and should be used as follows:

        with pool.get() as pooled_object:
            pooled_object.send()

        Args:
            block: If true, method will block until a pooled
                object becomes available.
            timeout: Number of seconds to block (if True) for
                a pooled object to become available before
                raising PoolEmptyException.
        Returns:
            PoolContextManager instance wrapping the pooled object.

        Raises:
            PoolEmptyException if a pooled object is not available.
        """
        return

    @abc.abstractmethod
    def put(self, instance):
        """Put a pooled object back in the pool.

        This method will be called by the PoolContextManager
        upon exit.

        Args:
            instance: Pooled object instance to return to pool.
        """
        return
