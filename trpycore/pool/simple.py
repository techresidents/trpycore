from trpycore.pool.base import Pool, PoolContextManager, PoolEmptyException

class SimplePool(Pool):
    """Simple pool class which always returns the same instance object.

    This class gives the allusion of a pool of object, but will always
    return the same instance object. This is useful when a Pool interface
    is required, but the underlying object is actually thread/greenlet
    safe and does not require pooling.
    """

    def __init__(self, instance):
        """SimplePool constructor.
        
        Args:
            instance: Pooled object instance which will be returned
                for each and every get().
        """
        self.instance = instance
    
    def get(self, block=True, timeout=None):
        """
        Returns a PoolContextManager to manager the pooled object
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
            PoolEmptyException if no pooled object is available.
        """
        if self.instance is None:
            raise PoolEmptyException
        else:
            return PoolContextManager(self, self.instance)

    def put(self, instance):
        """Put a pooled object back in the pool.

        This method will be called by the PoolContextManager
        upon exit.

        Args:
            instance: Pooled object instance to return to pool.
        """
        return
