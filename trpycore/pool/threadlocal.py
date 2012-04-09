import logging
import threading

from trpycore.pool.base import Pool, PoolContextManager, PoolEmptyException

class ThreadLocalPool(Pool):
    """Thread local pool class.

    This class manages a pool of objects using thread local storage. Each
    thread will have its own pooled object created lazily by the factory.
    """
    
    #Thread local storage
    threadlocal = threading.local()


    def __init__(self, factory):
        """ThreadLocalPool constructor.
        
        Args:
            factory: Instance of Factory or object which provides
                a create method taking no arguments. This factory
                will be used to lazily create thread local
                pooled objects.
        """
        self.factory = factory
        
    def get(self, block=True, timeout=None):
        """
        Returns a PoolContextManager to manage the pooled object
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
        try:
            #If thread local pooled_object does not exist, create it.
            if not getattr(self.threadlocal, "pooled_object", None):
                self.threadlocal.pooled_object = self.factory.create()
            
            instance = self.threadlocal.pooled_object

            if instance is None:
                raise PoolEmptyException
            else:
                return PoolContextManager(self, instance)
        except Exception as error:
            logging.exception(error)
            raise PoolEmptyException

    def put(self, instance):
        """Put a pooled object back in the pool.

        This method will be called by the PoolContextManager
        upon exit.

        Args:
            instance: Pooled object instance to return to pool.
        """
        return
