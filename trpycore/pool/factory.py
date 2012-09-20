import logging

from trpycore.pool.base import Pool, PoolContextManager, PoolEmptyException

class FactoryPool(Pool):
    """Factory pool class which always returns a new instance object.

    This class gives the illusion of a pool of object, but will always
    return a new instance object from the factory. This is useful when
    a Pool interface is required, but the underlying object is inexpensive
    to create and is not actually worth pooling.
    """

    def __init__(self, factory):
        """SimplePool constructor.
        
        Args:
            factory: Instance of Factory or object which provides
                a create method taking no arguments.
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
                a Pool object to become available before
                raising PoolEmptyException.
        Returns:
            PoolContextManager instance wrapping the pooled object.

        Raises:
            PoolEmptyException if no pooled object is available.
        """
        try:
            instance = self.factory.create()
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
