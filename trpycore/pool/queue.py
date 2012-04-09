import logging
import Queue

from trpycore.pool.base import Pool, PoolContextManager, PoolEmptyException

class QueuePool(Pool):
    """Queue pool class.

    This class manages a pool of objects using a queue. Pooled objects
    can be explicitly specified or the provided Factory will be used
    to create the objects.
    """

    def __init__(self, size, factory=None, queue_class=Queue.Queue, pooled_objects=None):
        """QueuePool constructor.
        
        Args:
            size: Number of object to create for pool.
            factory: Instance of Factory or object which provides
                a create method taking no arguments.
            queue_class: Queue class to use. If not provided, will
                default to Queue.Queue. The specified class must
                have a no-arg constructor and provide a get(block, timeout)
                method.
            pooled_objects: Optional list of objects to pool. This
                can be provided as alternative to specifying a
                factory object.
        """
        self.size = size
        self.factory = factory
        self.queue = queue_class()
        self.pooled_objects = pooled_objects or []
        
        #Create the pooled objects using the specified factory if
        #pooled_objects were not provided.
        if not self.pooled_objects:
            for i in range(0, self.size):
                self.pooled_objects.append(self.factory.create())
        
        #Add pooled objects to the queue
        for pooled_object in self.pooled_objects:
            self.queue.put(pooled_object)
    
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
            instance = self.queue.get(block=block, timeout=timeout)

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
        self.queue.put(instance)
