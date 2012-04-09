import abc
import logging

class CreateException(Exception):
    """Object creation exception."""
    pass

class Factory(object):
    """Base class for simple factory.

    This class can also be used standalone if a factory 
    method is provided.
    """

    __metaclass__ = abc.ABCMeta
    
    def __init__(self, factory_method=None):
        """Factory constructor.

        Args:
            factory_method: optional no-arg factory method
            to be used to create factory objects. If this
            is not provided this class must be subclassed.
        """
        self.factory_method = factory_method

    @abc.abstractmethod
    def create(self):
        """Create a factory object.

        Returns:
            Factory object.
        
        Raises:
            CreateException upon error.
        """

        if self.factory_method:
            try:
                return self.factory_method()
            except Exception as exception:
                logging.exception(exception)
                raise CreateException
        else:
            raise CreateException
