import abc

class Chunker(object):
    """Chunker base class."""

    __metaclass__ = abc.ABCMeta
    
    def __init__(self, obj, size=None):
        """Chunker constructor.

        Args:
            obj: object to chunk
            size: maximum number of bytes to read.
        """
        self.obj = obj

    @abc.abstractmethod
    def chunks(self, chunk_size=4096):
        """Return a chunk generator yielding chunk_size chunks
        
        Args:
            chunk_size: size of chunks to yield
        Returns:
            Chunk size generator
        """
        return
