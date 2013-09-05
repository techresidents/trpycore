import StringIO

from trpycore.chunk.base import Chunker

class BasicChunker(Chunker):
    """Basic chunker class."""

    def __init__(self, obj, size=None):
        """BasicChunker constructor.

        Args:
            obj: object to chunk
            size: maximum number of bytes to read.
        """
        self.obj = obj
        self.size = size
        self.pos = None
        
        #total number of bytes read in last chunks()
        self.last_size = 0

        if hasattr(obj, "tell") and hasattr(obj, "seek"):
            self.pos = obj.tell()

    def chunks(self, chunk_size=4096):
        """Return a chunk generator yielding chunk_size chunks
        
        Args:
            chunk_size: size of chunks to yield
        Returns:
            Chunk size generator
        """
        
        if self.pos is not None:
            self.obj.seek(self.pos)

        generator = self.obj

        if self.obj is None:
            generator = self._empty_generator()
        elif isinstance(self.obj, basestring):
            obj =  StringIO.StringIO(self.obj)
            generator = self._read_generator(obj, chunk_size)
        elif hasattr(self.obj, "chunks"):
            generator = self._chunks_generator(self.obj, chunk_size)
        elif hasattr(self.obj, "read"):
            generator = self._read_generator(self.obj, chunk_size)

        return generator

    def _empty_generator(self):
        if False:
            yield None
    
    def _chunks_generator(self, chunker, chunk_size):
        read = 0
        for chunk in chunker.chunks(chunk_size):
            read += len(chunk)
            yield chunk

        self.last_size = read

    def _read_generator(self, obj, chunk_size):
        read = 0
        read_size = self._read_size(read, chunk_size)

        while read_size:
            chunk = obj.read(read_size)
            read += len(chunk)

            if chunk:
                yield chunk
            else:
                break

            read_size = self._read_size(read, chunk_size)
        
        self.last_size = read

    def _read_size(self, read, chunk_size):
        result = chunk_size
        if self.size:
            result = min(chunk_size, self.size - read)
        return result
