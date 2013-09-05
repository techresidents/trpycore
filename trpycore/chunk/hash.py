from trpycore.chunk.basic import BasicChunker

import hashlib

class HashChunker(BasicChunker):
    """Hash chunker class.

    Extends basic chunker to keep a running hash of chunked
    data in self.last_hash. This is useful for computing
    and verifying checksums of chunked data.
    """

    def __init__(self, obj, size=None, hash_class=hashlib.md5):
        """HashChunker constructor.

        Args:
            obj: object to chunk
            size: maximum number of bytes to read.
        """
        super(HashChunker, self).__init__(obj, size)
        self.hash_class = hash_class
        self.last_hash = None


    def chunks(self, chunk_size=4096):
        """Return a chunk generator yielding chunk_size chunks
        
        Args:
            chunk_size: size of chunks to yield
        Returns:
            Chunk size generator
        """
        self.last_hash = self.hash_class()

        generator = super(HashChunker, self).chunks(chunk_size)
        for chunk in generator:
            self.last_hash.update(chunk)
            yield chunk
