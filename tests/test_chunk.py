import hashlib
import os
import unittest

import testbase
from trpycore.chunk.basic import BasicChunker
from trpycore.chunk.hash import HashChunker

class TestBasicChunker(unittest.TestCase):

    def test_string(self):
        string = "abcdefghijklmnopqrstuvwxyz"
        chunker = BasicChunker(string)
        chunks = [c for c in chunker.chunks(26)]
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], string)
        self.assertEqual(chunker.last_size, len(string))

        chunks = [c for c in chunker.chunks(13)]
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], string[:13])
        self.assertEqual(chunks[1], string[13:])
        self.assertEqual(chunker.last_size, len(string))

        chunks = [c for c in chunker.chunks(3)]
        self.assertEqual(len(chunks), 9)
        self.assertEqual(chunks[0], 'abc')
        self.assertEqual(chunks[8], 'yz')
        self.assertEqual(chunker.last_size, len(string))
    
    def test_file(self):
        #file contains 'abcdefghijklmnopqrstuvwxyz\n'
        path = os.path.join(os.path.dirname(__file__), "data/chunk.txt")
        data = open(path, "r").read()

        f = open(path, "r")
        chunker = BasicChunker(f)
        chunks = [c for c in chunker.chunks(27)]
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], data)
        self.assertEqual(chunker.last_size, len(data))
        
        chunks = [c for c in chunker.chunks(13)]
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0], data[:13])
        self.assertEqual(chunks[1], data[13:26])
        self.assertEqual(chunks[2], data[26:])
        self.assertEqual(chunker.last_size, len(data))

    def test_chunks(self):
        string = "abcdefghijklmnopqrstuvwxyz"
        chunker = BasicChunker(BasicChunker(string))
        chunks = [c for c in chunker.chunks(26)]
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], string)
        self.assertEqual(chunker.last_size, len(string))

        chunks = [c for c in chunker.chunks(13)]
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], string[:13])
        self.assertEqual(chunks[1], string[13:])
        self.assertEqual(chunker.last_size, len(string))

        chunks = [c for c in chunker.chunks(3)]
        self.assertEqual(len(chunks), 9)
        self.assertEqual(chunks[0], 'abc')
        self.assertEqual(chunks[8], 'yz')
        self.assertEqual(chunker.last_size, len(string))

    def test_size(self):
        string = "abcdefghijklmnopqrstuvwxyz"
        chunker = BasicChunker(string, 26)
        chunks = [c for c in chunker.chunks(13)]
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], string[:13])
        self.assertEqual(chunks[1], string[13:])
        self.assertEqual(chunker.last_size, len(string))

        chunker = BasicChunker(string, 13)
        chunks = [c for c in chunker.chunks(13)]
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], string[:13])
        self.assertEqual(chunker.last_size, 13)

        chunker = BasicChunker(string, 13)
        chunks = [c for c in chunker.chunks(2)]
        self.assertEqual(len(chunks), 7)
        self.assertEqual(chunks[0], 'ab')
        self.assertEqual(chunks[6], 'm')
        self.assertEqual(chunker.last_size, 13)

class TestHashChunker(unittest.TestCase):

    def test_string(self):
        string = "abcdefghijklmnopqrstuvwxyz"
        md5 = hashlib.md5(string).hexdigest()
        chunker = HashChunker(string)
        chunks = [c for c in chunker.chunks(26)]
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], string)
        self.assertEqual(md5, chunker.last_hash.hexdigest())
        self.assertEqual(chunker.last_size, len(string))

        chunks = [c for c in chunker.chunks(13)]
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], string[:13])
        self.assertEqual(chunks[1], string[13:])
        self.assertEqual(md5, chunker.last_hash.hexdigest())
        self.assertEqual(chunker.last_size, len(string))

        chunks = [c for c in chunker.chunks(3)]
        self.assertEqual(len(chunks), 9)
        self.assertEqual(chunks[0], 'abc')
        self.assertEqual(chunks[8], 'yz')
        self.assertEqual(md5, chunker.last_hash.hexdigest())
        self.assertEqual(chunker.last_size, len(string))

    def test_file(self):
        #file contains 'abcdefghijklmnopqrstuvwxyz\n'
        path = os.path.join(os.path.dirname(__file__), "data/chunk.txt")
        data = open(path, "r").read()
        md5 = hashlib.md5(data).hexdigest()

        f = open(path, "r")
        chunker = HashChunker(f)
        chunks = [c for c in chunker.chunks(27)]
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], data)
        self.assertEqual(md5, chunker.last_hash.hexdigest())
        self.assertEqual(chunker.last_size, len(data))
        
        chunks = [c for c in chunker.chunks(13)]
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0], data[:13])
        self.assertEqual(chunks[1], data[13:26])
        self.assertEqual(chunks[2], data[26:])
        self.assertEqual(md5, chunker.last_hash.hexdigest())
        self.assertEqual(chunker.last_size, len(data))

    def test_chunks(self):
        string = "abcdefghijklmnopqrstuvwxyz"
        md5 = hashlib.md5(string).hexdigest()
        chunker = HashChunker(BasicChunker(string))
        chunks = [c for c in chunker.chunks(26)]
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], string)
        self.assertEqual(md5, chunker.last_hash.hexdigest())
        self.assertEqual(chunker.last_size, len(string))

        chunks = [c for c in chunker.chunks(13)]
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], string[:13])
        self.assertEqual(chunks[1], string[13:])
        self.assertEqual(md5, chunker.last_hash.hexdigest())
        self.assertEqual(chunker.last_size, len(string))

        chunks = [c for c in chunker.chunks(3)]
        self.assertEqual(len(chunks), 9)
        self.assertEqual(chunks[0], 'abc')
        self.assertEqual(chunks[8], 'yz')
        self.assertEqual(md5, chunker.last_hash.hexdigest())
        self.assertEqual(chunker.last_size, len(string))

    def test_size(self):
        string = "abcdefghijklmnopqrstuvwxyz"
        md5 = hashlib.md5(string[:13]).hexdigest()
        chunker = HashChunker(string, 13)
        chunks = [c for c in chunker.chunks(2)]
        self.assertEqual(len(chunks), 7)
        self.assertEqual(md5, chunker.last_hash.hexdigest())
        self.assertEqual(chunker.last_size, 13)
    
if __name__ == "__main__":
    unittest.main()
