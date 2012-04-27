import unittest

from trpycore.datastruct.trie import Trie

class TestTrie(unittest.TestCase):

    def setUp(self):
        self.trie = Trie()
        for k in ["b", "batter", "bat", "a", "ax", "at", "attic", "ape", "aped"]:
            self.trie.insert(k, k)
    
    def test_contains(self):
        self.assertEquals("a" in self.trie, True)
        self.assertEquals("alpha" in self.trie, False)
    
    def test_clear(self):
        items = self.trie.find()
        self.assertNotEqual(len(items), 0)
        self.trie.clear()
        items = self.trie.find()
        self.assertEqual(len(items), 0)

    def test_insert(self):
        self.assertEqual(self.trie.get("cat"), None)
        self.trie.insert("cat", 100)
        self.assertEqual(self.trie.get("cat"), 100)
    
    def test_get(self):
        self.assertEqual(self.trie.get("cat"), None)
        self.assertEqual(self.trie.get("batter"), "batter")
    
    def test_remove(self):
        self.assertEqual(self.trie.get("ape"), "ape")
        self.trie.remove("ape")
        self.assertEqual(self.trie.get("ape"), None)
        self.assertEqual(self.trie.get("aped"), "aped")
    
    def test_find(self):
        items = self.trie.find("at")
        self.assertEqual(items[0], ("at", "at"))
        self.assertEqual(items[1], ("attic", "attic"))

if __name__ == "__main__":
    unittest.main()
