import unittest

from trpycore.datastruct.trie import Trie, MultiTrie

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

class TestMultiTrie(unittest.TestCase):

    def setUp(self):
        self.trie = MultiTrie()
        for k in ["b", "batter", "bat", "a", "ax", "at", "attic", "ape", "aped"]:
            self.trie.insert(k, k)
        self.trie.insert("multi", 0)
        self.trie.insert("multi", 1)
        self.trie.insert("multi", 2)
    
    def test_contains(self):
        self.assertEquals("a" in self.trie, True)
        self.assertEquals("multi" in self.trie, True)
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
        self.assertEqual(self.trie.get("cat")[0], 100)
        self.trie.insert("cat", 200)
        self.assertEqual(self.trie.get("cat")[0], 100)
        self.assertEqual(self.trie.get("cat")[1], 200)
    
    def test_get(self):
        self.assertEqual(self.trie.get("cat"), None)
        self.assertEqual(self.trie.get("batter")[0], "batter")
        self.assertEqual(self.trie.get("multi")[0], 0)
        self.assertEqual(self.trie.get("multi")[1], 1)
        self.assertEqual(self.trie.get("multi")[2], 2)
    
    def test_remove(self):
        self.assertEqual(self.trie.get("ape")[0], "ape")
        self.trie.remove("ape")
        self.assertEqual(self.trie.get("ape"), None)
        self.assertEqual(self.trie.get("aped")[0], "aped")

        self.assertEqual(self.trie.get("multi")[0], 0)
        self.assertEqual(self.trie.get("multi")[1], 1)
        self.assertEqual(self.trie.get("multi")[2], 2)
        self.trie.remove("multi")
        self.assertEqual(self.trie.get("multi"), None)
    
    def test_find(self):
        items = self.trie.find("at")
        self.assertEqual(items[0], ("at", "at"))
        self.assertEqual(items[1], ("attic", "attic"))

        multi_items = self.trie.find("multi")
        self.assertEqual(len(multi_items), 3)
        self.assertEqual(multi_items[0], ("multi", 0))
        self.assertEqual(multi_items[1], ("multi", 1))
        self.assertEqual(multi_items[2], ("multi", 2))

    def test_max_results(self):
        multi_items = self.trie.find("multi", max_results=2)
        self.assertEqual(len(multi_items), 2)
        self.assertEqual(multi_items[0], ("multi", 0))
        self.assertEqual(multi_items[1], ("multi", 1))

if __name__ == "__main__":
    unittest.main()
