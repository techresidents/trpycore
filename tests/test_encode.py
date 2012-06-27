import unittest

from trpycore.encode.basic import enbase, debase, reverse_bits, basic_encode, basic_decode

class TestEncodeBasic(unittest.TestCase):
    def test_enbase(self):
        self.assertEquals(enbase(0), '0')
        self.assertEquals(enbase(1), '1')
        self.assertEquals(enbase(100000), '255s')
        self.assertEquals(enbase(1234, 16), '4d2')
        self.assertEquals(enbase(1234, 10), '1234')
    
    def test_debase(self):
        self.assertEquals(debase('0'), 0)
        self.assertEquals(debase('1'), 1)
        self.assertEquals(debase('255s'), 100000)
        self.assertEquals(debase('4d2', 16), 1234)
        self.assertEquals(debase('1234', 10), 1234)

    def test_reverse_bits(self):
        self.assertEquals(reverse_bits(8, 4), 1)
        self.assertEquals(reverse_bits(1, 4), 8)

    def test_encoding(self):
        self.assertEquals(basic_encode(8), '4fti4g')
        self.assertEquals(basic_decode('4fti4g'), 8)

if __name__ == "__main__":
    unittest.main()
