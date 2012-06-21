import unittest

from trpycore.alg.grouping import group

def check_result(result, n, min, max):
    ret = True
    if result is not None:
        if len(result) != (max - min + 1):
            ret = False

        check = 0
        for index, value in enumerate(range(min, max + 1)):
            check += result[index] * value

        if n != check:
            ret = False
    return ret


class TestThreadPool(unittest.TestCase):

    def test_basic(self):
        result = group(29, 7, 8)
        self.assertEqual(check_result(result, 29, 7, 8), True)
        self.assertEqual(result, [3, 1])

        result = group(21, 7, 8)
        self.assertEqual(check_result(result, 21, 7, 8), True)
        self.assertEqual(result, [3, 0])

        result = group(22, 2, 4)
        self.assertEqual(check_result(result, 22, 2, 4), True)
        self.assertEqual(result, [1, 0, 5])

        result = group(10, 9, 8)
        self.assertEqual(check_result(result, 10, 9, 8), True)
        self.assertEqual(result, None)
    
    def test_extensive(self):
        for min, max in [ (2,3), (2, 4), (2,5), (3, 7) ]:
            for n in range(min, 100000):
                result = group(n, min, max)
                self.assertNotEqual(result, None)
                self.assertEqual(check_result(result, n, min, max), True)


if __name__ == "__main__":
    unittest.main()
