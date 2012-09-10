import threading
import time
import unittest
import Queue

import testbase
from trpycore.thread.threadpool import ThreadPool

class TestThreadPool(unittest.TestCase):
    def setUp(self):
        self.counter = 0
        self.sleep = 0
        self.lock = threading.Lock()

        def processor(item):
            time.sleep(self.sleep)
            with self.lock:
                self.counter += item

        self.pool = ThreadPool(5, processor, queue=Queue.Queue(5))

    def test_basic(self):
        self.assertEqual(self.pool.is_alive(), False)
        self.assertEqual(self.counter, 0)
        self.pool.put(1)
        self.pool.put(2)
        self.pool.put(1)
        self.assertEqual(self.counter, 0)
        self.pool.start()
        self.assertEqual(self.pool.is_alive(), True)
        self.pool.stop()
        self.pool.join()
        self.assertEqual(self.counter, 4)
        self.assertEqual(self.pool.is_alive(), False)
    
    def test_join_timeout(self):
        self.sleep = 2
        self.pool.start()
        self.pool.put(1)
        self.pool.stop()
        self.pool.join(1)
        self.assertEqual(self.pool.is_alive(), True)
        self.pool.join(2)
        self.assertEqual(self.pool.is_alive(), False)
        self.assertEqual(self.counter, 1)
    
    def test_queue_full(self):
        self.pool.put(1)
        self.pool.put(1)
        self.pool.put(1)
        self.pool.put(1)
        self.pool.put(1)
        with self.assertRaises(Queue.Full):
            self.pool.put(1, block=False)
        self.pool.start()
        self.pool.stop()
        self.pool.join()
        self.assertEqual(self.counter, 5)
    
    def test_subclass(self):
        class MyThreadPool(ThreadPool):
            def __init__(self, *args, **kwargs):
                super(MyThreadPool, self).__init__(*args, **kwargs)
                self.lock = threading.Lock()
                self.counter = 0
            def process(self, item):
                with self.lock:
                    self.counter += item
        
        pool = MyThreadPool(5)
        self.assertEqual(pool.is_alive(), False)
        self.assertEqual(pool.counter, 0)
        pool.put(1)
        pool.put(2)
        pool.put(1)
        self.assertEqual(pool.counter, 0)
        pool.start()
        self.assertEqual(pool.is_alive(), True)
        pool.stop()
        pool.join()
        self.assertEqual(pool.counter, 4)
        self.assertEqual(pool.is_alive(), False)




if __name__ == "__main__":
    unittest.main()
