import logging
import threading
import time
import Queue

from trpycore.thread.util import join

STOP_ITEM = object()

class WorkerThread(threading.Thread):
    """Worker thread for use with ThreadPool class.
    Worker will pull work items from the queue
    and delegate item processing to the processor.
    """

    def __init__(self, queue, processor, daemon=False):
        """WorkerThread constructor

        Arguments:
            queue: queue to pull work items from
            processor: callable or object with 'process'
                method taking a single work item parameter.
            daemon: if True make a daemon thread
        """
        super(WorkerThread, self).__init__()

        self.daemon = daemon
        self.queue = queue

        if callable(processor):
            self.processor = processor
        elif hasattr(processor, "process"):
            self.processor = processor.process
        else:
            raise NotImplementedError("processor must be callable or have a 'process' method")
    
    def run(self):
        """Worker run method."""
        while True:
            item = self.queue.get()
            if item is STOP_ITEM:
                break
            try:
                self.processor(item)
            except Exception as error:
                logging.exception(error)


class ThreadPool(object):
    """Thread pool class.
    This pool can either be extended with the 'process' method overriden,
    or passed a processor callabale or any object containing a 'process'
    method which will receive a worker item as its sole argument.
    """

    def __init__(self, num_threads, processor=None, daemon=False, queue=None):
        """ThreadPool constructor.

        Arguments:
            num_threads: number of worker thread to create
            processor: optional callable or object with 'process'
                method taking a single work item parameter.
                If not provided, self.process() will be used
                to process work items.
            daemone: if True worker threads will be made daemone threads
            queue: optional worker item Queue instance
        """
        self.num_threads = num_threads
        self.processor = processor or self
        self.daemon = daemon
        self.queue = queue or Queue.Queue()
        self.threads = []
        self.running = False

    def start(self):
        """Start the thread pool."""
        if not self.running:
            self.running = True
            self.threads = []
            for i in range(self.num_threads):
                worker = WorkerThread(self.queue, self.processor, self.daemon)
                self.threads.append(worker)
                worker.start()
    
    def stop(self):
        """Stop the thread pool."""
        if self.running:
            self.running = False
            for i in range(self.num_threads):
                self.queue.put(STOP_ITEM)
    
    def join(self, timeout=None):
        """Join worker threads.
        If timeout is not None, is_alive() must be used to determine
        if the thread pool is still running.

        Arguments:
            timeout: join timeout in seconds
        """
        join(self.threads, timeout)
   
    def is_alive(self):
        """Test if thread pool is running / alive.

        Returns: True if threadpool is running, false otherwise.
        """
        result = self.running
        for thread in self.threads:
            if thread.is_alive():
                result = True
                break
        return result

    def put(self, item, block=True, timeout=None):
        """Put a work item in the queue for processing by worker thread.

        Arguments:
            item: processor specific work item
            block: if True call will block if work item is full
            timeout: if block True, block timeout seconds at most
                and then raise Queue.Full exception.
        
        Raises:
            Queue.Full
        """
        self.queue.put(item, block, timeout)

    def process(self, item):
        """Work item process method.
        
        This method will be used as the work item processor in the event
        that no explicit processor is provided in the constructor.
        Subclasses should override this method if an explicit processor
        callable or object is not being used.
        """
        raise NotImplementedError("process not implemented")
