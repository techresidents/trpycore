import threading

class AsyncResult(object):
    """AsyncResult is a convenience class for async methods.

    It is intended as an alternative to passing callbacks to async methods
    to be notified when an async operation completes. Instead, async methods
    can return this object which the caller can use to wait for a result
    in a non-busy manner.
    """

    class Timeout(Exception):
        pass

    def __init__(self):
        self.event = threading.Event()
        self.result = None
        self.exception = None
    
    def ready(self):
        return self.event.is_set()
    
    def get(self, block=True, timeout=None):
        if not self.ready() and block:
            self.event.wait(timeout)
        
        if not self.ready():
            raise self.Timeout("Timeout: result not ready")
        
        if self.exception is not None:
            raise self.exception
        else:
            return self.result

    def set(self, value=None):
        self.result = value
        self.event.set()

    def set_exception(self, exception):
        self.exception = exception
        self.event.set()
