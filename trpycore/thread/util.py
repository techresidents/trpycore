import time

def join(threads, timeout=None):
    """Join each thread in threads iterable with overarching timeout.

    Arguments:
        threads: iterable of threads to join
        timeout: timeout in seconds for all joins
    """    
    threads = threads or []

    start = time.time()
    remaining_timeout = timeout
    for thread in threads:
        if remaining_timeout is None or remaining_timeout > 0:
            thread.join(remaining_timeout)
            if remaining_timeout is not None:
                remaining_timeout = timeout -  (time.time() - start)
        else:
            break
