import time

def join(greenlets, timeout=None):
    """Join each greenlet in greenlets iterable with overarching timeout.

    Arguments:
        greenlets: iterable of greenlets to join
        timeout: timeout in seconds for all joins
    """    
    greenlets = greenlets or []

    start = time.time()
    remaining_timeout = timeout
    for greenlet in greenlets:
        if remaining_timeout is None or remaining_timeout > 0:
            greenlet.join(remaining_timeout)
            if remaining_timeout is not None:
                remaining_timeout = timeout -  (time.time() - start)
        else:
            break
