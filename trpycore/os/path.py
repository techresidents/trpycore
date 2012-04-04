import fcntl
import os
from contextlib import contextmanager

class LockFileException(Exception):
    pass

@contextmanager
def lockfile(path, create_directory=False, directory_mode=0700, mode=0600):
    """lockfile context manager
       
       Creates lock file with mode if it does not already exist, otherwise,
       LockFileException is raised. The lock file will be safely removed when
       the context manager exits.

       If create_directory is True, the necessary directories will be created
       with mode directory_mode via os.makedirs()
    """
    try:
        fd = None
        
        #Create directories if requested
        if create_directory:
            directory = os.path.dirname(path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, directory_mode)

        
        #If lock file exists raise exception
        if os.path.exists(path):
            raise LockFileException("pid file exists - %s" % path)
        
        
        #Create lock file
        fd = os.open(path, os.O_CREAT|os.O_RDWR, mode)
        
        #Attempt to acquire the lock and raise exception if we can't
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            #Close fd and set to None to ensure that the lock file
            #is not removed in the finally block3
            if fd:
                os.close(fd)
                fd = None
            raise LockFileException("pid file exists - %s" % path)

        yield fd

    finally:
        if fd:
            os.close(fd)
            os.remove(path)
