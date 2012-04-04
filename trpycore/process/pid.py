import errno
import os
from contextlib import contextmanager

from trpycore.os.path import lockfile, LockFileException

class PidFileException(Exception):
    pass

@contextmanager
def pidfile(path, create_directory=False, directory_mode=0700, mode=0600):
    """pid file context manager
       
       Creates pid file with mode if it does not already exist, otherwise,
       PidFileException is raised. The pid file will be safely removed when
       the context manager exits.

       If create_directory is True, the necessary directories will be created
       with mode directory_mode via os.makedirs()
    """
    try:
        with lockfile(path, create_directory, directory_mode, mode) as fd:
            os.write(fd, str(os.getpid()))
            yield fd

    except LockFileException:
        raise PidFileException("pid file exists - %s" % path)


def pid_exists(pid):
    result = False
    try:
        #Send signal 0 to test if process exists.
        #This will do nothing to the process.
        os.kill(pid, 0)
        result = True
    except OSError as error:
        #Process does not exist if we get an exception
        #unless permission is denied.
        result = (error.errno == errno.EPERM)
    
    return result
