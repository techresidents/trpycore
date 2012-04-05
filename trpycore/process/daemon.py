import logging
import os
import sys

from trpycore.os.privs import drop_privs

def exec_daemon(path, args, umask=None, working_directory=None, username=None, groupname=None):
    """Exec a daemon process and return its pid.
       
    This method will fork twice:
     1) First fork will create a short-lived parent process whose
        sole purpose is to exit (to create daemon process) and return
        the daemon process's pid.
     
     2) Second fork is the process which will be replaced with the
        daemon process via the execv call.
    
    Args:
        path: path of executable   - identical to os.execv()
        args: executable arguments - identical to os.execv()
        umask: optional umask for the daemon process
        working_directory: optional working directory for daemon process
        username: optional username to drop privieleges to for deamon process
            (groupname also required)
        groupname: optional groupname to drop privieleges to for daemon process
            (usernam also required)

    Returns:
        pid of the daemon process.
    """
    #Create a pipe so that we can return the pid of the daemon process to the caller
    #The daemon process will write its pid back to the parent through this pipe.
    pipe = os.pipe()
    pid = os.fork()
    
    #Child process
    if pid == 0:
        try:
            #Close read end of pipe
            os.close(pipe[0])
    
            #Note that execv_daemon() will for again.
            #This will ensure that the daemon process is created right away since
            #it's parent is going to exit immediately.
            daemon_pid = execv_daemon(path, args, umask, working_directory, username, groupname)
    
            #Write the daemon pid back to the forker through the pipe
            #and then exit this parent process so that the daemon process actually becomes
            #a daemon process.
            os.write(pipe[1], str(daemon_pid))
            os._exit(0)

        except Exception as error:
            logging.exception(str(error))

    #Parent process
    else:
        #Close write end of the pipe
        os.close(pipe[1])
        daemon_pid = int(os.read(pipe[0], 128))
        return daemon_pid

def execv_daemon(path, args, umask=None, working_directory=None, username=None, groupname=None):
    """Fork and execv a process that will become a daemon as soon as the parent process terminates.

    Forks a child process and replaces it with a process prepared to become a daemon.
    1) New session is created to be come leader of the session and process group
    2) Controlling terminal is removed
    3) stdin, stdout, and stderr redirected to /dev/null
    4) umask is set per parameter
    5) working directory is set per paramter

    Args:
        path: path of executable   - identical to os.execv()
        args: executable arguments - identical to os.execv()
        umask: optional umask for the daemon process
        working_directory: optional working directory for daemon process
        username: optional username to drop privieleges to for deamon process
            (groupname also required)
        groupname: optional groupname to drop privieleges to for daemon process
            (usernam also required)

    Returns:
        The pid of the newly created process is returned.  This is useful if you're preparing a daemon
        process in a short-lived process that needs the pid of the soon-to-be daemon process.
    """

    pid = os.fork()
    
    #Child process
    if pid == 0:
        try:
            #Create new session to become leader of new session and process group.
            #Additionally, this will get rid of any controlling terminal.
            os.setsid()
    
            #Redirect stdin, stdout, and stderr to /dev/null
            dev_null = os.open("/dev/null", os.O_RDWR)
            os.dup2(dev_null, sys.stdin.fileno())
            os.dup2(dev_null, sys.stdout.fileno())
            os.dup2(dev_null, sys.stderr.fileno())
    
            #Set umask if provided
            if umask is not None:
                os.umask(umask)
            
            #Set working directory if provided
            if working_directory is not None:
                os.chdir(working_directory)
            #Drop privileges if username and groupname provided
            if username and groupname:
                drop_privs(username, groupname)

            #Replace current process with daemon process
            os.execv(path, args)

        except Exception as error:
            logging.error(str(error))

    
    #Parent process
    else:
        return pid
