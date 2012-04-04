import grp
import os
import pwd

def drop_privs(username, groupname, umask=None):

    #If not root, nothing to drop.
    if os.getuid() != 0:
        return

    uid = pwd.getpwnam(username).pw_uid
    gid = grp.getgrnam(groupname).gr_gid

    #Remove groups
    os.setgroups([])

    #Set gid and uid
    os.setgid(gid)
    os.setuid(uid)
    
    #Set umask if provided
    if umask is not None:
        os.umask(umask)
