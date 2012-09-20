#!/usr/bin/env python

import argparse
import os
import logging
import platform
import subprocess
import sys


def main(argv):

    def parse_arguments():
        parser = argparse.ArgumentParser(description="bootstrap.py bootstraps your environment with specified modules.")
        parser.add_argument("-r", "--requirements", default=os.path.join("requirements", "requirements.txt"))
        return parser.parse_args(argv[1:])

    #configure logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setLevel(logging.INFO)
    logger.addHandler(consoleHandler)
    
    log = logging.getLogger("main")
                                         
    try:
        args = parse_arguments()

        if "VIRTUAL_ENV" not in os.environ:
            log.error("bootstrap must be run in a virtual environment")
            return 2
        
        if platform.system() == 'Darwin':
            os.putenv("CFLAGS", "-I/opt/local/include")
        else:
            os.putenv("CFLAGS", "-I/opt/3ps/include -L/opt/3ps/lib64 -L/opt/3ps/lib")
            os.putenv("LIBRARY_PATH", "$LIBRARY_PATH:/opt/3ps/lib64:/opt/3ps/lib")

        subprocess.call(["pip", "install", "--requirement", args.requirements])

        return 0

    except Exception as error:
        log.error("Unhandled exception: %s" % str(error))
        return 1

if __name__ == "__main__":
    sys.exit(main(sys.argv))
