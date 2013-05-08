import glob
import os 
import re

#fabric imports
from fabric.api import *

env.project = os.path.basename(os.path.dirname(__file__))
env.project_dir = os.path.dirname(__file__)


def _file_replace(fileglob, pattern, replacement, count=0, flags=re.DOTALL, expected_substitutions=1):
    """Helper to replace a pattern in files."""
    for filename in glob.glob(fileglob):
        with open(filename, 'r') as f:
            data = f.read()
        
        result, substitutions = re.subn(pattern, replacement, data, count, flags)
        if substitutions != expected_substitutions:
            raise RuntimeError("number of substitutions (%d) != expected substitutions (%d)"\
                    % (substitutions, expected_substitutions))

        with open(filename, 'w') as f:
            f.write(result)


def bump_version(current_version, new_version):
    """Bump version."""
    info = {
        "library": env.project,
        "current_version": current_version,
        "new_version": new_version
    }

    _file_replace(
            fileglob="setup.py",
            pattern=r"version\s*=\s*'{current_version}'".format(**info),
            replacement=r"version = '{new_version}'".format(**info))
    
def release(new_version, new_snapshot_version, current_version=None):
    """Cut release"""

    new_major_version = new_version.rsplit('.', 1)[0]

    info = {
        "library": env.project,
        "current_version": current_version or "%s-SNAPSHOT" % new_major_version,
        "new_version": new_version,
        "new_major_version": new_major_version,
        "new_snapshot_version": new_snapshot_version,
        "release_branch": "release-%s" % new_major_version
    }

    answer = prompt("Releasing with the following parameters\n\n%s\n\nContinue with release?" % info, default="n")
    if answer not in ["Y", "y", "Yes", "YES"]:
        return

    #pull the latest
    local("git checkout master")
    local("git pull")
    local("git checkout integration")
    local("git pull")

    #create and checkout release branch
    local("git checkout integration")
    local("git branch {release_branch}".format(**info))
    local("git push origin {release_branch}".format(**info))
    local("git branch --set-upstream {release_branch} origin/{release_branch}".format(**info))
    local("git checkout {release_branch}".format(**info))

    #bump release branch versions
    bump_version(info["current_version"], info["new_version"])

    #commit changes to release branch and push 
    local("git commit -a -m 'Bumping version to {new_version}'".format(**info))
    local("git push")

    #Checkout master and merge release
    local("git checkout master")
    local("git branch --no-merged")
    local("git merge --no-ff {release_branch}".format(**info))
    local("git tag -a {new_version} -m 'Release {new_version}'".format(**info))
    local("git push --all")
    local("git push --tags")
    local("git branch --no-merged")

    #Checkout integration branch and merge release
    local("git checkout integration")
    local("git branch --no-merged")
    local("git merge --no-ff {release_branch}".format(**info))
    local("git branch --no-merged")

    #bump release branch versions
    bump_version(info["new_version"], info["new_snapshot_version"])
    local("git commit -a -m 'Bumping version to {new_snapshot_version}'".format(**info))
    local("git push")
