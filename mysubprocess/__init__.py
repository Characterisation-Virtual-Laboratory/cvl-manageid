"""An internal implementation of subprocess to allow for 'dry-run' operation
through a series of flags"""

import subprocess
debug = True
testvar = False
PIPE = subprocess.PIPE


class myPopen:
    @staticmethod
    def communicate():
        return None, None


def Popen(*args, **kwargs):
    query = kwargs.pop('query',False)
    if debug and not query:
        print(" ".join(args[0]))
        return myPopen
    else:
        return subprocess.Popen(*args, **kwargs)


def call(*args, **kwargs):
    query = kwargs.pop('query',False)
    if debug and not query:
        print(" ".join(args[0]))
        return myPopen
    else:

        return subprocess.Popen(*args, **kwargs)
