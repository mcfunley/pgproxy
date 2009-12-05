from __future__ import with_statement
import subprocess
import os
import socket
import sys
import time
import signal
from contextlib import closing

__all__ = ['__version__', 'run', 'PGProxy',]


_this_dir = os.path.realpath(os.path.dirname(__file__))

__version__ = '0.1'


def run(listenPort=5433, serverAddr=('localhost', 5432), 
        pidfile=None, logfile=None):
    p = PGProxy(listenPort, serverAddr, pidfile, logfile)
    return p.start()



def _serverUp(port):
    """
    Returns true if the given port is accepting connections.
    """
    try:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.connect(('localhost', port))
    except socket.error:
        return False
    return True



def _waitForServerUp(port):
    for _ in range(50):
        if _serverUp(port):
            return True
        time.sleep(0.1)
    return False



class PGProxy(object):

    def __init__(self, listenPort=5433, serverAddr=('localhost', 5432), 
                 pidfile=None, logfile=None):
        self.serverHost, self.serverPort = serverAddr
        self.listenPort = listenPort
        self.pidfile = pidfile or os.path.join(_this_dir, 'pgproxy.pid')
        self.logfile = logfile
        self.tacfile = os.path.join(_this_dir, 'service.tac')
        self.twistd = os.path.join(_this_dir, 'twistd.py')
        self.proxy = None


    def start(self):
        args = [sys.executable, self.twistd, '-n', '-y', self.tacfile, 
                '--pidfile=%s' % self.pidfile, 
                '--listen-port=%s' % self.listenPort,
                '--server-port=%s' % self.serverPort, 
                '--server-host=%s' % self.serverHost,]
        if self.logfile:
            args.extend(['-l', self.logfile])
        self.proxy = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if not _waitForServerUp(self.listenPort):
            raise AssertionError('Could not start pgproxy on port %s' 
                                 % self.listenPort)
        return self


    __enter__ = start


    def stop(self):
        os.kill(self.proxy.pid, signal.SIGTERM)
        return self

    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
