# -*- python -*-
from twisted.application import internet, service
import sys
import os

this_dir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(this_dir)

from proxy import PGProxyServerFactory
from twistd import Options



def PGProxy():
    class _PGProxy(object):
        application = service.Application('pgproxy')
        config = Options()

        def __init__(self):
            self.config.parseOptions()
            self.setServiceParent()

    
        def setServiceParent(self):
            self.server = internet.TCPServer(
                self.config['listen-port'], 
                PGProxyServerFactory(self)
                )
            self.server.setServiceParent(self.application)

    return _PGProxy().application


application = PGProxy()
