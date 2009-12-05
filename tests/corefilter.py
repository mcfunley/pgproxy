from twisted.trial import unittest
from pgproxy.proxy import PostgresClientProtocol, PGProxyProtocol
from pgproxy import messages



class MockTransport(object):
    deferred = None

    def write(self, data):
        if self.deferred:
            self.deferred.callback(data)


    def expectNothing(self):
        def die(data):
            raise AssertionError('should not have been called, got %r' % data)
        self.write = die


class MockPeer(object):
    transport = MockTransport()



class FilterTest(unittest.TestCase):
    def protocols(self):
        b = PostgresClientProtocol()
        b.transport = MockTransport()
        
        f = PGProxyProtocol()
        f.transport = MockTransport()
        f.postgresProtocol = b
        b.attachClient(f)
        return b, f


    def backend(self):
        return self.protocols()[0]


    def receiveAuth(self, p):
        p.messageReceived(messages.authenticationOk())
        p.messageReceived(messages.parameterStatus('foo', 'bar'))
        p.messageReceived(messages.readyForQuery('idle'))
        
