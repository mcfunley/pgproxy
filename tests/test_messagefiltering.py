from twisted.trial import unittest
from pgproxy.protocol import MessageProtocol, FilteringProtocol
from pgproxy.filters import Filter
from twisted.internet import defer
from twisted.internet.defer import Deferred
from twisted.internet.base import DelayedCall


DelayedCall.debug = True



class MockMessage(object):
    def __init__(self):
        self.value = ''
        self.type = 'M'


    def consume(self, data):
        if '\n' in data:
            parts = data.split('\n')
            self.value += parts[0]
            extra = '\n'.join(parts[1:])
            return True, extra
        self.value += data
        return False, None


    def serialize(self):
        return self.value + '\n'
    


class MockProtocol(MessageProtocol):
    messageType = MockMessage

    def __init__(self):
        self.receivedMessages = []
        MessageProtocol.__init__(self)

    def messageReceived(self, m):
        self.receivedMessages.append(m)



class MessageProtocolTests(unittest.TestCase):
    def test_parsing_one_message(self):
        p = MockProtocol()
        p.dataReceived('foobar\n')
        self.assertEqual(p.receivedMessages[0].value, 'foobar')
        
    
    def test_parsing_two_messages(self):
        p = MockProtocol()
        setattr(p, 'first', 1)

        d = Deferred()

        def f(m):
            val = 'foo' if p.first else 'bar'
            self.assertEqual(m.value, val)
            if p.first:
                p.first = 0
            else:
                d.callback(None)

        p.messageReceived = f

        p.dataReceived('foo\nbar\n')
        return d


    def test_multiple_packets(self):
        p = MockProtocol()
        p.dataReceived('foob')
        self.assertEqual(p.receivedMessages, [])
        p.dataReceived('ar\n')
        self.assertEqual(p.receivedMessages[0].value, 'foobar')


    def test_multiple_messages_multiple_packets(self):
        # if the remembered message is not cleared after being resumed, this 
        # will enter an infinite loop. 
        p = MockProtocol()
        p.dataReceived('foob')
        self.assertEqual(p.receivedMessages, [])
        p.dataReceived('ar\nbaz\ngoo\n')
        self.assertEqual([m.value for m in p.receivedMessages],
                         ['foobar', 'baz', 'goo'])




class MockTransport(object):
    expect = None
    test = None

    def write(self, data):
        if self.expect:
            self.test.assertEqual(self.expect, data)
        return defer.succeed(None)



class MockFilter(Filter):
    filterNext = False

    def filter(self, msg):
        if self.filterNext:
            m = MockMessage()
            m.consume('filtered\n')
            self.protocol.writePeer([m])
            return None, None
        return msg, None



class MockFilterProtocol(FilteringProtocol):
    peer = None
    filterType = MockFilter


    def __init__(self, test):
        FilteringProtocol.__init__(self)
        self.transport = MockTransport()
        self.transport.test = test


    def getPeer(self):
        return self.peer

    
    def expect(self, value):
        self.transport.expect = value



class TestFilteringProtocol(unittest.TestCase):

    def filters(self):
        f = MockFilterProtocol(self)
        f2 = MockFilterProtocol(self)
        f.peer, f2.peer = f2, f
        return f, f2


    def msg(self):
        m = MockMessage()
        m.consume('original\n')
        return m


    def test_filter_message(self):
        f, f2 = self.filters()
        f.filter.filterNext = True
        f2.expect('filtered\n')
        return f.messageReceived(self.msg())

    
    def test_original_returned(self):
        f, f2 = self.filters()
        f2.expect('original\n')
        return f.messageReceived(self.msg())
