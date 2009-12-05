"""
Module containing the base protocol classes used to create the proxy. 
They are:

    MessageProtocol   - This protocol parses raw data into particular 
                        message classes, and provides a messageReceived()
                        method for derived classes to override. 

    FilteringProtocol - Extends MessageProtocol to add message filters. 
                        FilterProtocols define a filter type, which can
                        manipulate messages as they are received. 

"""
from __future__ import with_statement
from twisted.internet import reactor, protocol, defer
from twisted.internet.defer import Deferred, DeferredList
from twisted.python import log
from messages import Message



class MessageProtocol(protocol.Protocol):
    """
    Base protocol that parses messages that may span multiple packets.
    Provides a messageReceived method that receives the parsed messages
    as they become ready. 

    Requires the use of a message class that understands the termination
    conditions of the messages on the wire. The messageType attribute 
    should be set to a class that provides a consume method--this method
    accepts the raw data and returns a tuple. 

    The first element in the returned tuple should be True if the
    message has finished parsing, otherwise False. The second element 
    should be any data left unconsumed, or an empty string. 
    """

    # Should be defined to be a message class that defines a consume 
    # method, outlined above. 
    messageType = None


    def __init__(self):
        # This field stores an incomplete message while it is being
        # constructed.
        self._message = None
        self._queue = []


    def dataReceived(self, data):
        """
        Parses as many messages as possible with the given data, resuming
        the previous message if there was one.
        """
        while 1:
            m = self._message or self.messageType()
            done, extra = m.consume(data)

            if done:
                # Discard the previous message, if there was one. This 
                # prevents an infinite loop. 
                self._message = None

                # Entire message was contained in the data, raise
                # the notification. 
                self._queue.append(m)
                if extra:
                    data = extra
                else:
                    break
            else:
                # More data is necessary to complete this message.
                self._message = m
                break
        return self._receive()


    def _receive(self):
        """
        Processes all of the messages currently in the receive queue. 
        """
        log.msg('recv %s' % ''.join(map(str, self._queue)))
        ds = []
        try:
            for x in self._queue:
                ds.append(self.messageReceived(x))

            # Only return deferred if necessary. If we return deferred
            # from, for example, a Startup message, the client will disconnect
            # as it expects us to read its entire message. 
            ds = [d for d in ds if d]
            if ds:
                return DeferredList(ds)

        finally:
            self._queue = []


    def messageReceived(self, message):
        """
        Function that is called whenever a completed message is ready
        to be handled. Override to do something useful in a derived
        class.
        """
        pass


    @property
    def parsingMessage(self):
        return self._message is not None

    
    def discardMessage(self):
        self._message = None




class FilteringProtocol(MessageProtocol):
    filterType = None


    def __init__(self):
        self.filter = self.filterType(self)
        self.filterMessage = self.filter.filter
        MessageProtocol.__init__(self)


    def getPeer(self):
        """
        Gets the companion protocol, that receives the filtered
        message data.
        """
        pass


    def writePeer(self, messages):
        """
        Serializes and writes the message to the peer. 
        """
        p = self.getPeer()
        if p:        
            data = ''.join([m.serialize() for m in messages])
            return p.transport.write(data)
        log.msg('Dropping message(s): %s, peer disconnected.' % 
                ' '.join(messages))
    

    def messageReceived(self, msg):
        # cases - 
        #   just write the message 
        #   write a different set of messages
        #   don't write the message
        #   write a different set of messages, process those replies, 
        #      then write a response (either spoofed or geniune)
        m, cb = self.filterMessage(msg)

        messages = [m] if hasattr(m, 'serialize') else m
        if not messages:
            return None

        d = self.writePeer(messages)
        if cb:
            d.addCallback(cb)
        return d

