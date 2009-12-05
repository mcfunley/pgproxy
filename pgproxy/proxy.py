"""
Module containing the protocols that make up the proxy, as well 
as the factory that creates protocols for new client connections.

There is only one connection to the postgres backend at a time, 
so no postgres protocol factory is necessary. The postgres backend 
protocol is created as needed by the client factory. 

The single postgres backend protocol maintains a list of its clients,
and designates one of them as the active connection, which will 
receive the replies. 

"""
from twisted.internet import reactor, defer, protocol
from twisted.python import log
from protocol import FilteringProtocol
from messages import FrontendMessage, BackendMessage
from filters import FrontendFilter, BackendFilter
from itertools import count
import messages



class PostgresClientProtocol(FilteringProtocol):
    pgproxyFactory = None

    messageType = BackendMessage
    filterType = BackendFilter
    dead = False
    in_test = False
    transactionStatus = None


    def __init__(self):
        FilteringProtocol.__init__(self)
        
        # The list of clients is maintained as a stack because disconnections
        # automatically designate the next one in the list as the active. 
        # However, queries can be received from clients in any order. When a 
        # frontend recieves a query, it will call this instance back so that 
        # we know to send it the reply. 
        self.clientStack = []

        # The first set of authentication response messages (between the
        # AuthenticationOk/R and the ReadyForQuery/Z) are saved here. 
        self.authenticationResponse = []


    def setTransactionStatus(self, status):
        self.transactionStatus = status


    def connectionMade(self):
        log.msg('PostgresClientProtocol connection made: %s' % id(self))


    def connectionLost(self, *a):
        log.msg('PostgresClientProtocol connection lost.')
        self.pgproxyFactory.postgresClientLost()
        self.dead = True


    def signalTest(self, value):
        """
        Called to start or end a test. Inside tests, BEGIN/(ROLLBACK|COMMIT) 
        pairs are rewritten to use savepoints.        
        """
        self.in_test = value


    def inTest(self):
        return self.in_test


    def attachClient(self, client):
        """
        Adds a new client to the list. 
        """
        if self.parsingMessage:
            raise AssertionError(
                'Still parsing a message, but attaching client.')
        log.msg('Attaching new client.')
        self.clientStack.append(client)


    def detachClient(self, client):
        """
        Called when a client has disconnected. 
        """
        log.msg('Detaching client.')
        if self.parsingMessage:
            self.discardMessage()
        self.clientStack.remove(client)


    def activateClient(self, client):
        """
        Activates a client that is already attached. 
        """
        if client == self.currentClient():
            return
        self.clientStack.remove(client)
        self.clientStack.append(client)


    def currentClient(self):
        """
        Returns the current client (the one that will receive reply messages). 
        """
        if not self.clientStack:
            return None
        return self.clientStack[-1]


    getPeer = currentClient


    def saveAuthMessage(self, msg):
        """
        Stores an authentication response method. Also keeps track of
        server settings. The original authentication response as well as
        an updated set of server settings is sent to new clients as 
        they connect. 
        """
        if self.authenticationComplete:
            if msg.type == 'S':
                # If a parameter has been SET, add to the authentication 
                # message, overwriting the previous value if it's there. 
                self.overwriteSetting(msg)
                return

            # otherwise this is not expected. 
            raise AssertionError(
                'Adding auth message, but authentication complete')
        log.msg('Saving authentication message: %s' % msg.type)
        self.authenticationResponse.append(msg)

        
    def overwriteSetting(self, msg):
        """
        Given a parameter status backend message, replaces the same named 
        parameter in the authentication response that will be sent to later
        clients.
        """
        for i, x in zip(count(0), self.authenticationResponse):
            if getattr(x, 'name', '') == msg.name:
                self.authenticationResponse[i] = msg


    def ignoreMessages(self, messageTypes):
        """
        Given a string in which the characters are the sequence of
        message types to ignore, causes the protocol to drop the messages
        as they are received in that order. 
        """
        self.filter.ignoreMessages(messageTypes)


    @property
    def authenticationComplete(self):
        """
        Returns true if the authentication handshake has been fully received. 
        """
        if not self.authenticationResponse:
            return False

        # This is true if the last message in the auth response is 
        # ReadyForQuery. 
        last = self.authenticationResponse[-1]
        return last.type == 'Z'


    def terminate(self):
        """
        Shuts down the PG connection. This only happens when PGProxy
        is shutting down.
        """
        self.transport.write(messages.terminate().serialize())



class PGProxyProtocol(FilteringProtocol):
    """
    Protocol that represents one client frontend connection. There are many
    of these and one PostgresProtocol. 
    """

    messageType = FrontendMessage
    filterType = FrontendFilter


    def __init__(self):
        FilteringProtocol.__init__(self)
        self.postgresProtocol = None


    def signalTest(self, value):
        """
        Called to set the current testing state. This determines whether
        or not BEGINs/ROLLBACKs are rewritten to SAVEPOINTs.
        """
        self.postgresProtocol.signalTest(value)

    
    def inTest(self):
        """
        Returns true if the proxy is currently in a test.
        """
        return self.postgresProtocol.inTest()


    def connectionMade(self):
        log.msg('PGProxyProtocol connection made.')
        return self.factory.attachPostgresProtocol(self)


    def connectionLost(self, reason=protocol.connectionDone):
        log.msg('PGProxyProtocol connection lost')
        if self.postgresProtocol:
            self.postgresProtocol.detachClient(self)


    def getPeer(self):
        return self.postgresProtocol


    def messageReceived(self, msg):
        # tell the postgres protocol to mark this one as the active, 
        # because it will need to see the reply for this message. 
        self.postgresProtocol.activateClient(self)
        return FilteringProtocol.messageReceived(self, msg)



class PGProxyServerFactory(protocol.ServerFactory):
    """
    Class responsible for creating new PGProxyProtocol instances as 
    client connections are received. 

    This also maintains at most one PostgresProtocol instance at a time, 
    that is shared among the client protocols. 
    """

    protocol = PGProxyProtocol

    # This may hold a deferred if the postgres protocol is already being 
    # created. 
    creatingPostgresProtocol = None

    
    def __init__(self, pgproxy):
        self.pgproxy = pgproxy
        self.postgresProtocol = None

        # Give postgres clients access to this factory
        PostgresClientProtocol.pgproxyFactory = self


    def stopFactory(self):
        if self.postgresProtocol:
            log.msg('Sending terminate to postgres.')
            self.postgresProtocol.terminate()


    def attachPostgresProtocol(self, pgproxyProtocol):
        """
        Connects a new pgproxy protocol instance to the single
        PostgresClientProtocol.
        """
        def attach(s):
            s.attachClient(pgproxyProtocol)
            pgproxyProtocol.postgresProtocol = s
            return s

        if self.postgresProtocol:
            # Not the first client. Attach immediately. 
            return defer.succeed(attach(self.postgresProtocol))

        # If this is the first client, we need to stop reading from 
        # it until the connection is made. 
        pgproxyProtocol.transport.pauseProducing()
        def resume(s):
            pgproxyProtocol.transport.resumeProducing()
            return s

        d = self.makePostgresProtocol().addCallback(attach)
        return d.addCallback(resume)


    def postgresClientLost(self):
        log.msg('Factory reset - postgres client lost')
        self.postgresProtocol = None


    def makePostgresProtocol(self):
        """
        Creates the postgres server connection. This is called only if 
        self.postgresProtocol is not set. 
        """
        if self.creatingPostgresProtocol:
            log.msg('Already creating postgres protocol')
            return self.creatingPostgresProtocol

        log.msg('Creating Postgres connection.')
        def gotProto(p):
            if p.dead:
                log.msg('PostgresClientProtocol died immediately.')
                return

            if self.postgresProtocol:
                if self.postgresProtocol.dead:
                    log.msg('Already had postgres protocol, but it was dead.')
                else:
                    log.msg('Already had postgres protocol')
                    return self.postgresProtocol

            log.msg('Got PostgresClientProtocol instance.')
            self.postgresProtocol = p
            self.creatingPostgresProtocol = None
            return p

        cc = protocol.ClientCreator(reactor, PostgresClientProtocol)
        self.creatingPostgresProtocol = cc.connectTCP(
            self.pgproxy.config['server-host'],
            self.pgproxy.config['server-port']).addCallback(gotProto)
        return self.creatingPostgresProtocol

