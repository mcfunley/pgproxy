"""
This module contains filter objects that inspect and manipulate messages 
as they come over the wire. Filters drop, spoof, translate, or leave 
messages alone. 

Each protocol instance is generally associated with one filter instance. 

"""
from twisted.internet import reactor
from twisted.python import log
import messages
import re
import time




class Filter(object):
    """
    Base class for filters. By default, messages are not altered. To implement
    a derived filter, create methods named filter_<message type code> (so for 
    example, filter_Q would be called when a frontend query message passed 
    through the filter). 

    Each filter function should return one of: 

        self.transmit(msg)        - Just pass the message on to the peer. 
        self.drop(msg)            - Do not pass the message on to the peer. 
        self.translate(*messages) - In place of msg, return one or more messages
                                    in its place. 

    The filter can also call self.spoof(messages) to send replies back to its
    protocol. These replies are deferred. 

    Each protocol (which corresponds to one socket) has one filter associated 
    with it. The protocol using the filter is available via the self.protocol
    field. 
    """
    def __init__(self, protocol):
        self.protocol = protocol
        self.dropMessages = ''


    def ignoreMessages(self, messageTypes):
        """
        Given a string in which the characters are the sequence of
        message types to ignore, causes the filter to drop the
        messages as they are received in that order, without
        additional processing. 
        """
        self.dropMessages += messageTypes


    def filter(self, msg):
        """
        Filters the message. Returns a message or a set of messages to 
        be written to the peer.
        """
        if msg.type == self.dropMessages[:1]:
            self.dropMessages = self.dropMessages[1:]
            return self.drop(msg, 'instructed to ignore')
        return getattr(self, 'filter_' + msg.type, self.transmit)(msg)


    def transmit(self, msg):
        """
        Returns the message without changing it. 
        """
        return [msg], None


    def translate(self, *messages):
        """
        Converts the message to one or more different messages. 
        """
        return messages, None


    def drop(self, msg, why=''):
        """
        Returns a value that will result in the current message being dropped.
        """
        l = 'Dropping message: %s' % msg
        if why:
            l += ' because: %s' % why
        log.msg(l)
        return None, None


    def spoof(self, messages):
        """
        Sends the provided list of messages back to the protocol's transport. 
        """
        log.msg('Spoofing data: %s' % ''.join(map(str, messages)))
        data = ''.join([m.serialize() for m in messages])
        reactor.callLater(0, lambda: self.protocol.transport.write(data))



class FrontendFilter(Filter):
    """
    Filters the messages coming from the client to the PG server. This
    also handles pgproxy custom syntax: BEGIN TEST, ROLLBACK TEST,
    etc.

    Queries are inspected here, mostly to detect transaction-related 
    operations. There are various match_* functions defined to divide
    this work into manageable pieces. 
    
    """

    # Cache some stock replies for messages that we'll be dropping or 
    # translating. 

    spoofed_commit = (
        messages.commandComplete('COMMIT'),
        messages.readyForQuery('transaction'),)

    spoofed_end = (
        messages.commandComplete('END WORK'),
        messages.readyForQuery('transaction'),)

    spoofed_begin = (
        messages.commandComplete('BEGIN'),
        messages.readyForQuery('transaction'),)

    spoofed_rollback = (
        messages.commandComplete('ROLLBACK'),
        messages.readyForQuery('transaction'),)

    # psycopg2 issues a BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED; 
    # query at the start of every connection. It's not legal to set the 
    # transaction isolation level except at the (real) beginning of a 
    # transaction, so the SET has to be ignored. 
    psyco_spoofed_begin = (
        messages.commandComplete('BEGIN'),
        messages.commandComplete('SET'),
        messages.readyForQuery('transaction'),)

    transaction_aborted = (
        messages.errorResponse(
            ('S', 'ERROR'), ('C', '25P02'), 
            ('M', ('current transaction is aborted, commands ignored until '
                   'end of transaction block')), 
            ('F', 'postgres.c'), ('L', '906'), 
            ('R', 'exec_simple_query')),
        messages.readyForQuery('failed'),)


    # Match pgproxy special syntax
    begin_test_re = re.compile("begin test '([^']*)';?$")
    rollback_test_re = re.compile("rollback test '([^']*)';?$")

    # sentinel value for match_* functions to return when they fail to match 
    # a query. 
    no_match = (False, 0)

    # This is a stack of the savepoint names that have been created by this 
    # filter / connection. 
    savepoints = []


    def __init__(self, protocol):
        Filter.__init__(self, protocol)
        self.savepoints = []
    

    def filter_Startup(self, msg):
        """
        Filters Startup messages on connection. If this is not the first 
        startup message, it is dropped and the original auth response is 
        spoofed.
        """
        pg = self.protocol.postgresProtocol
        if pg.authenticationComplete:
            self.spoof(pg.authenticationResponse)
            return self.drop(msg)
        return self.transmit(msg)


    def match_test_syntax(self, msg, sql):
        """
        Begins/rolls back a transaction at the start/end of a
        test. Accepts a special query syntax: 

        <BEGIN|ROLLBACK> TEST "<test name>";
        """
        for rx, stmt, test in ((self.begin_test_re, 'BEGIN', True), 
                               (self.rollback_test_re, 'ROLLBACK', False),):
            m = rx.match(sql)
            if m:
                # Need to keep track of whether we're currently inside of a test
                # or not. Inside of a test, BEGINs are translated to SAVEPOINTs. 
                # Outside of a test they're ignored. (Some drivers, like psyco, 
                # automatically issue the BEGINs, so it's not really possible to
                # require that the frontend only issue the begin statements 
                # within tests.)
                self.protocol.signalTest(test)

                name = m.groups()[0]
                log.msg('%s test: %s' % (stmt, name))
                ret = self.translate(messages.query('%s; -- %s' % (stmt, name)))
                return True, ret

        return self.no_match


    def match_commit(self, msg, sql):
        """
        Drops commit statements outside of tests, and maps them to
        RELEASE SAVEPOINT inside of tests. 
        """
        if sql.startswith('commit'):
            return self.releaseSavepoint(msg, self.spoofed_commit)
        return self.no_match


    def match_end_work(self, msg, sql):
        """
        Drops end work (synonym for commit) statements outside of tests, 
        and maps them to RELEASE SAVEPOINT inside of a test. 
        """
        if sql.startswith('end work') or sql.startswith('end transaction'):
            return self.releaseSavepoint(msg, self.spoofed_end)
        return self.no_match


    def releaseSavepoint(self, msg, spoofData):
        if self.postgresProtocol().transactionStatus == 'failed':
            # The client is trying to commit when the transaction has
            # failed. In this case we don't want to spoof succcess but
            # transmit a RELEASE SAVEPOINT, because the reply would
            # confuse the client driver. 
            self.spoof(self.transaction_aborted)
            return True, self.drop(msg)

        self.spoof(spoofData)
        return self.translateSavepoint(msg, 'RELEASE SAVEPOINT %s')


    def match_rollback(self, msg, sql):
        """
        Drops rollback statements outside of tests, and maps them to
        ROLLBACK TO SAVEPOINT inside of tests. 
        """
        if sql.startswith('rollback'):
            self.spoof(self.spoofed_rollback)
            return self.translateSavepoint(msg, 'ROLLBACK TO SAVEPOINT %s')
        return self.no_match


    def translateSavepoint(self, msg, sqlFormat):
        """
        Replaces the msg in the stream with a savepoint operation, and causes
        the backend's replies regarding the savepoint to be ignored. Requires
        a SQL format string that performs the operation, given the name of the
        savepoint. Outside of a test, or if there are no savepoints, msg is
        dropped.
        """
        if self.protocol.inTest() and self.savepoints:
            m = self.translate(
                messages.query(sqlFormat % self.savepoints.pop()))
            self._ignoreBackendMessages('CZ')
        else:
            m = self.drop(msg)
        return True, m


    def _ignoreBackendMessages(self, messageTypes):
        """
        Tells the backend to ignore the given set of message replies. 
        """
        self.postgresProtocol().ignoreMessages(messageTypes)


    def postgresProtocol(self):
        """
        Returns the postgres protocol peered with the owner protocol of 
        this filter. 
        """
        return self.protocol.getPeer()


    def match_begin(self, msg, sql):
        """
        Matches begin statements. The reply to this is always spoofed. The 
        backend will either receive no message, or a savepoint if we are 
        currently inside of a test. 
        """
        if sql.startswith('begin'):
            if 'set transaction' in sql:
                # this is the psycopg2-issued BEGIN - hacky, but w/e
                self.spoof(self.psyco_spoofed_begin)
            else:
                self.spoof(self.spoofed_begin)
                
            # Translate the begin to a new savepoint, if we're in a test.
            # If we're not then just ignore it. 
            if self.protocol.inTest():
                m = self.translate(self.savepoint())
                self._ignoreBackendMessages('CZ')
            else:
                m = self.drop(msg, 'BEGIN outside of test')
            return True, m

        return self.no_match


    def savepoint(self):
        """
        Pushes a new savepoint onto the stack. Names the savepoint uniquely. 
        Returns the query message to be sent to the backend.
        """
        name = 'sp_%s' % str(time.time()).replace('.', '_')
        self.savepoints.append(name)
        return messages.query('SAVEPOINT %s' % name)


    def filter_Q(self, msg):
        """
        Inspects query messages in order to support special syntax, and 
        to play games with transactions. 
        """
        filters = (self.match_test_syntax, 
                   self.match_begin,
                   self.match_commit,
                   self.match_end_work,
                   self.match_rollback,)

        # ignore the \0 in the query. 
        sql = msg.data.lower()[:-1]

        for m in filters:
            matched, val = m(msg, sql)
            if matched:
                return val

        # nothing matched, just pass on the query. 
        return self.transmit(msg)


    def filter_X(self, msg):
        """
        Drops terminate messages.
        """
        return self.drop(msg)



class BackendFilter(Filter):
    """
    Filters the messages coming from the PG server to the client. 
    """    

    def saveAuth(self, msg):
        """
        Saves authentication response messages from the backend. These will be
        sent to new frontends without actually re-authenticating. 
        """
        self.protocol.saveAuthMessage(msg)
        return self.transmit(msg)


    filter_R = saveAuth
    filter_S = saveAuth
    filter_K = saveAuth


    def filter_Z(self, msg):
        """
        Saves the initial ReadyForQuery message, which is considered part of the
        authentication response. Subsequent Z's are returned unmolested. 
        """
        self.protocol.setTransactionStatus(msg.transaction_status)
        if not self.protocol.authenticationComplete:
            self.protocol.saveAuthMessage(msg)
        return self.transmit(msg)
        
