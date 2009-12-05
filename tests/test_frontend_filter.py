from corefilter import FilterTest
from twisted.internet.defer import Deferred
from twisted.internet import defer
from pgproxy import messages
from pgproxy.filters import FrontendFilter



class FrontendFilterTests(FilterTest):

    def test_spoof_handshake(self):
        b, f = self.protocols()
        self.receiveAuth(b)
        
        auth = ''.join([m.serialize() for m in b.authenticationResponse])
        
        # now a new Startup message through the frontend should 
        # result in its transport receiving a spoofed auth message.
        def checkAuth(data):
            self.assertEqual(data, auth)
        d = Deferred()
        d.addCallback(checkAuth)
        f.transport.deferred = d

        # the startup message should not make it to the backend's transport
        b.transport.expectNothing()

        f.messageReceived(messages.startup('postgres'))
        return d


    def _dropped_and_spoofed_test(self, sql, response_messages):
        b, f = self.protocols()
        b.transport.expectNothing()

        rdy = ''.join([m.serialize() for m in response_messages])
        def checkSpoofedResponse(data):
            self.assertEqual(data, rdy)
        
        d = Deferred()
        d.addCallback(checkSpoofedResponse)
        f.transport.deferred = d

        f.messageReceived(messages.query(sql))
        return d


    def _commit_test(self, sql):
        return self._dropped_and_spoofed_test(sql, FrontendFilter.spoofed_commit)


    def test_end_work(self):
        return self._dropped_and_spoofed_test(
            'end work;', FrontendFilter.spoofed_end)


    def test_end_transaction(self):
        return self._dropped_and_spoofed_test(
            'END TRANSACTION', FrontendFilter.spoofed_end)


    def test_begin_test(self):
        # the filter should map BEGIN TEST 'name' to:
        # BEGIN; -- name
        return self._test_syntax_test("BEGIN TEST 'test name'")


    def test_begin_test_case_insensitive(self):
        return self._test_syntax_test("begin test 'test name'")


    def _test_syntax_test(self, sql):
        b, f = self.protocols()

        # extract the rollback/begin
        s = sql.split(' ')[0]

        def checkBegin(data):
            msg = messages.query('%s; -- test name' % s)
            self.assertEqual(data.lower(), msg.serialize().lower())

        d = Deferred()
        d.addCallback(checkBegin)
        b.transport.deferred = d

        f.messageReceived(messages.query(sql))
        return d


    def test_begin_test_semicolon_accepted(self):
        return self._test_syntax_test("BEGIN TEST 'test name';")


    def test_rollback_test(self):
        return self._test_syntax_test("ROLLBACK TEST 'test name'")


    def test_rollback_test_semicolon_accepted(self):
        return self._test_syntax_test("ROLLBACK TEST 'test name';")


    def test_drop_commits1(self):
        return self._commit_test('COMMIT')


    def test_drop_commits2(self):
        return self._commit_test('COMMIT;')


    def test_drop_commits3(self):
        return self._commit_test('commit')


    def test_normal_queries_work(self):
        b, f = self.protocols()
        q = messages.query('select 1;')

        def checkQuery(data):
            self.assertEqual(data, q.serialize())

        d = Deferred()
        d.addCallback(checkQuery)
        b.transport.deferred = d

        f.messageReceived(q)
        return d


    def test_drop_terminate(self):
        b, f = self.protocols()
        b.transport.expectNothing()
        f.messageReceived(messages.terminate())

        
    def test_psyco_connection_bootup_handled(self):
        # Psyco issues a BEGIN; SET TRANSACTION ISOLATION LEVEL ...
        # at the start of each connection. Since the SET TRANSACTION
        # ISOLATION LEVEL is only legal at the start of a connection,
        # it will raise an error if pgproxy does not spoof it. 
        return self._dropped_and_spoofed_test(
            'BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED;',
            FrontendFilter.psyco_spoofed_begin)

    
    def test_begins_ignored_outside_of_test(self):
        # Make sure that BEGIN outside of a test is just ignored. We would 
        # expect to see one if issuing the BEGIN TEST instruction through 
        # a driver that issues an automatic BEGIN with each connection. 
        return self._dropped_and_spoofed_test(
            'BEGIN;', FrontendFilter.spoofed_begin)


    def test_rollback_rewritten_to_rollback_to_savepoint_in_test(self):
        return self._sp_outcome_test('ROLLBACK TO SAVEPOINT', 'rollback')


    def test_commit_rewritten_to_release_savepoint_in_test(self):
        return self._sp_outcome_test('RELEASE SAVEPOINT', 'commit;')


    def _sp_outcome_test(self, spPrefix, sql):
        b, f = self.protocols()
        d = Deferred()
        b.transport.deferred = d
        
        # hack in a previous savepoint
        f.filter.savepoints = ['foo']

        def check(data):
            ss = f.filter.savepoints
            self.assertFalse(ss, 'Should have deleted FrontendFilter savepoint')

            m = messages.query(spPrefix+' foo')
            self.assertEqual(data, m.serialize())

            self.assertEqual(
                b.filter.dropMessages, 'CZ',
                'backend should have been told to ignore the complete')
            return data

        d.addCallback(check)
        b.signalTest(True)
        f.messageReceived(messages.query(sql))
        return d


    def test_begin_in_test_rewritten_to_savepoint(self):
        # Support transaction-ish behavior within tests by rewriting them 
        # to savepoints. 
        b, f = self.protocols()
        d = Deferred()
        b.transport.deferred = d

        def check(data):
            ss = f.filter.savepoints
            self.assertTrue(ss, 'FrontendFilter should have a savepoint')
            m = messages.query('SAVEPOINT %s' % ss[-1])
            self.assertEqual(data, m.serialize())

            self.assertEqual(
                b.filter.dropMessages, 'CZ', 
                'backend should have been told to ignore the savepoint complete')
            return data

        d.addCallback(check)

        b.signalTest(True)
        f.messageReceived(messages.query('BEGIN;'))
        return d

    
    def test_release_savepoint_not_issued_error_in_transaction(self):
        b, f = self.protocols()
        b.setTransactionStatus('failed')
        b.transport.expectNothing()

        d = Deferred()
        f.transport.deferred = d
        
        def check(data):
            ms = FrontendFilter.transaction_aborted
            self.assertEqual(data, ''.join([m.serialize() for m in ms]))
            return data

        d.addCallback(check)
        b.signalTest(True)
        f.messageReceived(messages.query('end work;'))
        return d
        
