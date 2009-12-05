from corefilter import FilterTest
from pgproxy.filters import BackendFilter
from pgproxy.proxy import PostgresClientProtocol
from pgproxy import messages



class BackendFilterTests(FilterTest):

    def test_saving_authmessage(self):
        p = self.backend()
        m = messages.authenticationOk()
        p.messageReceived(m)
        self.assertEqual(p.authenticationResponse, [m])
        self.assertFalse(p.authenticationComplete)
        

    def test_auth_message_types(self):
        p = self.backend()
        self.receiveAuth(p)
        self.assertEqual(len(p.authenticationResponse), 3)
        self.assertTrue(p.authenticationComplete)

                
    def test_extra_auth_messages_raise_errors(self):
        p = self.backend()
        p.messageReceived(messages.readyForQuery('idle'))
        
        try:
            p.messageReceived(messages.authenticationOk())
        except AssertionError, e:
            self.assertEqual(
                e.message, 
                'Adding auth message, but authentication complete')
        else:
            self.fail('Should have failed')


    def test_subsequent_sets_override_original_auth_parameter_status(self):
        p = self.backend()
        a = messages.authenticationOk()
        f = messages.parameterStatus('foo', 'bar')
        b = messages.parameterStatus('baz', 'goo')
        z = messages.readyForQuery('idle')
        p.messageReceived(a)
        p.messageReceived(f)
        p.messageReceived(b)
        p.messageReceived(z)
        self.assertEqual(p.authenticationResponse, [a, f, b, z])

        x = messages.parameterStatus('foo', 'bar2')
        p.messageReceived(x)
        self.assertEqual(p.authenticationResponse, [a, x, b, z])


    def test_transaction_status_tracked(self):
        p = self.backend()
        self.assertEqual(None, p.transactionStatus)
        z = messages.readyForQuery('idle')
        p.messageReceived(z)
        self.assertEqual('idle', p.transactionStatus)


    def test_ignoreMessages(self):
        b, f = self.protocols()
        b.ignoreMessages('CZ')
        c = messages.commandComplete('SAVEPOINT')
        z = messages.readyForQuery('idle')
        
        f.transport.expectNothing()
        b.messageReceived(c)
        b.messageReceived(z)
