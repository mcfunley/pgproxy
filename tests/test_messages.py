from twisted.trial import unittest
from pgproxy.messages import FrontendMessage, BackendMessage
from pgproxy import messages
from pgproxy.data import pack_int32



class MessageTests(unittest.TestCase):

    def test_sub_header_length(self):
        m = FrontendMessage()
        done, extra = m.consume('R')
        self.assertFalse(done)
        self.assertFalse(extra)
        

    def test_not_enough_data(self):
        m = FrontendMessage()
        done, extra = m.consume('R\x00\x00\x00\x07')
        self.assertEqual(m.length, 8)
        self.assertFalse(done)
        self.assertFalse(extra)


    def test_normal_generic_message(self):
        m = FrontendMessage()
        done, extra = m.consume('Q\x00\x00\x00\x05N')
        self.assertTrue(done)
        self.assertEqual(m.data, 'N')
        self.assertEqual(m.length, 6)
        self.assertEqual(m.type, 'Q')
        self.assertFalse(extra)
        

    def test_normal_generic_message_with_extra(self):
        m = FrontendMessage()
        done, extra = m.consume('Q\x00\x00\x00\x05Nfoo')
        self.assertTrue(done)
        self.assertEqual(extra, 'foo')
        self.assertEqual(m.length, 6)
        self.assertEqual(m.type, 'Q')
        self.assertEqual(m.data, 'N')
        

    def test_separate_packets(self):
        m = FrontendMessage()
        done, _ = m.consume('Q\x00')
        self.assertFalse(done)
        done, extra = m.consume('\x00\x00\x05Nfoo')
        self.assertEqual(extra, 'foo')
        self.assertEqual(m.length, 6)
        self.assertEqual(m.type, 'Q')
        self.assertEqual(m.data, 'N')


    def test_Startup(self):
        msg = ('\x00\x00\x00\x26'   # length
               '\x00\x03\x00\x00'   # protocol version

               # fields
               'user\x00postgres\x00'
               'database\x00master\x00'
               
               # extra data
               'foobar baz \x00 goo')

        m = FrontendMessage()
        done, extra = m.consume(msg)
        self.assertTrue(done)
        self.assertEqual(extra, 'foobar baz \x00 goo')
        self.assertEqual(m.length, 0x26)
        self.assertEqual(m.type, 'Startup')
        self.assertEqual(
            m.parameters, 
            {'user': 'postgres', 'database': 'master'})


    def test_CancelRequest(self):
        msg = ('\x00\x00\x00\x10'
               '\x80\x87\x71\x02'
               '\xde\xad\xbe\xef'
               '\xfe\xed\xbe\xef'
               'extra')
        m = FrontendMessage()
        self.assertEqual((True, 'extra'), m.consume(msg))
        self.assertEqual(m.type, 'Cancel')
        self.assertEqual(m.length, 16)
        self.assertEqual(m.pid, 0xdeadbeef)
        self.assertEqual(m.key, 0xfeedbeef)


    def test_SSLRequest(self):
        m = FrontendMessage()
        done, _ = m.consume('\x00\x00\x00\x08\x80\x87\x71\x03')
        self.assertTrue(done)
        self.assertEqual(m.type, 'SSLRequest')


    def test_unknown_specials(self):
        self.assertUnknown('\x00\x00\x00\x08\x00\x00\x00\x01')


    def assertUnknown(self, msg):
        m = FrontendMessage()
        try:
            m.consume(msg)
        except ValueError, e:
            self.assertTrue(e.message.startswith('Unknown '))
        else:
            self.fail('Should have gotten a ValueError')


    def test_bad_startup_version(self):
        msg = ('\x00\x00\x00\x26'   
               '\x00\x07\x00\x00'   
               'user\x00postgres\x00'
               'database\x00master\x00')
        self.assertUnknown(msg)


    def test_serialize(self):
        # test a special message
        msg = ('\x00\x00\x00\x26' 
               '\x00\x03\x00\x00' 
               'user\x00postgres\x00'
               'database\x00master\x00'
               'foobar baz \x00 goo')
        m = FrontendMessage()
        m.consume(msg)
        self.assertEqual(
            m.serialize(),
            '\x00\x00\x00\x26' 
            '\x00\x03\x00\x00' 
            'user\x00postgres\x00'
            'database\x00master\x00')

        # test a normal message
        m = FrontendMessage()
        m.consume('S\x00\x00\x00\x05NX')
        self.assertEqual(m.serialize(), 'S\x00\x00\x00\x05N')


    def _msg(self, msg, cls):
        m = cls()
        done, _ = m.consume(msg)
        self.assertTrue(done)
        return m


    def backend(self, msg):
        return self._msg(msg, BackendMessage)


    def frontend(self, msg):
        return self._msg(msg, FrontendMessage)


    def test_AuthenticationResponse(self):
        m = self.backend('R\x00\x00\x00\x08\x00\x00\x00\x00')
        self.assertTrue(m.success)

        # AuthenicationSSPI
        m = self.backend('R\x00\x00\x00\x08\x00\x00\x00\x09')
        self.assertFalse(m.success)


    def test_Close(self):
        m = self.frontend('C\x00\x00\x00\x05S')
        self.assertEqual(m.kind, 'prepared')


    def test_query(self):
        m = messages.query('SELECT 1')
        self.assertEqual(m.type, 'Q')
        self.assertEqual(
            m.serialize(),
            'Q\x00\x00\x00\x0dSELECT 1\x00')


    def test_ReadyForQuery(self):
        m = self.backend('Z\x00\x00\x00\x05I')
        self.assertEqual(m.transaction_status, 'idle')


    def test_default_str(self):
        # AFAIK this is not a real message type.
        m = self.backend('O\x00\x00\x00\x05I')
        self.assertEqual(str(m), 'O')
        
        
    def test_authenticationOk(self):
        m = messages.authenticationOk()
        self.assertEqual(m.serialize(), 'R\x00\x00\x00\x08\x00\x00\x00\x00')


    def test_readyForQuery(self):
        def check(m, c):
            self.assertEqual(m.serialize(), 'Z\x00\x00\x00\x05'+c)
        check(messages.readyForQuery('idle'), 'I')
        check(messages.readyForQuery('transaction'), 'T')
        check(messages.readyForQuery('failed'), 'E')
        self.assertRaises(KeyError, messages.readyForQuery, 'foo')


    def test_parameterStatus(self):
        self.assertEqual(
            messages.parameterStatus('foo', 'bar').serialize(),
            'S%sfoo\x00bar\x00' % pack_int32(12))


    def test_commandComplete(self):
        self.assertEqual(
            messages.commandComplete('DELETE 2').serialize(),
            'C\x00\x00\x00\x0dDELETE 2\x00')


    def test_parse_parameterStatus(self):
        m = messages.parameterStatus('foo', 'bar')
        self.assertEqual((m.name, m.value), ('foo', 'bar'))
        

    def test_str_parameterStatus(self):
        m = messages.parameterStatus('foo', 'bar')
        self.assertEqual('S foo = bar', str(m))


    def test_errorResponse(self):
        m = messages.errorResponse(('a', 32), ('F', 'postgres.c'))
        self.assertEqual(
            m.serialize(),
            'E\x00\x00\x00\x15'
            'a32\x00'
            'Fpostgres.c\x00\x00')
        self.assertEqual(m.fields, [('a', '32'), ('F', 'postgres.c')])


    def test_errorResponse_no_fields(self):
        m = messages.errorResponse()
        self.assertEqual(m.serialize(), 'E\x00\x00\x00\x05\x00')
        self.assertEqual(m.fields, [])

