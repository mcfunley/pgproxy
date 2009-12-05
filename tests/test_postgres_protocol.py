from twisted.trial import unittest
from pgproxy.proxy import PostgresClientProtocol



class PostgresProtocolTests(unittest.TestCase):

    def test_attaching_clients(self):
        p = PostgresClientProtocol()
        c1, c2 = object(), object()

        self.assertEqual(p.currentClient(), None)

        p.attachClient(c1)
        self.assertEqual(p.currentClient(), c1)

        p.attachClient(c2)
        self.assertEqual(p.currentClient(), c2)

        p.detachClient(c2)
        self.assertEqual(p.currentClient(), c1)


    def test_activate_client(self):
        p = PostgresClientProtocol()
        c1, c2 = object(), object()
        
        p.attachClient(c1)
        p.attachClient(c2)
        self.assertEqual(p.currentClient(), c2)
        p.activateClient(c1)
        self.assertEqual(p.currentClient(), c1)


