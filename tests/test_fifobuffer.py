from twisted.trial import unittest
from pgproxy.fifobuffer import FIFOBuffer



class FIFOBufferTests(unittest.TestCase):

    def test_reset(self):
        b = FIFOBuffer('foo')
        self.assertEqual(b.get_char(), 'f')
        b.reset()
        self.assertEqual(b.get_char(), 'f')

        
    def test_getitem(self):
        b = FIFOBuffer('\xfe\xff')
        self.assertEqual(b[1], 0xff)
        self.assertEqual(b.pos, 0)

        
    def test_get_int16(self):
        b = FIFOBuffer('\x36\xf8\x00\x35')
        self.assertEqual(b.get_int16(), 0x36f8)
        self.assertEqual(b.get_int16(), 0x35)


    def test_get_int32(self):
        b = FIFOBuffer('\xde\xad\xbe\xef')
        self.assertEqual(b.get_int32(), 0xdeadbeef)

        
    def test_append(self):
        b = FIFOBuffer('foo')
        b.append('bar')
        self.assertEqual(b.raw_value(), 'foobar')


    def test_remainder(self):
        b = FIFOBuffer('foobar')
        b.pos += 3
        self.assertEqual(b.remainder(), 'bar')
