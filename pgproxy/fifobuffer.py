from data import *


class FIFOBuffer(object):
    """
    A class that is handy for treating a raw string as an input stream, 
    and reading different datatypes out of it. 
    """
    def __init__(self, data=''):
        self.str_buf = data
        self.pos = 0


    def append(self, data):
        """
        Adds data to the end of the buffer. 
        """
        self.str_buf += data


    def reset(self):
        """
        Resets the current read position of the buffer. 
        """
        self.pos = 0


    def get_char(self):
        """
        Gets the next character from the stream, and advances the position. 
        """
        p = self.pos
        self.pos += 1
        return self.str_buf[p]


    def __getitem__(self, i):
        """
        Gets the integer value of the ith character from the stream.
        """
        return ord(self.str_buf[i])


    def get_int16(self):
        """
        Reads a 16-bit integer from the stream (in network order) and advances
        the current position. 
        """
        p = self.pos
        self.pos += 2
        return unpack_int16_from(self.str_buf, p)[0]


    def get_int32(self):
        """
        Reads a 32-bit integer from the stream (in network order) and advances
        the current position. 
        """
        p = self.pos
        self.pos += 4
        return unpack_int32_from(self.str_buf, p)[0]


    def raw_value(self):
        """
        Gets the real string backing the buffer. 
        """
        return self.str_buf


    def __len__(self):
        return len(self.str_buf)


    def remainder(self):
        """
        Returns the portion of the backing string that is beyond the
        current position.
        """
        return self.str_buf[self.pos:]


    def truncate(self, length):
        """
        Truncates the buffer at the given length, discarding any data that comes
        after it. Returns the discarded data. 
        """
        extra = self.str_buf[length:]
        self.str_buf = self.str_buf[:length]
        return extra

