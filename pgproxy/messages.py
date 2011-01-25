"""
This module is responsible for parsing and serializing messages for the postgres
frontend/backend protocol. 

The Message class handles parsing messages, and the exported functions can be 
used to construct new messages. 

See here:
http://developer.postgresql.org/pgdocs/postgres/protocol-message-formats.html

"""
from fifobuffer import FIFOBuffer
from data import pack_int32


# constant values
eight_packed = pack_int32(8)
five_packed = pack_int32(5)


class Message(object):
    """
    Base class that for frontend or backend messages. Data is parsed 
    via the consume() method, and the message can be persisted for the wire
    using the serialize() method. 

    Instances of this class will have a variable set of properties, as 
    determined by the self.type field. Messages that are not yet completely 
    parsed will also have an inconsistent set of properties. 

    """

    def __init__(self):
        self.buffer = FIFOBuffer()
        self.parsed_header = False
        self.type = ''
        self.length = -1

    
    def consume(self, data):
        """
        Parses the provided data into this message instance. Returns a 2-tuple,
        of which the first element is a boolean indicating whether or not the 
        message was completely parsed. 

        If the message is not done parsing, the second element of the return 
        tuple is not meaningful. Otherwise, it contains the unconsumed data. 
        """
        self.buffer.append(data)

        if not self.parse_header():
            # not done. 
            return False, ''

        if len(self.buffer) < self.length:
            # not done. 
            return False, ''
    
        self.extra = self.buffer.truncate(self.length)
        self.parse_body()
        return True, self.extra


    def parse_body(self):
        """
        Provides a default parse function, which just places the amount of
        data as specified by the length field into the self.data property. 

        Derived classes can provide parse_* functions named for particular 
        type codes, which can do additional processing. 
        """
        def nothing():
            pass

        self.data = self.buffer.raw_value()[self.buffer.pos:self.length]
        getattr(self, 'parse_' + self.type, nothing)()


    def parse_header(self):
        """
        Parses the header out of the buffer. Returns true if successful, 
        false if there's not enough data yet. 
        """
        if self.parsed_header:
            return True

        if len(self.buffer.remainder()) < 5:
            # Not enough data for the minimum header yet. 
            return False

        t = self.buffer.get_char()
        if ord(t) != 0:
            # this is an ordinary packet.
            self.type = t

            # add one, wire length doesn't include the type byte. 
            self.length = self.buffer.get_int32() + 1
        else:
            if not self.parse_special_header():
                return False

        self.parsed_header = True
        return True


    def parse_special_header(self):
        """
        Parses irregular message types, meaning messages that do not start
        with a single-character identifier and a length. 
        """
        self.raise_unknown()


    def raise_unknown(self):
        raise ValueError(
            'Unknown %s packet: %r' % (
                self.__class__.__name__, 
                self.buffer.raw_value()[:200]))


    def serialize(self):
        """
        Returns the data that should be written to the wire for this messag
        """
        return self.buffer.raw_value()[:self.length]


    def parseDict(self, data=None):
        """
        Parses a set of zero-delimited name, value pairs in data (or, by default,
        self.data) into a dict. 
        """
        data = data or self.data
        params = [x for x in data.split('\x00') if x]
        return dict([(k, v) for k, v in zip(params[::2], params[1::2])])



    def __str__(self):
        """
        Returns a human-readable form of the message. Dispatches to str_<type>
        functions, if they exist on the instance. 
        """
        return getattr(self, 'str_' + self.type, lambda: self.type)()



class FrontendMessage(Message):
    """
    Message type for messages coming from clients. This has methods for parsing
    and representing those messages. 
    """
    # Codes that identify certain special packets. 
    Cancel = 0x80877102
    SSLRequest = 0x80877103


    def parse_Startup(self):
        self.parameters = self.parseDict()


    def parse_Cancel(self):
        self.pid = self.buffer.get_int32()
        self.key = self.buffer.get_int32()


    def parse_C(self):
        """
        Parses a Close command.
        """
        self.kind = 'prepared' if self.buffer.get_char() == 'S' else 'portal'
        # name follows, not handled


    def parse_special_header(self):
        if len(self.buffer.remainder()) < 7:
            # Need more data, call us back later. 
            self.buffer.reset()
            return False

        self.buffer.get_char()
        self.length = self.buffer.get_int16()
        code = self.buffer.get_int32()

        if code == FrontendMessage.Cancel:
            self.type = 'Cancel'
        elif code == FrontendMessage.SSLRequest:
            self.type = 'SSLRequest'
        elif self.is_startup_code(code):
            self.type = 'Startup'
        else:
            self.raise_unknown()

        return True


    def is_startup_code(self, code):
        # Validate the major version (3) of the protocol, and ignore
        # the minor version.
        return (code >> 16) == 3 and (code & 0xffff) < 2


    def str_Q(self):
        return 'Q %s' % self.data[:-1]



class BackendMessage(Message):
    """
    Class for messages being returned from the postgres server. 
    """

    def parse_R(self):
        """
        Parses an authentication response message. The status dword
        is zero if authentication was successful. The other cases are
        not really currently handled. 
        """
        self.status = self.buffer.get_int32()
        self.success = (self.status == 0)


    def parse_Z(self):
        """
        Parses a ReadyForQuery message.
        """
        self.transaction_status = {
            'I': 'idle',
            'E': 'failed',
            'T': 'transaction',
            }.get(self.buffer.get_char())


    def parse_S(self):
        """
        Parses a parameter status message. 
        """
        self.name, self.value = self.data.split('\x00')[:2]


    def parse_E(self):
        """
        Parses an error response message. 
        """
        code = ord(self.buffer.get_char())
        self.fields = []
        if code:
            for f in self.data.split('\x00'):
                if not f:
                    continue
                self.fields.append((f[0], f[1:]))


    def str_S(self):
        """
        Formats a parameter status message. 
        """
        return 'S %s = %s' % (self.name, self.value)


    def str_C(self):
        """
        Formats a command complete message. 
        """
        return 'C[%s]' % self.data[:-1]


    def str_E(self):
        """
        Formats an error response message. 
        """
        return 'E - %r' % self.fields



def _string_message(s, t, cls):
    m = cls()
    m.consume('%s%s%s\x00' % (t, pack_int32(len(s)+5), s))
    return m


def _int_message(t, intval, cls):
    m = cls()
    m.consume('%s%s%s' % (t, eight_packed, pack_int32(intval)))
    return m


def _char_message(t, c, cls):
    m = cls()
    m.consume('%s%s%s' % (t, five_packed, c))
    return m


def query(sql):
    """
    Constructs a new Query message containing the given SQL string.
    """
    return _string_message(sql, 'Q', FrontendMessage)

    
def authenticationOk():
    """
    Constructs a new AuthenticationOk message.
    """
    return _int_message('R', 0, BackendMessage)


def readyForQuery(transactionStatus):
    """
    Constructs a new ReadyForQuery message.
    """
    return _char_message('Z', {
            'idle': 'I',
            'failed': 'E',
            'transaction': 'T',
            }[transactionStatus], BackendMessage)


def parameterStatus(name, value):
    """
    Constructs a new ParameterStatus message holding the given name and value. 
    """
    m = BackendMessage()
    l = pack_int32(len(name) + len(value) + 2 + 4)
    m.consume('S%s%s\x00%s\x00' % (l, name, value))
    return m


def startup(user):
    """
    Constructs a new Startup message. 
    """
    m = FrontendMessage()
    payload = ('\x00\x03\x00\x00' 
               'user\x00%s\x00' % user)
    m.consume(pack_int32(len(payload)+4) + payload)
    return m


def commandComplete(tag):
    """
    Constructs a new CommandComplete message. 
    """
    return _string_message(tag, 'C', BackendMessage)


def terminate():
    """
    Constructs a new Terminate message. 
    """
    m = FrontendMessage()
    m.consume('X\x00\x00\x00\x04')
    return m


def errorResponse(*fields):
    """
    Creates an ErrorResponse message. Fields should be a list of 2-tuples
    consisting of a single-byte field type and a string. 
    """
    m = BackendMessage()
    
    # convert fields to strings 
    fields = [(b, str(f)) for b, f in fields]

    # four bytes for the length, one byte plus string length plus \0 for
    # each field, one terminating \0. 
    length = pack_int32(4 + sum([1+1+len(f) for _, f in fields]) + 1)
    m.consume('E' + length)
    for b, f in fields:
        m.consume(b+f+'\x00')
    m.consume('\x00')
    return m
