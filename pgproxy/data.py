"""
Some utilities for converting data to and from python types. 

These are just minor optimizations of struct -- they skip some of the
unnecessary logic in it.
"""
import struct


_fmt_int32 = struct._compile('!I')
_fmt_int16 = struct._compile('!H')
pack_int32 = _fmt_int32.pack
unpack_int32 = _fmt_int32.unpack
unpack_int16 = _fmt_int16.unpack
unpack_int32_from = _fmt_int32.unpack_from
unpack_int16_from = _fmt_int16.unpack_from
