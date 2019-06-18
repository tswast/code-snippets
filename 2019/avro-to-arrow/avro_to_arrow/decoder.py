# Copyright 2019 The Avro-to-Arrow Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Private module."""

import numba
import numpy


@numba.jit(nopython=True, nogil=True)
def read_boolean(position, block):
    """Read a single byte whose value is either 0 (false) or 1 (true).

    Returns:
        Tuple[int, numba.uint8]:
            (new position, boolean)

    """
    # We store bool as a bit array. Return 0xff so that we can bitwise AND with
    # the mask that says which bit to write to.
    value = numba.uint8(0xFF if block[position] != 0 else 0)
    return (position + 1, value)


@numba.jit(nopython=True, nogil=True)
def read_double(position, block):
    """A double is written as 8 bytes.

    Returns:
        Tuple[numba.int, numba.float64]:
            (new position, double precision floating point)
    """
    # Temporarily use an integer data type for bit shifting purposes. Encoded
    # as little-endian IEEE 754 floating point.
    value = numpy.uint64(block[position])
    value = numpy.uint64(
        value
        | (numpy.uint64(block[position + 1]) << 8)
        | (numpy.uint64(block[position + 2]) << 16)
        | (numpy.uint64(block[position + 3]) << 24)
        | (numpy.uint64(block[position + 4]) << 32)
        | (numpy.uint64(block[position + 5]) << 40)
        | (numpy.uint64(block[position + 6]) << 48)
        | (numpy.uint64(block[position + 7]) << 56)
    )
    return (position + 8, value.view(numpy.float64))


# @numba.jit(nopython=True, nogil=True)
def read_long(position, block):
    """Read an int64 using variable-length, zig-zag coding.

    Returns:
        Tuple[int, int]:
            (new position, long integer)
    """
    b = block[position]
    n = b & 0x7F
    shift = 7

    while (b & 0x80) != 0:
        position += 1
        b = block[position]
        n |= (b & 0x7F) << shift
        shift += 7

    datum = numpy.int64((n >> 1) ^ -(n & 1))
    return (position + 1, datum)
