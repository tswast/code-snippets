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

import copy

import numpy
import numba
import pandas
import pyarrow


# Scalar types to support.
# INT64
# FLOAT64
# BOOL
#
# Later:
# NUMERIC (decimal) ??? how is this actually serialized. Let's wait.
# DATE
# TIME
# TIMESTAMP
#
# Even later:
# DATETIME - need to parse from string


def generate_avro_to_arrow_parser(avro_schema):
    """Return a parser that takes a ReadRowsResponse message and returns a
    :class:`pyarrow.Table` object.

    Args:
        avro_schema (Map):
            Avro schema in JSON format.

    Returns:
        A function that takes a message and returns a table.
    """
    gen_globals = copy.copy(globals())
    gen_locals = copy.copy(locals())

    gen_code = _generate_message_to_buffers(avro_schema) + """
def message_to_table(message):
    global message_to_buffers

    row_count = message.avro_rows.row_count
    block = message.avro_rows.serialized_binary_rows
"""

    field_indexes = list(range(len(avro_schema["fields"])))
    fields = ", ".join("field_{}".format(field_index) for field_index in field_indexes)
    gen_code += "    {} = message_to_buffers(row_count, block)\n".format(fields)

    for field_index in field_indexes:
        gen_code += "    field_{field_index}_nullmask, field_{field_index}_rows = field_{field_index}\n".format(field_index=field_index)
        gen_code += "    field_{field_index}_array = pyarrow.Array.from_buffers({pyarrow_type}, row_count, [\n".format(field_index=field_index, pyarrow_type=_generate_pyarrow_type(avro_schema["fields"][field_index]))
        gen_code += "        pyarrow.py_buffer(field_{field_index}_nullmask),\n".format(field_index=field_index)
        gen_code += "        pyarrow.py_buffer(field_{field_index}_rows),\n".format(field_index=field_index)
        gen_code += "    ])\n"

    field_arrays = ", ".join("field_{}_array".format(field_index) for field_index in field_indexes)
    field_names = ", ".join(repr(field["name"]) for field in avro_schema["fields"])
    gen_code += "    return pyarrow.Table.from_arrays([{}], names=[{}])\n".format(field_arrays, field_names)
    exec(gen_code, gen_globals, gen_locals)

    return gen_locals["message_to_table"]


def _generate_pyarrow_type(avro_field):
    # TODO: Verify first is "null", since all fields should be nullable.
    avro_type = avro_field["type"][1]

    if avro_type == "long":
        return "pyarrow.int64()"
    elif avro_type == "double":
        return "pyarrow.float64()"
    elif avro_type == "boolean":
        return "pyarrow.bool_()"
    else:
        raise NotImplementedError("Got unexpected type: {}.".format())


def _generate_populate_data_array(field_index, avro_field):
    # TODO: Verify first is "null", since all fields should be nullable.
    avro_type = avro_field["type"][1]
    lines = []

    if avro_type == "long":
        lines.append(
            "            position, field_{}_data[i] = _read_long(position, block)".format(field_index)
        )
    elif avro_type == "double":
        lines.append(
            "            position, field_{}_data[i] = _read_double(position, block)".format(field_index)
        )
    elif avro_type == "boolean":
        lines.append(
            "            position, boolmask = _read_boolean(position, block)"
        )
        lines.append(
            "            field_{field_index}_data[nullbyte] = field_{field_index}_data[nullbyte] | (boolmask & nullbit)".format(field_index=field_index)
        )
    else:
        raise NotImplementedError("Got unexpected type: {}.".format())
    return "\n".join(lines)


def _generate_data_array(field_index, avro_field):
    # TODO: Verify first is "null", since all fields should be nullable.
    avro_type = avro_field["type"][1]

    if avro_type == "long":
        constructor = "numpy.empty(row_count, dtype=numpy.int64)"
    elif avro_type == "double":
        constructor = "numpy.empty(row_count, dtype=numpy.float64)"
    elif avro_type == "boolean":
        constructor = "_make_bitarray(row_count)"
    else:
        raise NotImplementedError("Got unexpected type: {}.".format())
    return "    field_{}_data = {}".format(field_index, constructor)


def _generate_message_to_buffers(avro_schema):
    gen_lines = ["""
@numba.jit(nopython=True, nogil=True)
def message_to_buffers(row_count, block):  #, avro_schema):
    '''Parse all rows in a stream block.

    Args:
        block ( \
            ~google.cloud.bigquery_storage_v1beta1.types.ReadRowsResponse \
        ):
            A block containing Avro bytes to parse into rows.
        avro_schema (fastavro.schema):
            A parsed Avro schema, used to deserialized the bytes in the
            block.

    Returns:
        Iterable[Mapping]:
            A sequence of rows, represented as dictionaries.
    '''
    position = 0
    nullbit = numba.uint8(0)
"""]

    # Each column needs a nullmask and a data array.
    for field_index, field in enumerate(avro_schema["fields"]):
        gen_lines.append("    field_{}_nullmask = _make_bitarray(row_count)".format(field_index))
        gen_lines.append(_generate_data_array(field_index, field))

    gen_lines.append("""
    for i in range(row_count):
        nullbit = _rotate_nullbit(nullbit)
        nullbyte = i // 8
""")

    for field_index, field in enumerate(avro_schema["fields"]):
        gen_lines.append("""
        position, union_type = _read_long(position, block)
        if union_type != 0:
            field_{field_index}_nullmask[nullbyte] = field_{field_index}_nullmask[nullbyte] | nullbit
""".format(field_index=field_index))
        gen_lines.append(_generate_populate_data_array(field_index, field))

    gen_lines.append("""
    return (
""")

    for field_index in range(len(avro_schema["fields"])):
        gen_lines.append(
            "        (field_{field_index}_nullmask, field_{field_index}_data),".format(field_index=field_index)
        )
    gen_lines.append("    )")
    return "\n".join(gen_lines)



@numba.jit(nopython=True, nogil=True)
def _copy_bytes(input_bytes, input_start, output_bytes, output_start, strlen):
    input_pos = input_start
    output_pos = output_start
    input_end = input_start + strlen
    while input_pos < input_end:
        output_bytes[output_pos] = input_bytes[input_pos]
        input_pos += 1
        output_pos += 1


@numba.jit(nopython=True, nogil=True)
def _read_boolean(position, block):
    """Read a single byte whose value is either 0 (false) or 1 (true).

    Returns:
        Tuple[int, numba.uint8]:
            (new position, boolean)

    """
    # We store bool as a bit array. Return 0xff so that we can bitwise AND with
    # the mask that says which bit to write to.
    value = numba.uint8(0xff if block[position] != 0 else 0)
    return (position + 1, value)


@numba.jit(nopython=True, nogil=True)
def _read_bytes(position, block):
    position, strlen = _read_long(position, block)
    value = numpy.empty(strlen, dtype=numpy.uint8)
    for i in range(strlen):
        value[i] = block[position + i]
    return (position + strlen, value)


@numba.jit(nopython=True, nogil=True)
def _read_double(position, block):
    """A double is written as 8 bytes.

    Returns:
        Tuple[numba.int, numba.float64]:
            (new position, double precision floating point)
    """
    # Temporarily use an integer data type for bit shifting purposes. Encoded
    # as little-endian IEEE 754 floating point.
    value = numpy.uint64(block[position])
    value = (value
            | (numpy.uint64(block[position + 1]) << 8)
            | (numpy.uint64(block[position + 2]) << 16)
            | (numpy.uint64(block[position + 3]) << 24)
            | (numpy.uint64(block[position + 4]) << 32)
            | (numpy.uint64(block[position + 5]) << 40)
            | (numpy.uint64(block[position + 6]) << 48)
            | (numpy.uint64(block[position + 7]) << 56))
    return (position + 8, numpy.uint64(value).view(numpy.float64))


@numba.jit(nopython=True, nogil=True)
def _read_long(position, block):
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

    return (position + 1, (n >> 1) ^ -(n & 1))


@numba.jit(nopython=True, nogil=True)
def _make_bitarray(row_count):  #, avro_schema):
    extra_byte = 0
    if (row_count % 8) != 0:
        extra_byte = 1
    return numpy.zeros(row_count // 8 + extra_byte, dtype=numpy.uint8)


@numba.jit(nopython=True, nogil=True)
def _rotate_nullbit(nullbit):
    # TODO: Arrow assumes little endian. Detect big endian machines and modify
    #       rotation direction.
    nullbit = (nullbit << 1) & 255

    # Have we looped?
    if nullbit == 0:
        return numba.uint8(1)

    return nullbit
