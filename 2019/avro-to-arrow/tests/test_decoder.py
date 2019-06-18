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

import struct

import pytest


@pytest.fixture
def module_under_test():
    from avro_to_arrow import decoder

    return decoder


@pytest.mark.parametrize(
    "block,position,expected",
    [
        (b"\x00", 0, 0x00),
        (b"\x01", 0, 0xFF),
        (b"\xff", 0, 0xFF),
        (b"\x00\x01\x00", 1, 0xFF),
        (b"\xff\x00\xff", 1, 0x00),
    ],
)
def test_read_boolean(module_under_test, block, position, expected):
    actual_position, actual_bitmask = module_under_test.read_boolean(position, block)
    assert actual_position == position + 1
    assert actual_bitmask == expected


@pytest.mark.parametrize(
    "expected",
    [
        0.0,
        1.0,
        (2 ** 1023 * (1 + (1.0 - (2 ** -52)))),  # maximum double
        float("inf"),
        float("-inf"),
    ],
)
def test_read_double(module_under_test, expected):
    block = struct.pack("<d", expected)
    actual_position, actual_double = module_under_test.read_double(0, block)
    assert actual_position == 8
    assert actual_double == expected


def test_read_double_w_nan(module_under_test):
    block = struct.pack("<d", float("nan"))
    _, actual_double = module_under_test.read_double(0, block)
    assert actual_double != actual_double


@pytest.mark.parametrize(
    "block,position,expected_position,expected_long",
    [
        # ZigZag encoding. See:
        # https://developers.google.com/protocol-buffers/docs/encoding#varints
        (b"\x00", 0, 1, 0),
        (b"\x01", 0, 1, -1),
        (b"\x02", 0, 1, 1),
        (b"\x0f", 0, 1, -8),
        (b"\x10", 0, 1, 8),
        (b"\x7f", 0, 1, -64),
        (b"\x80\x01", 0, 2, 64),
        (b"\xff\x7f", 0, 2, -8192),
        (b"\x80\x80\x01", 0, 3, 8192),
        (b"\x81\x80\x01", 0, 3, -8193),
        (b"\x82\x80\x01", 0, 3, 8193),
        (b"\xfd\xff\xff\xff\xff\xff\xff\xff\x7f", 0, 9, -(2 ** 62 - 1)),
        (b"\xfe\xff\xff\xff\xff\xff\xff\xff\x7f", 0, 9, 2 ** 62 - 1),
        (b"\xff\xff\xff\xff\xff\xff\xff\xff\x7f", 0, 9, -2 ** 62),
        (b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01", 0, 10, 2 ** 62),
        (b"\xfd\xff\xff\xff\xff\xff\xff\xff\xff\x01", 0, 10, -(2 ** 63 - 1)),
        (b"\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01", 0, 10, 2 ** 63 - 1),
        (b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01", 0, 10, -2 ** 63),
        (b"\xff\x00\xff", 1, 2, 0),
        (b"\xff\x01\xff", 1, 2, -1),
        (b"\xff\x02\xff", 1, 2, 1),
    ],
)
def test_read_long(module_under_test, block, position, expected_position, expected_long):
    actual_position, actual_long = module_under_test.read_long(position, block)
    assert actual_position == expected_position
    assert actual_long == expected_long

