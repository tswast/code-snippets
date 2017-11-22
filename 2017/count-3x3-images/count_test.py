# Copyright 2017 Google LLC
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

import pytest

from count import rotate_image, image_seq


@pytest.mark.parametrize('img,expected', [
    ('001\n010\n100\n', '100\n010\n001\n'),
    ('010\n010\n000\n', '000\n110\n000\n'),
])
def test_rotate_image(img, expected):
    assert rotate_image(img) == expected


@pytest.mark.parametrize('seq,expected', [
    (0, '000\n0 0\n000\n'),
    (1, '000\n0 0\n001\n'),
    (2, '000\n0 0\n002\n'),
    (3, '000\n0 0\n010\n'),
    (3**2, '000\n0 0\n100\n'),
    (3**3, '000\n0 1\n000\n'),
    (3**4, '000\n1 0\n000\n'),
    (3**5, '001\n0 0\n000\n'),
    (3**6, '010\n0 0\n000\n'),
    (3**7, '100\n0 0\n000\n'),
    (3**7 + 1, '100\n0 0\n001\n'),
    (2 * (3**7) + 1, '200\n0 0\n001\n'),
])
def test_image_seq(seq, expected):
    assert image_seq(seq) == expected
