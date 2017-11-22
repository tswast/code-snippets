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


def rotate_image(img):
    """Rotates an image 90-degrees.

    Args:
        img (str): A 3x3 image, represented by a 3-line string.

    Returns:
        str: A string representing the rotated image.
    """
    lines = img.split('\n')
    if len(lines) != 4:
        raise ValueError('Expected 3 \\n-terminated lines')

    out_cells = [[' ' for _ in range(3)] for _ in range(3)]

    for line_i, line in enumerate(lines[:3]):
        for char_i, char in enumerate(line):
            x = char_i - 1
            y = line_i - 1
            x_rot = y
            y_rot = -x
            out_cells[y_rot + 1][x_rot + 1] = char

    out_lines = [''.join(cells) for cells in out_cells]
    return '{}\n'.format('\n'.join(out_lines))


def image_seq(seq):
    """Gets an image from the sequence.

    Args:
        seq (int): An integer, which will be interpreted as an image.

    Returns:
        str: A 3x3 image, represented by a 3-line string.
    """
    cells = []

    while seq > 0:
        cells.insert(0, str(seq % 3))
        seq = seq // 3

    while len(cells) < 8:
        cells.insert(0, '0')

    cells.insert(4, ' ')
    lines = []

    for line_i in range(3):
        start_i = 3 * line_i
        end_i = 3 * (line_i + 1)
        lines.append(''.join(cells[start_i:end_i]))

    return '{}\n'.format('\n'.join(lines))


def add_to_set(img, images):
    for _ in range(4):
        if img in images:
            return
        img = rotate_image(img)
    images.add(img)


def main():
    images = set()

    for seq in range(3**8):
        img = image_seq(seq)
        add_to_set(img, images)

    print('Found {} unique images.'.format(len(images)))


if __name__ == '__main__':
    main()