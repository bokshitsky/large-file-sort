#!/usr/bin/env python3
import argparse
import itertools
import os
import sys
import tempfile
import time


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--max_chunk_size', help='Max size of chunk (in lines) loaded to memory', default=10000,
                        type=int)
    return parser.parse_args()


def _get_chunk_filename(index):
    return f'{index}_{time.time_ns()})'


def _prepare_initial_sorted_chunks(temp_dir, max_chunk_size):
    for chunk_index in itertools.count(start=0):
        chunk = sorted(itertools.islice(sys.stdin, max_chunk_size))
        if not chunk:
            return chunk_index

        with open(os.path.join(temp_dir, _get_chunk_filename(chunk_index)), 'w') as chunk_file:
            chunk_file.writelines(chunk)


def _iterate_over_chunk_pairs(temp_dir):
    while True:
        files = os.scandir(temp_dir)
        pair = tuple(itertools.islice(files, 2))
        if len(pair) != 2:
            break
        yield pair


def _sorted_lines(lines_iterator_1, lines_iterator_2):
    line1 = next(lines_iterator_1, None)
    line2 = next(lines_iterator_2, None)
    while True:
        if line1 is None and line2 is None:
            next_line = None
        elif line1 is not None and (line2 is None or line1 <= line2):
            next_line = line1
            line1 = next(lines_iterator_1, None)
        else:
            next_line = line2
            line2 = next(lines_iterator_2, None)

        if next_line is None:
            break
        yield next_line


def _merge_chunks(temp_dir, last_chunk_index):
    for chunk_index, (file1, file2) in enumerate(_iterate_over_chunk_pairs(temp_dir), start=last_chunk_index + 1):
        with open(file1.path, 'r') as input_chunk_1, open(file2.path, 'r') as input_chunk_2, \
          open(os.path.join(temp_dir, _get_chunk_filename(chunk_index)), 'w') as output_chunk:
            for line in _sorted_lines(iter(input_chunk_1), iter(input_chunk_2)):
                output_chunk.write(line)

        os.remove(file1.path)
        os.remove(file2.path)


def _send_last_file_to_stdout(temp_dir):
    last_file = next(iter(os.scandir(temp_dir)))
    with open(last_file.path, 'r') as sorted_lines_file:
        for line in sorted_lines_file:
            sys.stdout.write(line)
    os.remove(last_file)


def main():
    args = _parse_args()
    temp_dir = tempfile.mkdtemp('_boksh_filesort', str(time.time_ns()))
    last_chunk_index = _prepare_initial_sorted_chunks(temp_dir, args.max_chunk_size)
    _merge_chunks(temp_dir, last_chunk_index)
    _send_last_file_to_stdout(temp_dir)


if __name__ == '__main__':
    main()
