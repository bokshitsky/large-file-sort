#!/usr/bin/env python3
import argparse
import random
import string


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--maxsize', help='Max size of random string', type=int)
    parser.add_argument('--count', help='Random strings count', type=int)
    return parser.parse_args()


def main():
    args = _parse_args()
    allowed_symbols = string.ascii_letters + string.digits
    for _ in range(args.count):
        line_length = random.randint(1, args.maxsize)
        print(''.join(random.choice(allowed_symbols) for __ in range(line_length)))


if __name__ == '__main__':
    main()
