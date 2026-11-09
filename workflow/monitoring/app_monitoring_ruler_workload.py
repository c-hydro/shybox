"""Small, bounded process for testing resource monitoring; allocates 32 MiB."""
import argparse
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=float, default=10)
    args = parser.parse_args()
    if not 0 <= args.seconds <= 60:
        parser.error('--seconds must be between 0 and 60.')
    memory = bytearray(32 * 1024 * 1024)
    memory[::4096] = b'x' * (len(memory) // 4096)
    time.sleep(args.seconds)
    return len(memory)


if __name__ == '__main__':
    main()
