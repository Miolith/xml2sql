import argparse


def generate_args():
    parser = argparse.ArgumentParser(
        description='Python program to transform XML into relational database (SQLite)'
    )

    parser.add_argument('-f', '--files',
            nargs='+',
            )

    parser.add_argument('--config', '-c',
            default=''
            )

    parser.add_argument('-o', '--output',
            default="output.db"
            )

    return parser.parse_args()
