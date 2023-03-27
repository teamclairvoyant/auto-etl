from argparse import ArgumentParser

from app.services.query_builder import QueryBuilder
from app.utils import is_valid_file

if __name__ == "__main__":
    parser = ArgumentParser(description="ikjMatrix multiplication")
    parser.add_argument("-f", "--file", dest="meta_file", required=True,
                        help="absolute path of the metadata file", metavar="FILE")

    args = parser.parse_args()

    # checkng if file path is valid
    if is_valid_file(args.meta_file):
        QueryBuilder(args.meta_file).build()
