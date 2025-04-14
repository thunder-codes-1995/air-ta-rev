import argparse

from handlers.cyp.load_factor.parser import Handler

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="value should have the following format : YYYYMMDD", type=int)
    parser.add_argument("path_to_file", help="path to file to be parsed", type=str)
    args = parser.parse_args()

    handler = Handler(date=args.date, path=args.path_to_file)
    handler.parse()
