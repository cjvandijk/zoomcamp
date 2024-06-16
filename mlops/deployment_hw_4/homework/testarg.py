import argparse

parser = argparse.ArgumentParser(description='Enter desired year and month of NY taxi data')

parser.add_argument('--year', action="store", dest='year', default='2023')
parser.add_argument('--month', action="store", dest='month', default='03')

args = parser.parse_args()

print (args.year, args.month)