import argparse

'''
parser = argparse.ArgumentParser(description='Demo')
parser.add_argument('--verbose', action='store_true', help='verbose flag')

args = parser.parse_args()

if args.verbose:
    print("~ Verbose!")
else:
    print("~ Not so verbose")
'''

parser = argparse.ArgumentParser(description="Routine to The Actual Report for AEGF based on the Definition Parameters")
parser.add_argument("--id", nargs=1, help="Report Definition Identifier", required=True)
parser.add_argument("--progress_id", nargs=1, help="Optional GTM CLI Progress Id", required=False)
parser.add_argument("--adhoc", nargs=1, help="Optional Adhoc Generation", required=False)

args = parser.parse_args()