#!/usr/bin/env python
"""
s3diff - utility for determining differences between buckets.

Usage:
  s3diff.py --bucket <bucket> [--prefix <prefix>]
  s3diff.py -h | --help
  s3diff.py --version

Options:
  --version             Show version.
  -h --help             Show this screen.
  -b --bucket BUCKET    Set target bucket.
  -p --prefix PREFIX    set target prefix.
"""
from boto3 import client
from docopt import docopt, DocoptExit

__author__ = "Andrew Kuttor"
__credits__ = "Andrew Kuttor"
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Andrew Kuttor"
__status__ = "Development"

def main():
    try:
        arguments = docopt(__doc__, version='s3diff 1.0')
        keys(arguments)

    # invalid argument handler
    except DocoptExit as exit:
        print exit.message


# no max_key limit key listing
def keys(args):

    # setup param logic
    params = {'Bucket': args['--bucket'], 'Prefix': args['--prefix']}
    if not args['--prefix']: del params['Prefix']

    # paginator setup
    paginator = client('s3').get_paginator('list_objects')
    pages = paginator.paginate(**params)

    # list all keys per iterated page
    for page in pages:
        for keys in page['Contents']:
            print keys['Key']


if __name__ == "__main__":
    main()
