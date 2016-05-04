#!/usr/bin/env python
"""
s3diff - utility for determining differences between buckets.

Usage:
  s3diff.py <left-bucket> <right-bucket>
  s3diff.py [ -h | --help | --version ]

Options:
  --version             Show version.
  -h --help             Show this screen.
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
        left = keys(splitter(arguments['<left-bucket>']))
        right = keys(splitter(arguments['<right-bucket>']))

    # invalid argument handler
    except DocoptExit as exit:
        print exit.message


# splits bucket and the first / to seperate prefix
def splitter(bucket):
    divorced = bucket.split('/', 1)
    params = {'Bucket': divorced[0]}
    if len(divorced) == 2:
        params = {"Bucket": divorced[0], "Prefix": divorced[1]}
    return params


# creates list of object etags recursively starting at bucket/prefix
def keys(args):
    paginator = client('s3').get_paginator('list_objects')
    pages = paginator.paginate(**args)

    print "\nGenerating list of all e-tags for:", args['Bucket']
    etag_list = []
    # list all keys per iterated page
    for page in pages:
        for key in page['Contents']:
            etag_list.append(key['ETag'])

    return etag_list

if __name__ == "__main__":
    main()

# source: installation and usage manual
# url: https://pythonhosted.org/joblib

# source: aws guide for pagination
# url: http://boto3.readthedocs.io/en/latest/guide/paginators.html
