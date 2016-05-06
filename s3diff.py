#!/usr/bin/env python
"""
s3diff - utility that performs an e-tag lis buckets.

Usage:
  s3diff.py <left-bucket> <right-bucket>
  s3diff.py [ -h | --help | --version ]

Options:
  --version             Show version.
  -h --help             Show this screen.
"""
from boto3 import client
from docopt import docopt, DocoptExit
from multiprocessing import Process
from time import time, clock, sleep



__author__ = "Andrew Kuttor"
__credits__ = "Andrew Kuttor"
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Andrew Kuttor"
__status__ = "Development"


def main():
    try:
        arguments = docopt(__doc__, version='s3diff 1.0')
        parallel(
            left=keys(chop(arguments['<left-bucket>'])),
            right=keys(chop(arguments['<right-bucket>']))
        )
    except DocoptExit as exit:
        print exit.message


# splits bucket and the first / to seperate prefix
def chop(path):
    cleave = path.split('/', 1)
    params = {'Bucket': cleave[0]}
    if len(cleave) == 2:
        if not cleave[1].endswith("/"):
            cleave[1] = cleave[1] + "/"
        params = {"Bucket": cleave[0], "Prefix": cleave[1]}
    return params


# creates list of object etags recursively starting at bucket/prefix
def keys(args):
    print "\nGenerating list of all e-tags for:", args['Bucket']
    paginator = client('s3').get_paginator('list_objects')
    pages = paginator.paginate(**args)
    etag_list = []
    for page in pages:
        for key in page['Contents']:
            etag_list.append(key['ETag'])
    return etag_list


# executes two processes in parallel
def parallel(left, right):
    procs = []
    procs.append(Process(target=left))
    procs.append(Process(target=right))
    map(lambda x: x.start(), procs)
    map(lambda x: x.join(), procs)


# --------------------------------------------------------------------------
if __name__ == "__main__":
    main()


# source: installation and usage manual
# url: https://pythonhosted.org/joblib

# source: aws guide for pagination
# url: http://boto3.readthedocs.io/en/latest/guide/paginators.html
