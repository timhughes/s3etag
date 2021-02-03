#!/usr/bin/env python
from __future__ import print_function

import hashlib
import os
import sys
from functools import partial
from boto3.s3.transfer import TransferConfig


class S3Etag:

    def __init__(self, transfer_config=TransferConfig()):
        self.transfer_config = transfer_config

    def calculate_etag(self, file_path):

        file_size = os.path.getsize(file_path)
        if file_size <= self.transfer_config.multipart_threshold:
            with open(file_path,'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        else:
            digests = []
            with open(file_path,'rb') as f:
                for chunk in iter(partial(f.read, self.transfer_config.multipart_chunksize), b''):
                    digests.append(hashlib.md5(chunk).digest())
            sum = hashlib.md5(b''.join(digests)).hexdigest()
            return sum + "-" + str(len(digests))

if __name__ == "__main__":
    e = S3Etag()
    files = sys.argv[1:]
    for f in files:
        tag = e.calculate_etag(f)
        print(f, tag)
