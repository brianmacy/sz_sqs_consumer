#! /usr/bin/env python3

import argparse
import orjson
import logging
import traceback

import importlib
import sys
import os
import time
import random
import boto3

parser = argparse.ArgumentParser()
parser.add_argument('file')
parser.add_argument('-q', '--queue', dest='queue', required=True, help='queue')
parser.add_argument('-t', '--debugTrace', dest='debugTrace', action='store_true', default=False, help='output debug trace information')
args = parser.parse_args()

sqs = boto3.client('sqs')

with open(args.file, 'r') as read_file:
    cnt = 0
    for line in read_file:
      response = sqs.send_message(QueueUrl=args.queue, MessageBody=line)
      cnt += 1
      if cnt%1000==0:
        print(f'Published {cnt} messages')

