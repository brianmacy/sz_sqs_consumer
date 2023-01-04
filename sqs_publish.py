#! /usr/bin/env python3

import argparse
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
      try:
        response = sqs.send_message(QueueUrl=args.queue, MessageBody=line)
      except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'InvalidParameterValue':
          logger.warn(f'Record too long: {line}')
        else:
          raise error

      cnt += 1
      if cnt%1000==0:
        print(f'Published {cnt} messages')
