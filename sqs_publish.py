#! /usr/bin/env python3

import argparse
import boto3
import botocore

def send_batch(sqs, batch):
  needSingleSend = False

  try:
    payload = []
    cnt = 0
    for line in batch:
      payload.append({'Id': str(cnt), 'MessageBody': line})
      cnt += 1

    response = sqs.send_message_batch(QueueUrl=args.queue, Entries=payload)
    if 'Failed' in response:
      needSingleSend = True
  except Exception as error:
    print(error)
    needSingleSend = True

  if needSingleSend:
    print(f'Falling back to single send')
    for line in batch:
      try:
        sqs.send_message(QueueUrl=args.queue, MessageBody=line)
      except botocore.exceptions.ClientError as error:
        print(error)
        if error.response['Error']['Code'] == 'InvalidParameterValue':
          print(f'Record too long: {line}')
        else:
          raise error


parser = argparse.ArgumentParser()
parser.add_argument('file')
parser.add_argument('-q', '--queue', dest='queue', required=True, help='queue')
parser.add_argument('-t', '--debugTrace', dest='debugTrace', action='store_true', default=False, help='output debug trace information')
args = parser.parse_args()

sqs = boto3.client('sqs')

with open(args.file, 'r') as read_file:
    cnt = 0
    lines_batch = []
    cnt_batch = 0
    for line in read_file:
      lines_batch.append(line)
      cnt_batch += 1

      if cnt_batch == 10:
        send_batch(sqs, lines_batch)
        lines_batch = []
        cnt_batch = 0

      cnt += 1
      if cnt%1000==0:
        print(f'Published {cnt} messages')

    if lines_batch:
      send_batch(sqs, lines_batch)

