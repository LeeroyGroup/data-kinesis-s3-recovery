"""Recovers Kinesis S3 source record backup files, queueing each file to be proessed by recover_firehose_s3_backup.py"""
import boto3
import json


def main(event, context):
    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')

    paginator = s3.get_paginator('list_objects_v2')

    if 'prefix' in event:
        pages = paginator.paginate(Bucket=event['bucket'], Prefix=event['prefix'])
#        listing = s3.list_objects_v2(Bucket=event['bucket'], Prefix=event['prefix'])
    else:
        pages = paginator.paginate(Bucket=event['bucket'])
#        listing = s3.list_objects_v2(Bucket=event['bucket'])

    totallySent = 0
    messages = []
    for page in pages:
        if page['IsTruncated']:
            print("WARNING: S3 bucket listing was truncated")

        bucket = page['Name']
        for obj in page['Contents']:
            groupId = str(len(messages) + 1)
            if not obj['Key'].endswith('/'):
                message = {
                    'Id': groupId,
                    'MessageBody': json.dumps({
                        'bucket': bucket,
                        'item': obj['Key'],
                        'kinesis_stream': event['kinesis_stream'],
                        'queue_url': event['queue_url']
                    }),
                    'MessageGroupId': bucket + "-" + groupId
                }
#                print(f"Message prepared {message}")
                messages.append(message)
                if len(messages) == 10:
#                    print(f"sending {len(messages)}!")
                    response = sqs.send_message_batch(
                        QueueUrl=event['queue_url'],
                        Entries=messages)
                    print(f"Response from sending {len(messages)} msgs: {response}")
                    totallySent += 10
                    messages = []

#    print(f"sending {len(messages)}!")
    totallySent += len(messages)
    if len(messages) > 0:
        response = sqs.send_message_batch(
            QueueUrl=event['queue_url'],
            Entries=messages)
        print(f"Response from sending  {len(messages)} msgs: {response}")
    print(f"Totally sent {totallySent} messages!")
