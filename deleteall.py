#!/usr/bin/env python3

import boto3

TABLE = "cerberus-test2"

table = boto3.resource('dynamodb').Table(TABLE)
scan = None

with table.batch_writer() as batch:
    count = 0
    while scan is None or 'LastEvaluatedKey' in scan:
        if scan is not None and 'LastEvaluatedKey' in scan:
            scan = table.scan(
                ProjectionExpression="accountDataLookupKey,sortQualifier",
                ExclusiveStartKey=scan['LastEvaluatedKey'],
            )
        else:
            scan = table.scan(ProjectionExpression="accountDataLookupKey,sortQualifier")

        for item in scan['Items']:
            if count % 25 == 0:
                print(count)
            batch.delete_item(Key={"accountDataLookupKey": item["accountDataLookupKey"],"sortQualifier":item["sortQualifier"]})
            count = count + 1

