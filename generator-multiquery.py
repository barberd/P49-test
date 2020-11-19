#!/usr/bin/env python3

#import protobuf
import boto3
import pathlib
import random
import os
import base64
import time
import concurrent.futures
import botocore
import hashlib
import traceback

tablename_query='cerberus-test-3'
tablename_getitem='cerberus-test-4'
ddb=boto3.client('dynamodb')

def processbatch(items,tablename=tablename_query):
    request=[{'PutRequest':{'Item':i}} for i in items]
    retries=0
    while True:
        try:
            response=ddb.batch_write_item(RequestItems={tablename:request})
            break
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] not in ['ProvisionedThroughputExceededException','ThrottlingException','RequestLimitExceeded']:
                raise
            print("Got Error:",err," retrying...")
            time.sleep(2 ** retries)
            retries +=1

    i = 0
    while 'UnprocessedItems' in response and response['UnprocessedItems']!={}:
        request=response['UnprocessedItems']
        time.sleep(max(2**i,32)+random.randint(-5,5))
        retries=0
        while True:
            try:
                response=ddb.batch_write_item(RequestItems=request)
                break
            except botocore.exceptions.ClientError as err:
                if err.response['Error']['Code'] not in ['ProvisionedThroughputExceededException','ThrottlingException','RequestLimitExceeded']:
                    raise
                print("Got Error:",err," retrying...")
                time.sleep(2 ** retries)
                retries +=1
        i+=1
    return

featurelist=[b"AT1",b"AT2",b"AT3",b"AT4",b"AT5"]

def generate(start,end):
  try:
    print("Starting batch",start,end)
    items_query=[]
    items_getitem=[]
    for i in range(start,end):
        myhash=bytes(hashlib.sha256(b'%i'%i).hexdigest(),'ascii')
        accountid=myhash[0:40]
        for att in featurelist:
            length=random.randint(200,400000)
            payload=os.urandom(length)
            compositeKey = accountid+b"#"+att
            item_getitem={'PK':{'S':compositeKey.decode("utf-8")},
                    'part-count':{'N':'001'},
                    'payload':{'B':payload}}
            item_query={'PK':{'S':accountid.decode("utf-8")},
			'SK':{'S':att.decode("utf-8")},
		    'part-count':{'N':'001'},
		    'payload':{'B':payload}}
            items_query.append(item_query)
            items_getitem.append(item_getitem)
            if len(items_query)>24:
                processbatch(items_query,tablename_query)
                items_query=[]
            if len(items_getitem)>24:
                processbatch(items_getitem,tablename_getitem)
                items_getitem=[]
    if len(items_query)>0:
        processbatch(items_query,tablename_query)
    if len(items_getitem)>0:
        processbatch(items_getitem,tablename_getitem)
    print("Ending batch",start,end)
  except Exception as err:
      print("Error:",err,traceback.print_exc())


batch=1000
with concurrent.futures.ThreadPoolExecutor() as executor:
    i=0
    while i<200000:
        future=executor.submit(generate,i,i+batch)
        i+=batch

