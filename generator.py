#!/usr/bin/env python3

#import protobuf
import boto3
import pathlib
import random
import os
import base64
import time
import concurrent.futures

tablename='cerberus-test2'
ddb=boto3.client('dynamodb')

def processbatch(items):
    request=[{'PutRequest':{'Item':i}} for i in items]
    response=ddb.batch_write_item(RequestItems={tablename:request})
    i = 0
    while 'UnprocessedItems' in response and response['UnprocessedItems']!={}:
        request=response['UnprocessedItems']
        time.sleep(max(2**i,32)+random.randint(-5,5))
        response=ddb.batch_write_item(RequestItems=request)
        i+=1
    return

def generate(start,end):
    print("Starting batch",start,end)
    items=[]
    for i in range(start,end):
        hexnumber="%06x"%(i)
        firstdir=hexnumber[0:2]
        seconddir=hexnumber[2:4]
        pathlib.Path("data/%s/%s"%(firstdir,seconddir)).mkdir(parents=True, exist_ok=True)
        accountid = '%040x' % random.randrange(16**40)
        plasticid = '%010i' % random.randrange(10**10)
        with open("data/%s/%s/%s"%(firstdir,seconddir,hexnumber),"w") as fh:
            fh.write(accountid+","+plasticid)
        #print(i,hexnumber,firstdir,seconddir,accountid,plasticid)
        length=random.randint(200,400000)
        #payload=base64.b64encode(os.urandom(length))
        payload=os.urandom(length)
        item={'accountDataLookupKey':{'S':accountid},
                'sortQualifier':{'S':"sortQualifier=plasticId#"+plasticid},
                'part-count':{'N':'001'},
                'async-aggregates-plastic':{'B':payload}}
        items.append(item)
        if len(items)>24:
            processbatch(items)
            items=[]
    if len(items)>0:
        processbatch(items)
    print("Ending batch",start,end)


batch=1000
with concurrent.futures.ThreadPoolExecutor() as executor:
    i=0
    while i<1000000:
        future=executor.submit(generate,i,i+batch)
        i+=batch

