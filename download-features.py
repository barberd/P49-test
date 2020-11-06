#!/usr/bin/env python3

import boto3
import math
import concurrent.futures
import threading
import datetime

s3 = boto3.client('s3')

minchunksize=5*2**20;
bucket="donbarb-cerberus"
key="features"

response=s3.head_object(Bucket=bucket,Key=key)
size=response['ContentLength']

chunks=10000
chunksize=math.ceil(size/chunks)

if chunksize<minchunksize:
    chunks=math.ceil(size/minchunksize)
    chunksize=minchunksize;

print(chunksize,chunks)

fh=open("/data/dataout/features2","wb")
filelock = threading.Lock()

def getchunk(chunknum):
    try:
        print("Starting",chunknum)
        start=chunknum*chunksize
        if chunknum==chunks-1:
            end=size
        else:
            end=(chunknum+1)*chunksize-1
        myrange="bytes=%i-%i"%(start,end)
        print("Range:",chunknum,myrange)
        response=s3.get_object(Bucket=bucket,Key=key,Range=myrange)
        data=response['Body'].read()
        with filelock:
            print("Writing",chunknum)
            fh.seek(chunknum*chunksize)
            fh.write(data)
        print("Ending",chunknum)
    except Exception as err:
        print("Error:",err)
        raise
    return

start=datetime.datetime.now()
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    for i in range(0,chunks):
        t = executor.submit(getchunk,i)
#for i in range(0,chunks+1):
#    getchunk(i)
totaltime=(datetime.datetime.now()-start).total_seconds()
fh.close()
print("Total Time (sec):",totaltime)
print("Speed (MB/s):",size/totaltime/2**20)
