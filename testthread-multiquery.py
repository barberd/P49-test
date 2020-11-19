#!/usr/bin/env python3

import concurrent.futures
import boto3
import time
import random
import datetime
import botocore
import numpy
import sys
import matplotlib.pyplot as pl
import threading
import queue
import hashlib

ddb = boto3.client('dynamodb',config=botocore.client.Config(max_pool_connections=50,read_timeout=10))

if 'use_1' in sys.argv:
  threads_per_query = 1
elif 'use_3' in sys.argv:
  threads_per_query = 3
elif 'use_4' in sys.argv:
  threads_per_query = 4
elif 'use_5' in sys.argv:
  threads_per_query = 5
elif 'use_6' in sys.argv:
  threads_per_query = 6
else:
  threads_per_query = 2

tablename="cerberus-test-3"

def getrandomaccountid():
    i=random.randint(0,200000-1)
    myhash=bytes(hashlib.sha256(b'%i'%i).hexdigest(),'ascii')
    accountid=myhash[0:40]
    return(accountid)

def doquery(query,q=None):
    def querythread(query):
        #time.sleep(random.randint(5,10))
        response = ddb.query(**query)
        return(response)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads_per_query+1)
    threads = []
    for i in range(threads_per_query):
        t = executor.submit(querythread,query)
        threads.append(t)
    for t in concurrent.futures.as_completed(threads):
        result = t.result()
        threads.remove(t)
        break
    #for t in threads:
    #    t.cancel()
    executor.shutdown(wait=False)
    if q is not None:
        q.put(result)
    return result

query={ 'TableName':tablename,
        'Select':'SPECIFIC_ATTRIBUTES',
        'KeyConditionExpression': "PK=:PK",
        'ExpressionAttributeValues':{
              ":PK":{'S':"placeholder"}
            },
        'ProjectionExpression':'SK,payload'
      }

featurelist=[b"AT1",b"AT2",b"AT3",b"AT4",b"AT5"]
num_features=5

querytimes=[]
querysizes=[]
i = 0
highest=0
savedquery=None
try:
    while True:
        accountid=getrandomaccountid()
        threads=[]
        threadmap={}
        results={}
        starttime=datetime.datetime.now()
        query["ExpressionAttributeValues"][":PK"]["S"]=accountid.decode('UTF-8')
        response=doquery(query)
        responsetime=(datetime.datetime.now()-starttime).total_seconds()*1000
        #print(query,"\n",len(response['Items']))
        if i!=0:
            totalqueryresponsesize=0
            for item in response['Items']:
                size=len(item['payload']['B'])
                totalqueryresponsesize+=size
            querysizes.append(totalqueryresponsesize)
            querytimes.append(responsetime)
            if responsetime>highest:
                highest=responsetime 
        if responsetime>65:
            print(responsetime,accountid)
            print()
            #time.sleep(2)
        i+=1
        if i>10000:
            break
except KeyboardInterrupt:
    pass

if True:
    print("\n",i,highest,savedquery)
    print("P50, P99, P99.99",numpy.percentile(querytimes,[50,99,99.99]))
    countbad = numpy.count_nonzero(numpy.array(querytimes)>65)
    print(countbad,len(querytimes),100-countbad/len(querytimes)*100)

    figfilename="ddb-multiquery-%i.png"%(threads_per_query)

    pl.figure()
    fig = pl.hist(querytimes,log=True,bins=30)
    pl.title('Query latency %i features Query with %i parallel threads each'%(num_features,threads_per_query))
    pl.xlabel('latency')
    pl.ylabel('Frequency')
    pl.xlim(0,200)
    pl.ylim(.90,6000)
    pl.savefig("histogram-"+figfilename)
    pl.clf()
    pl.figure()
    fig = pl.scatter(querysizes,querytimes,marker=",",s=5)
    pl.xlabel('Total Response Size')
    pl.ylabel('Response Time')
    pl.ylim(0, 150)
    pl.xlim(0, 400000*num_features)
    pl.title('Response Time to Size for %i features Query with %i parallel threads each'%(num_features,threads_per_query))
    pl.savefig("sizetime-"+figfilename)

    lf=numpy.polyfit(querysizes,querytimes,1)
    print(lf[0],lf[1],lf[1]+lf[0]*200,lf[1]+lf[0]*200000,lf[1]+lf[0]*400000)

