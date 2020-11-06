#!/usr/bin/env python3

import concurrent.futures
import boto3
import time
import random
import datetime
import botocore
import numpy
import sys

if 'use_dax' in sys.argv:
    import amazondax
    ddb = amazondax.AmazonDaxClient(endpoint_url='cerberus-test.rnsoon.clustercfg.dax.use1.cache.amazonaws.com:8111')
else:
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

tablename="cerberus-test2"

def getrandomfeature():
    i=random.randint(0,1000000-1)
    hexnumber="%06x"%(i)
    firstdir=hexnumber[0:2]
    seconddir=hexnumber[2:4]
    with open("data/%s/%s/%s"%(firstdir,seconddir,hexnumber),"r") as fh:
        indata=fh.read(1024)
    accountid=indata[0:indata.find(',')]
    plasticid=indata[indata.find(',')+1:]
    return(accountid,plasticid)

def doquery(query):
    def querythread(query):
        #time.sleep(random.randint(5,10))
        response = ddb.get_item(**query)
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
    return result

query={ 'TableName':tablename,
        'Key':{
              'accountDataLookupKey':{'S':"placeholder"},
              'sortQualifier':{'S':'placeholder'}
            },
        'AttributesToGet':['async-aggregates-plastic']
      }

querytimes=[]
i = 0
highest=0
savedquery=None
try:
    while True:
        (accountid,plasticid)=getrandomfeature()
        print(accountid,plasticid)
        query["Key"]["accountDataLookupKey"]["S"]=accountid
        query["Key"]["sortQualifier"]["S"]="sortQualifier=plasticId#"+plasticid
        starttime=datetime.datetime.now()
        response=doquery(query) 
        responsetime=(datetime.datetime.now()-starttime).total_seconds()*1000
        print("Query time:",responsetime)
        if i!=0:
            querytimes.append(responsetime)
            if responsetime>highest:
                highest=responsetime 
                savedquery=query
        if responsetime>65:
            print(responsetime,query)
            #time.sleep(2)
        i+=1
        if i>50000:
            break
except KeyboardInterrupt:
    pass

if True:
    print("\n",i,highest,savedquery)
    print("P50, P99, P99.99",numpy.percentile(querytimes,[50,99,99.99]))
    countbad = numpy.count_nonzero(numpy.array(querytimes)>65)
    print(countbad,len(querytimes),100-countbad/len(querytimes)*100)

