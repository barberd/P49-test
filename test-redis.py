#!/usr/bin/env python3

import concurrent.futures
import time
import random
import datetime
import botocore
import numpy
import sys
import rediscluster

startup_nodes = [{"host":'cerberus-test.rnsoon.clustercfg.use1.cache.amazonaws.com','port':6379}]

rc = rediscluster.RedisCluster(startup_nodes=startup_nodes,decode_responses=False,skip_full_coverage_check=True)


def getrandomfeature():
    i=random.randint(0,1000000-1)
    hexnumber="%06x"%(i)
    firstdir=hexnumber[0:2]
    seconddir=hexnumber[2:4]
    with open("data-redis/%s/%s/%s"%(firstdir,seconddir,hexnumber),"r") as fh:
        indata=fh.read(1024)
    accountid=indata[0:indata.find(',')]
    plasticid=indata[indata.find(',')+1:]
    return(accountid,plasticid)

def getItem(key):
    return(rc.get(key))

querystart=datetime.datetime.now()
querytimes=[]
i = 0
highest=0
savedquery=None
try:
    while True:
        (accountid,plasticid)=getrandomfeature()
        print(accountid,plasticid)
        key="%s#%s"%(accountid,plasticid)
        starttime=datetime.datetime.now()
        response=getItem(key)
        responsetime=(datetime.datetime.now()-starttime).total_seconds()*1000
        print("Query time:",responsetime)
        if i!=0:
            querytimes.append(responsetime)
            if responsetime>highest:
                highest=responsetime 
        if responsetime>65:
            print("Slow:",key,responsetime)
            #time.sleep(2)
        i+=1
        if i>10000:
            break
except KeyboardInterrupt:
    pass

allqueriestime=(datetime.datetime.now()-querystart).total_seconds()
if True:
    print("\n",i,highest,savedquery)
    print("P50, P99, P99.99",numpy.percentile(querytimes,[50,99,99.99]))
    countbad = numpy.count_nonzero(numpy.array(querytimes)>65)
    print(countbad,len(querytimes),100-countbad/len(querytimes)*100)
    print("TPS:",len(querytimes)/allqueriestime)
