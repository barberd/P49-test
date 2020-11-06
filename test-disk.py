#!/usr/bin/env python3

import concurrent.futures
import time
import random
import datetime
import botocore
import numpy
import sys

def getrandomfeature():
    i=random.randint(0,1000000-1)
    hexnumber="%06x"%(i)
    firstdir=hexnumber[0:2]
    seconddir=hexnumber[2:4]
    with open("data-disk/%s/%s/%s"%(firstdir,seconddir,hexnumber),"r") as fh:
        indata=fh.read(1024)
    accountid=indata[0:indata.find(',')]
    plasticid=indata[indata.find(',')+1:]
    return(accountid,plasticid)

if 'use_ebs' in sys.argv:
    pathprefix = "/data/dataout/diskstore"
else:
    pathprefix = "/data/ephemeral1"

def getItem(key):
    safe_filename="".join([c for c in key if c.isalpha() or c.isdigit() or c==' ']).rstrip()
    s1=safe_filename[0:2]
    s2=safe_filename[2:4]
    s3=safe_filename[4:6]
    path="%s/%s/%s/%s"%(pathprefix,s1,s2,s3)
    with open("%s/%s"%(path,safe_filename),"rb") as fh:
        payload=fh.read()
    return(payload)

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

