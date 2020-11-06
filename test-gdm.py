#!/usr/bin/env python3

import concurrent.futures
import time
import random
import datetime
import botocore
import numpy
import sys
import dbm

def getrandomfeature():
    i=random.randint(0,1000000-1)
    hexnumber="%06x"%(i)
    firstdir=hexnumber[0:2]
    seconddir=hexnumber[2:4]
    with open("data-dbm/%s/%s/%s"%(firstdir,seconddir,hexnumber),"r") as fh:
        indata=fh.read(1024)
    accountid=indata[0:indata.find(',')]
    plasticid=indata[indata.find(',')+1:]
    return(accountid,plasticid)

if 'use_ebs' in sys.argv:
    db = dbm.open("/data/dataout/features",flag='r')
else:
    db = dbm.open("/data/ephemeral0/features",flag='r')

def getItem(key):
    return(db[key])

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

db.close()
