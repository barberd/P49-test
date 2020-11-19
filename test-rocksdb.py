#!/usr/bin/env python3

import concurrent.futures
import time
import random
import datetime
import botocore
import numpy
import sys
import hashlib
import rocksdb


opts = rocksdb.Options()
opts.create_if_missing = False
#opts.max_open_files = 300000
opts.max_open_files = 1000
opts.write_buffer_size = 67108864
opts.max_write_buffer_number = 3
opts.target_file_size_base = 67108864

#opts.table_factory = rocksdb.BlockBasedTableFactory(
#    filter_policy=rocksdb.BloomFilterPolicy(10),
#    block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
#    block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))

db = rocksdb.DB("/data/dataout/features-rocksdb.db", opts, read_only=True)

def getrandomfeature():
    i=random.randint(0,1000000-1)
    myhash=bytes(hashlib.sha256(b'%i'%i).hexdigest(),'ascii')
    accountid=myhash[0:40]
    plasticid=int(myhash[0:9],16)%10**10
    return(accountid,plasticid)

def getItem(key):
    return(db.get(key))

querystart=datetime.datetime.now()
querytimes=[]
querysizes=[]
i = 0
highest=0
savedquery=None
try:
    while True:
        (accountid,plasticid)=getrandomfeature()
        print(accountid,plasticid)
        key=b"%s#%i"%(accountid,plasticid)
        starttime=datetime.datetime.now()
        response=getItem(key)
        #if response is None:
        #    continue
        responsetime=(datetime.datetime.now()-starttime).total_seconds()*1000
        print("Query time:",responsetime)
        if i!=0:
            querytimes.append(responsetime)
            querysizes.append(len(response))
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

    lf=numpy.polyfit(querysizes,querytimes,1)
    print(lf[0],lf[1],lf[1]+lf[0]*200,lf[1]+lf[0]*200000,lf[1]+lf[0]*400000)
