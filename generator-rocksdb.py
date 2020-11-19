#!/usr/bin/env python3

#import protobuf
import pathlib
import random
import os
import base64
import time
import concurrent.futures,threading
import rocksdb
import hashlib
from rocksdbcerberus import TTLComparator
import datetime
import traceback

ttlcomparator = TTLComparator()

opts = rocksdb.Options()
opts.create_if_missing = True
opts.max_open_files = 300000
opts.comparator = ttlcomparator
opts.write_buffer_size = 67108864
opts.max_write_buffer_number = 3
opts.target_file_size_base = 67108864
opts.table_factory = rocksdb.BlockBasedTableFactory(
    filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
    block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))

db = rocksdb.DB("/data/dataout/features-rocksdb2.db", opts)

ttlcomparator.register(db)

def generate(start,end):
  try:
    print("Starting batch",start,end)
    items=[]
    batch=rocksdb.WriteBatch()
    epoch=datetime.datetime.now().timestamp()
    for i in range(start,end):
        myhash=bytes(hashlib.sha256(b'%i'%i).hexdigest(),'ascii')
        accountid=myhash[0:40]
        plasticid=int(myhash[0:9],16)%10**10
        #print(i,hexnumber,firstdir,seconddir,accountid,plasticid)
        length=random.randint(200,400000)
        ttlnum=epoch+random.randint(300,86400)
        payload=b"%f:"%(ttlnum)+os.urandom(length)
        key=b'%s#%i'%(accountid,plasticid)
        ttlcomparator.record(key,payload)
        batch.put(key,payload)
    db.write(batch)
    print("Ending batch",start,end)
  except Exception as err:
      print("Error:",err)
      print(traceback.format_exc())
      raise

batch=1000
with concurrent.futures.ThreadPoolExecutor() as executor:
    i=0
    while i<10000:
        future=executor.submit(generate,i,i+batch)
        i+=batch

#if True:
#   i=0
#    while i<10000:
#        generate(i,i+batch)
#        i+=batch
