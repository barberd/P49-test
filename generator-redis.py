#!/usr/bin/env python3

#import protobuf
import pathlib
import random
import os
import base64
import time
import concurrent.futures
import rediscluster

startup_nodes = [{"host":'cerberus-test.rnsoon.clustercfg.use1.cache.amazonaws.com','port':6379}]
#startup_nodes = [{"host":'cerberus-test-0002-001.rnsoon.0001.use1.cache.amazonaws.com','port':6379}]

rc = rediscluster.RedisCluster(startup_nodes=startup_nodes,decode_responses=True,skip_full_coverage_check=True)

def writeItem(key,payload):
    rc.set(key,payload)
    return

def generate(start,end):
  try:
    print("Starting batch",start,end)
    items=[]
    for i in range(start,end):
        hexnumber="%06x"%(i)
        firstdir=hexnumber[0:2]
        seconddir=hexnumber[2:4]
        pathlib.Path("data-redis/%s/%s"%(firstdir,seconddir)).mkdir(parents=True, exist_ok=True)
        accountid = '%040x' % random.randrange(16**40)
        plasticid = '%010i' % random.randrange(10**10)
        with open("data-redis/%s/%s/%s"%(firstdir,seconddir,hexnumber),"w") as fh:
            fh.write(accountid+","+plasticid)
        #print(i,hexnumber,firstdir,seconddir,accountid,plasticid)
        length=random.randint(200,400000)
        #payload=base64.b64encode(os.urandom(length))
        payload=os.urandom(length)
        key="%s#%s"%(accountid,plasticid)
        writeItem(key,payload)
    print("Ending batch",start,end)
  except Exception as err:
      print("Error:",err)
      raise

batch=1000
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    i=0
    while i<1000000:
        future=executor.submit(generate,i,i+batch)
        i+=batch
