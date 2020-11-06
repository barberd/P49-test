#!/usr/bin/env python3

#import protobuf
import pathlib
import random
import os
import base64
import time
import concurrent.futures

def writeItem(key,payload):
    safe_filename="".join([c for c in key if c.isalpha() or c.isdigit() or c==' ']).rstrip()
    s1=safe_filename[0:2]
    s2=safe_filename[2:4]
    s3=safe_filename[4:6]
    path="/data/dataout/%s/%s/%s"%(s1,s2,s3)
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    with open("%s/%s"%(path,safe_filename),"wb") as fh:
        fh.write(payload)
    return

def generate(start,end):
  try:
    print("Starting batch",start,end)
    items=[]
    for i in range(start,end):
        hexnumber="%06x"%(i)
        firstdir=hexnumber[0:2]
        seconddir=hexnumber[2:4]
        pathlib.Path("data-disk/%s/%s"%(firstdir,seconddir)).mkdir(parents=True, exist_ok=True)
        accountid = '%040x' % random.randrange(16**40)
        plasticid = '%010i' % random.randrange(10**10)
        with open("data-disk/%s/%s/%s"%(firstdir,seconddir,hexnumber),"w") as fh:
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
with concurrent.futures.ThreadPoolExecutor() as executor:
    i=0
    while i<1000000:
        future=executor.submit(generate,i,i+batch)
        i+=batch

