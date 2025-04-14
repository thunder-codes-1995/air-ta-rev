#!/usr/bin/python3
import sys
from os.path import exists
import os
basedir=os.getcwd()
atarev_env = os.getenv('ATAREV_ENV')
if atarev_env is not None:
    print (f"ATAREV_ENV is set:{atarev_env}, current dir:{os.getcwd()}, basedir:{basedir}")
    atarev_env = atarev_env.lower()
    if exists(os.path.join(basedir, ".env")):
        print("Backup existing .env file")
        try:
            os.rename(os.path.join(basedir, f".env"), os.path.join(basedir, f".env.backup"))
        except Exception as e:
            print(f"Failed to backup .env file, got error:{e}")
            sys.exit(os.EX_OSFILE)
    print (f"Create .env file from .env.{atarev_env}")
    if exists(os.path.join(basedir, f".env.{atarev_env}")):
        try:
            os.rename(os.path.join(basedir, f".env.{atarev_env}"), os.path.join(basedir, ".env"))
        except Exception as e:
            print(f"Failed to prepare .env, cannot rename .env.{atarev_env} to .env file, got error:{e}")
            sys.exit(os.EX_OSFILE)
    else:
        print (f"File .env.{atarev_env} does not exist! Cannot prepare .env file, it's an error")
        sys.exit(os.EX_OSFILE)

    print ("Done")
    if not exists(os.path.join(os.getcwd(), f".env")):
        print ("Something has gone wrong - .env still does not exist")
        sys.exit(os.EX_OSFILE)
else:
    print(f"ATAREV_ENV is not set, ignoring this step, current dir:{os.getcwd()}")