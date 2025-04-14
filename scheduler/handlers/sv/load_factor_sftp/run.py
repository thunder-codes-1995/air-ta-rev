import os
import shutil
import sys
from datetime import datetime

project_root = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(project_root, '..', '..', '..'))
sys.path.append(project_root)

from download import download
from parser import Parser
from jobs.lib.utils.mongo_wrapper import MongoWrapper



def remove_contents(dir_path):
    shutil.rmtree(dir_path)
    os.makedirs(dir_path)
    print(f"{dir_path} content removed")


def main(date):
    try:
        download(date)
        parser = Parser(int(date))
        hitits = parser.parse()
        db = MongoWrapper()
        db.col_hitit().insert_many(hitits)
        print(f"{len(hitits)} record added to db successfully")
    except Exception as e:
        print(e)
    remove_contents("tmp")




if __name__ == '__main__':
    date = datetime.now().strftime('%y%m%d')
    main(date)


