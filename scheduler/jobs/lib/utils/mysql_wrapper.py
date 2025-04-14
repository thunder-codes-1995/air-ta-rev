import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

class MysqlWrapper:
    dbConn = None

    TABLE_DDS="msd_source_py"

    def _lazy_init(self):
        if self.dbConn is None:
            self.dbConn = mysql.connector.connect(host=os.getenv("MYSQL_HOST"), database=os.getenv("MYSQL_DBNAME"), user=os.getenv("MYSQL_USERNAME"), passwd=os.getenv("MYSQL_PASSWORD"), use_pure=True)
        return self.dbConn

    def get_connection(self):
        self._lazy_init()
        return self.dbConn
