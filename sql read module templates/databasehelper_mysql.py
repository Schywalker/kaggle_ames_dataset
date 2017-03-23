          #database class
import pymysql as MySQLdb

import sys

class DbHelper:

    global TBL_NAME
    global SQL_CREATE_TABLE

    global host
    global passwd
    global user
    global dbname
    global port

    #host = "localhost"
    #user = "root"
    host = "west2-mysql-instance1.ctrzwyr54voz.us-west-2.rds.amazonaws.com"
    port = 3306
    user = "awsuser"
    passwd = "nellierova"
    dbname = "db_name"

    TBL_NAME = "tbl_name"

    
    COLUMN_A = "column_name_a"
    COLUMN_B = "column_name_b"
    COLUMN_C = "column_name_c"
    COLUMN_D = "column_name_d"
    

    SQL_CREATE_TABLE = ("create table "
          + TBL_NAME + " ("
          + COLUMN_PRIMARY_KEY + " INTEGER PRIMARY KEY AUTO_INCREMENT,"
          + COLUMN_A + " text,"
          + COLUMN_B + " text,"
          + COLUMN_C + " integer,"
          + COLUMN_D + " text"
          + ")")


    def __init__(self):
    
        # db1 = MySQLdb.connect(host = host, user = user, passwd = passwd)
        # cursor = db1.cursor()
        # sql_createdb = 'CREATE DATABASE ' + dbname
        # cursor.execute(sql_createdb)

        self._host = host,
        self._user = user,
        self._passwd = passwd,
        self._name = dbname,
        self._charset='utf8mb4'
        status = self.connect_db() ##else it gives error about Latin1 cannot encode character. ordinal not in range

        self._cursor.execute('SET NAMES utf8mb4')
        self._cursor.execute("SET CHARACTER SET utf8mb4")
        self._cursor.execute("SET character_set_connection=utf8mb4")

        self._TBL_NAME = TBL_NAME

        # Create Table
        # self.create(TBL_NAME, SQL_CREATE_TABLE)

        # self.convert_columns_to_utf8()

    def connect_db(self):
        try:
            self._db = MySQLdb.connect(host = host, user = user, 
                                 passwd = passwd, port = port, db = dbname, charset = 'utf8mb4')

            self._cursor = self._db.cursor()        
            self._cursor.execute("SELECT VERSION()")
            results = self._cursor.fetchone()
            # Check if anything at all is returned
            if results:
                return True
            else:
                return False
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
                # Print results in comma delimited format
                print "ERROR IN CONNECTION" + str(MySQLdb.Error)
        return False
        

    def convert_columns_to_utf8(self):
        ##to convert the table columns to utf-8, since they can still be different
        self._cursor.execute("ALTER DATABASE `%s` CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci'" % dbname)
        
        sql = "SELECT DISTINCT(table_name) FROM information_schema.columns WHERE table_schema = '%s'" % dbname
        self._cursor.execute(sql)
        
        results = self._cursor.fetchall()
        for row in results:
          sql = "ALTER TABLE `%s` convert to character set DEFAULT COLLATE DEFAULT" % (row[0])
          self._cursor.execute(sql)


    def create(self, table, query):
        sql_tb_exists = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '" + table + "'"
        ##replace above query statement with the one below that allows for wildcard and format correction
        #sql_tb_exists("""
         # SELECT COUNT(*)
          #FROM information_schema.tables
          #WHERE table_name = '{0}'
          #""".format(tablename.replace('\'', '\'\'')))
        c = self._db.cursor()
        c.execute(sql_tb_exists)
        if c.fetchone()[0] != 1:
          c.execute(query)
          self._db.commit()


    def get_connection(self):
        return self._db

    def execute(self, query, params):
        c = self._db.cursor()
        status = c.execute(query, params)
        self._db.commit()
        return status

    def executemany(self, query, params):
        c = self._db.cursor()
        status = c.executemany(query, params)
        self._db.commit()
        return status


    def cursor(self):
        self._db.cursor()


    def commit(self):
        self._db.commit()


    def close(self):
      if self._db is not None:
        self._db.close()


        
    def main():
        main


if __name__ == "__main__":
    main()

    