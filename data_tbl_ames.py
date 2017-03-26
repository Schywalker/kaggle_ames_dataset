  #database class
import databasehelper_mysql as dbhelper
import pandas
import re

COLUMN_A = "column_name_a"
COLUMN_B = "column_name_b"
COLUMN_C = "column_name_c"
COLUMN_D = "column_name_d"

column_names = [COLUMN_A, COLUMN_B, COLUMN_C, COLUMN_D]
   
class TblName:

    def __init__(self):
      self._db = dbhelper.DbHelper()
      self._tblname = dbhelper.TBL_AMES_TRAIN


    def open_conn(self):
      self._db = dbhelper.DbHelper()


    def sql_do(self, sql, *params):
      self._db.execute(sql, params)
      self._db.commit()

    ''' # Loads data from the 'tbl_name' table into a dataframe
        # Inputs : 
                    1. 
        # Output:
                    1. 
    '''
    def read_from_sql_to_dataframe(self, include, list_values):
      
      select_column_names = [COLUMN_A, COLUMN_B, COLUMN_C, COLUMN_D]
      group_column_names = [COLUMN_A, COLUMN_B, COLUMN_C]
      
      # create the joined strings for column names for use in sql query string
      column_names_string = ",".join(str(x) for x in select_column_names)  # for columns to be extracted, join as string for use in sql query
      group_column_names_string = ",".join(str(x) for x in group_column_names)  # for columns to be grouped by, join as string for use in sql query
      
      # create the sql query string 
      if include == 1:
        sql = ("SELECT " + column_names_string + " FROM " + self._tblname 
            + " WHERE " + COLUMN_A + " IN ('" + "','".join(map(str, list_values))
            + "')" 
            #+ " GROUP BY " + group_column_names_string
            )
      elif include == 2:
            sql = ("SELECT " + column_names_string + " FROM " + self._tblname 
            + " WHERE " + COLUMN_A + " NOT IN ('" + "','".join(map(str, list_values))
            + "')" 
            #+ " GROUP BY " + group_column_names_string
            )
      else:
            sql = ("SELECT * FROM " + self._tblname)
      df = pandas.read_sql(sql, self._db.get_connection())
      #df = self.convert_to_unicode_dtype(df)
      return df
      

    def insert(self, row):
      format_strings = "(" + ','.join(column_names) + ") values (" + ','.join(['%s'] * len(column_names)) + ")"
      result = self._db.execute("insert into " + self._tblname + " %s" % format_strings,
                row)

    
    def write_to_sql(self, pdata):

        pdata.to_sql(self._tblname, self._db.get_connection(), flavor = 'mysql', if_exists='append', index = True, index_label = [COLUMN_A, COLUMN_B, COLUMN_C, COLUMN_D])  #cannot use 'replace' since the create table portion fails
                            #with error identifier too long (becasuse of the many foreign keys that violate identifier size <63 rule for msyql)
    
                
    def writeToTable(self, tbl_data):
        dict_obj = dict.fromkeys(column_names)
        for data, value in tbl_data.iteritems():
            #playerIDs are arrays inside the JSON blocks (multiple player IDs inside {})
            #code below runs through the array, and converts it into string to store in SQL column
            
            ####THE DICT_OBJ CREATION CODE HELPS TO ERROR CHECK AGAINST MISSING COLUMN VALUES, WHICH WILL MAKE THE PROGRAM FAIL DURING INSERT
            dict_obj[data] = value  #for all other fields, just copy to the new variable that hosts playerIDs as strings
        
        data_values = tuple(dict_obj[property] for property in column_names)  #create an ordered list according to that of columnsHabitats
        self.insert(data_values)
        return tbl_data



    def insert_multiple_to_table(self, tbl_data_list):
      list_data_tuple = []
      #for l in alliance_properties.iteritems
      for tbl_data in tbl_data_list:
        dict_obj = dict.fromkeys(column_names)
        for name, value in tbl_data.iteritems():
            #playerIDs are arrays inside the JSON blocks (multiple player IDs inside {})
            #code below runs through the array, and converts it into string to store in SQL column
            if value == 'None':
              value = 'null'
            dict_obj[name] = value#.encode('utf8')  #for all other fields, just copy to the new variable that hosts playerIDs as strings
        habitat_tuple = tuple(dict_obj[item] for item in column_names)  #create an ordered list according to that of columnsAlliance
        list_data_tuple.append(habitat_tuple)
      format_strings = " (" + ','.join(column_names) + ") "
      insert_query = "insert into " + self._tblname + format_strings + " VALUES (" + ','.join(['%s'] * len(column_names)) + ")"
      result = self._db.executemany(insert_query, list_data_tuple)
      return result



    def close(self):
      if self._db is not None:
        self._db.close()

    @property
    def filename(self):
      return self._filename

    @filename.setter
    def filename(self, fn):
      self._filename = fn
      self._db = sqlite3.connect(fn)
      self._db.row_factory = sqlite3.Row

    @property
    def table(self): return self._tblname

    @table.setter
    def settable(self, t): self._tblname = t


    def main():
      sql = ("SELECT *" + " FROM " + self.ames_train)
      df = pandas.read_sql(sql, self._db.get_connection())


if __name__ == "__main__":
    main()
