#v1_01: Base for sql data reader
#v1_02: 

import sqlite3
import pandas 
import numpy as np

import json
import time
import sys
import utilities

import data_tbl_ames

global loglevel
global ulog
global tbl


COLUMN_A = "column_name_a"
COLUMN_B = "column_name_b"
COLUMN_C = "column_name_c"
COLUMN_D = "column_name_d"

tbl_ames = data_tbl_ames.TblName()

if len(sys.argv) == 2:
  logLevel = int(sys.argv[1])
else:
  logLevel = 3

ulog = utilities.utilities(logLevel)

  

''' # Function: get_sql_table
    #  Read sql table from mysql database
    # Inputs : 
                1. None 
    # Output:
                1. None
'''

def getPctMissing(series,per):
    num = series.isnull().sum()
    den = len(series)
    missing_pct = 100*(num/den)
    df_missing_pct = pandas.DataFrame({'attribute':missing_pct.index, 'pct_missing':missing_pct.values})
    high_missing_col = df_missing_pct[df_missing_pct['pct_missing'] > 70]
    return high_missing_col


def get_sql_table(list):
    
    # read data from sql and create an activity table
    ulog.logit(2, "Read sql table xyz")
    column_names = [COLUMN_A, COLUMN_B, COLUMN_C, COLUMN_D]
    df_ames_data = pandas.DataFrame()
    df_ames_data = tbl_ames.read_from_sql_to_dataframe(0, column_names)  # load selected rows fromt the tablep
    return df_ames_data

        
#main flow
def main():
    start_time = time.time()
    ulog.logit(2, "Entering Main function.")
    df_ames_data = get_sql_table([])
    missing_pct = getPctMissing(df_ames_data,70)
    
    print missing_pct
    df_ames_data['SalePrice'].hist()
    #print missing_pct.iloc[:,1]
    #print type(missing_pct)
    #print list(missing_pct)

    end_time = time.time()
    ulog.logit(3, "Execution Time Main : " + utilities.show_elapsed(start_time, end_time))

if __name__ == "__main__":
  main()