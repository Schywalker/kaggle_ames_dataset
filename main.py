#v1_01: Base for sql data reader
#v1_02: 

import sqlite3
import pandas
import numpy as np

import json
import time
import sys

import data_tbl_name

global loglevel
global ulog
global tbl

COLUMN_A = "column_name_a"
COLUMN_B = "column_name_b"
COLUMN_C = "column_name_c"
COLUMN_D = "column_name_d"

tbl_name = data_tbl_name.TblName()

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
def get_sql_table():
    
    # read data from sql and create an activity table
    ulog.logit(2, "Read sql table xyz")
    column_names = [COLUMN_A, COLUMN_B, COLUMN_C, COLUMN_D]
    df_player_habitat_data = pandas.DataFrame()
    df_player_habitat_data = tbl_name.read_from_sql_to_dataframe(column_names)  # load selected rows fromt the table
        
#main flow
def main():
    start_time = time.time()
    ulog.logit(2, "Entering Main function.")
    end_time = time.time()
    ulog.logit(3, "Execution Time Main : " + utilities.show_elapsed(start_time, end_time))

if __name__ == "__main__":
  main()