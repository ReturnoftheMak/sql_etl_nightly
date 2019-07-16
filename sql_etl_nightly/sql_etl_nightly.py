# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 12:11:00 2019

@author: Makhan.Gill
"""

# ETL Main function

#%% Package Imports

import pandas as pd
from etl_functions import retrieve_inputs, connect_sql_server, table_manipulation


#%% Definition and execution of main function

def sql_etl():
    """Docstring
    """

    df_inputs = retrieve_inputs()

    for df_row in df_inputs.iterrows():

        sql_con_in = connect_sql_server(df_row[1]['IMPORT_SERVER_NAME'],
                                        df_row[1]['IMPORT_DB_NAME'])

        df_in = pd.read_sql(df_row[1]['IMPORT_TABLE_NAME'], sql_con_in)

        df_out = table_manipulation(df_in)

        sql_con_out = connect_sql_server(df_row[1]['EXPORT_SERVER_NAME'],
                                         df_row[1]['EXPORT_DB_NAME'])

        df_out.to_sql(df_row[1]['EXPORT_TABLE_NAME'], sql_con_out,
                      schema=df_row[1]['EXPORT_SCHEMA_NAME'], if_exists='replace', chunksize=1000)

if __name__ == "__main__":
    sql_etl()
