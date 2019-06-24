# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 11:19:56 2019

@author: Makhan.Gill
"""

# ETL for Actuarial Server

#%% Package Imports

import pandas as pd
import datetime
from sqlalchemy import create_engine


#%% Import relevant inputs

def retrieve_inputs():
    """Docstring
    """

    input_path = r'\\svrtcs04\Syndicate Data\Actuarial\Pricing\DataScience\Python\Scripts\sql_etl_nightly\sql_etl_nightly\inputs\database_name_inputs.csv'

    inputs = pd.read_csv(input_path)

    return inputs


#%% SQL connection

def connect_sql_server(server_name, db_name):
    """Returns a SQLAlchemy engine, given the server and database name.

    Args:
        server_name (str): - Server name
        db_name (str): - Database name

    Returns:
        Object of type (sqlalchemy.engine.base.Engine) for use in pandas pd.to_sql functions
    """

    sql_con = create_engine('mssql+pyodbc://@'
                            + server_name
                            + '/'
                            + db_name
                            + '?driver=ODBC+Driver+13+for+SQL+Server')

    return sql_con


#%% Table Manipulation

def table_manipulation(sql_dataframe):
    """Docstring
    """

    sql_dataframe['Date'] = str(datetime.datetime.now())

    return sql_dataframe
