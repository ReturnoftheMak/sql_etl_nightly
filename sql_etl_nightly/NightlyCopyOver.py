
"""
Created on Wed Mar 13 14:14:00 2019

@author: cgallagher
"""

import pandas as pd
import datetime
from sqlalchemy import create_engine


#%%
ImportServerName = 'tcspmSMDB02'
ImportDBName =  'USDF'
ImportTableName = 'CS'

sqlcon = create_engine('mssql+pyodbc://@' + ImportServerName + '/' + ImportDBName + '?driver=ODBC+Driver+13+for+SQL+Server')
Importdf = pd.read_sql("SELECT * from propr.CS",sqlcon)
Importdf['Date'] = str(datetime.datetime.now()) #add date column

#live pricing server
ExportServerName = 'tcspmSMDB01'
ExportDBName = 'Property'
ExportTableName = 'CS'
    
sqlcon = create_engine('mssql+pyodbc://@' + ExportServerName + '/' + ExportDBName + '?driver=ODBC+Driver+13+for+SQL+Server')
Importdf.to_sql(ExportTableName,sqlcon,schema='propr', if_exists='replace')

#%%
ImportServerName = 'tcspmSMDB02'
ImportDBName =  'USDF'
ImportTableName = 'PremSum'

sqlcon = create_engine('mssql+pyodbc://@' + ImportServerName + '/' + ImportDBName + '?driver=ODBC+Driver+13+for+SQL+Server')
Importdf = pd.read_sql("SELECT * from propr.PremSum",sqlcon)
Importdf['Date'] = str(datetime.datetime.now()) #add date column

#live pricing server
ExportServerName = 'tcspmSMDB01'
ExportDBName = 'Property'
ExportTableName = 'PremSum'
    
sqlcon = create_engine('mssql+pyodbc://@' + ExportServerName + '/' + ExportDBName + '?driver=ODBC+Driver+13+for+SQL+Server')
Importdf.to_sql(ExportTableName,sqlcon,schema='propr', if_exists='replace')

#%%
ImportServerName = 'tcspmSMDB02'
ImportDBName =  'USDF'
ImportTableName = 'PremSum2'

sqlcon = create_engine('mssql+pyodbc://@' + ImportServerName + '/' + ImportDBName + '?driver=ODBC+Driver+13+for+SQL+Server')
Importdf = pd.read_sql("SELECT * from propr.PremSum2",sqlcon)
Importdf['Date'] = str(datetime.datetime.now()) #add date column

#live pricing server
ExportServerName = 'tcspmSMDB01'
ExportDBName = 'Property'
ExportTableName = 'PremSum2'
    
sqlcon = create_engine('mssql+pyodbc://@' + ExportServerName + '/' + ExportDBName + '?driver=ODBC+Driver+13+for+SQL+Server')
Importdf.to_sql(ExportTableName,sqlcon,schema='propr', if_exists='replace')

#%%
ImportServerName = 'tcspmSMDB02'
ImportDBName =  'USDF'
ImportTableName = 'PolicyList'

sqlcon = create_engine('mssql+pyodbc://@' + ImportServerName + '/' + ImportDBName + '?driver=ODBC+Driver+13+for+SQL+Server')
Importdf = pd.read_sql("SELECT * from propr.PolicyList",sqlcon)
Importdf['Date'] = str(datetime.datetime.now()) #add date column

#live pricing server
ExportServerName = 'tcspmSMDB01'
ExportDBName = 'Property'
ExportTableName = 'PolicyList'
    
sqlcon = create_engine('mssql+pyodbc://@' + ExportServerName + '/' + ExportDBName + '?driver=ODBC+Driver+13+for+SQL+Server')
Importdf.to_sql(ExportTableName,sqlcon,schema='propr', if_exists='replace')






