import datetime
import json
import pyodbc

from settings import mssql_connection
con = None
connectionstring = 'DRIVER={SQL Server}' + \
                   ';SERVER=' + mssql_connection["hostname"] + \
                   ';DATABASE=' + mssql_connection["database"] + \
                   ';UID=' + mssql_connection["username"] + \
                   ';PWD=' + mssql_connection["password"]
print("database: ", connectionstring)
try:
    con = pyodbc.connect(connectionstring)
    print("OK CONNECT")
except Exception as e:
    print("Cant connect database: " + str(e), connectionstring)
con.close()
con = None