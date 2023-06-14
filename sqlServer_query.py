import pymssql
import configparser
import pandas as pd

# reading properties files
props = configparser.ConfigParser()
props.read("crs.properties")

sqlSection = "SQLSVR"
dbHost = props.get(sqlSection, "HOST")
dbUsr = props.get(sqlSection, "USER")
dbPwd = props.get(sqlSection, "PWD")
dbName = props.get(sqlSection, "DB")

conn = pymssql.connect(server=dbHost, user=dbUsr, password=dbPwd, database=dbName)

# rows as tuples
#cursor = conn.cursor()

# rows as dictionary
cursor = conn.cursor(as_dict=True)

cursor.execute('SELECT TOP (10) [POLNUMBER] ,[FIRSTNAME] ,[LASTNAME],[GOVTID] FROM [dbo].[LISTBILL.POLICY]')

# loop thru the rows
#for row in cursor:
    #print(row)

# get all rows
#rows = []
rows = cursor.fetchall()
print("Raw records -------")
print(rows)

# use pandas dataframe
df = pd.DataFrame(rows)
print("Records as dataframes -------")
print(df)
