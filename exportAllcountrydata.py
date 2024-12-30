import pandas as pd
from helper import Helper
import os

helper1 = Helper()
connection = helper1.connection
cursor = helper1.cursor

tablesQuery = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'incubyte' AND TABLE_NAME like 'customers_%';
            """

countries = pd.read_sql(tablesQuery, connection)

if os.path.exists("exportedFiles"):
    print("Folder Already Exist")
else:
    os.mkdir("exportedFiles")
    print("Folder for files created")

for i in countries.itertuples():
    tableName = i.TABLE_NAME
    fileName = f"{tableName.split('_')[1].upper()}.csv"
    sqlQuery = f"""SELECT
        customerId as Id,
        customerName as `Patient Name`,
        DOB,
        vaccinationId VaccinationType,
        lastCunsultedDate VaccinationDate,
        TIMESTAMPDIFF(Year,DOB,CURDATE()) Age,
        case when TIMESTAMPDIFF(DAY,lastCunsultedDate,CURDATE()) < 30 then TIMESTAMPDIFF(DAY,lastCunsultedDate,CURDATE())
        else '> 30'
        end lastCunsultedSince
    FROM
        {tableName};"""
    data = pd.read_sql(sqlQuery, connection)
    data.to_csv(f"./exportedFiles/{fileName}", index=False)
    
helper1.closeConnection()