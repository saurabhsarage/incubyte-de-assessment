import re, os
from datetime import datetime
from dotenv import load_dotenv
import pymysql
import os, json, pandas as pd

class Helper():
    # Load environment variables
    load_dotenv()
    
    # create database connnection
    connection = pymysql.connect(
        host=os.environ['HOST'],
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        database=os.environ["DATABASE"]
    )

    # create cursor
    cursor = connection.cursor()
    
    # validate date formats
    def detect_date_format_with_regex(self, date_string):
        patterns = {
            "%m/%d/%Y": r"\d{1,2}/\d{1,2}/\d{4}",
            "%d-%m-%Y": r"\d{1,2}-\d{1,2}-\d{4}",
            "%Y.%m.%d": r"\d{4}\.\d{1,2}\.\d{1,2}",
            "%Y-%m-%d": r"\d{4}-\d{1,2}-\d{1,2}",
            "%m%d%Y": r"^(0[1-9]|1[0-2])([0-2][0-9]|3[0-1])\d{4}$",
            "%Y%m%d": r"^\d{4}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$",
        }
        
        for fmt, pattern in patterns.items():
            if re.fullmatch(pattern, date_string):
                return fmt
        
        return "Unknown format"

    # convert string date into date
    def convertToDateFormat(self,date_string, fmt):
        try:
            return datetime.strptime(date_string, fmt)
        except:
            return date_string

    # close database connection
    def closeConnection(self):
        self.cursor.close()
        self.connection.close()
    
    # check if tables is already created or not
    def checkAndCreateCountryTable(self, uniqueCountries):
        tablesCheckQuery = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'incubyte' AND TABLE_NAME like 'customers_%';
            """
        tables = pd.read_sql(tablesCheckQuery, self.connection)
        tables = tables.TABLE_NAME.tolist()
        
        createTableQuery = """
            create table {tableName} (
                customerName varchar(255) not null,
                customerId varchar(18) not null,
                openDate date not null,
                lastCunsultedDate date,
                vaccinationId char(5),
                drName CHAR(255),
                state CHAR(5),
                country CHAR(5),
                postCode INT(5),
                DOB date,
                isActive CHAR(1),
                primary key (customerName,customerId, openDate)
            );
            """
            
        for country in uniqueCountries:
            print(f"customers_{country.lower()}")
            if f"customers_{country.lower()}" in tables:
                print("Table Exist")
            else:
                print(f"Need to Create table :- customers_{country.lower()}")
                # print(createTableQuery.format(tableName=f"customers_{country.lower()}"))
                self.cursor.execute(createTableQuery.format(tableName=f"customers_{country.lower()}"))
                self.connection.commit()
    
    # convert data into checks of 10000 records
    def toChuncks(self, df):
        n = 10000
        list_df = [df[i:i+n] for i in range(0, df.shape[0], n)]
        tuple_of_tuples = tuple(tuple(x.values.tolist()) for x in list_df)
        return tuple_of_tuples

    # get columns from table
    def getCols(self, tableName, df, cursor):
        sql = f"SHOW columns FROM {tableName}"
        cursor.execute(sql)
        l1 = [column[0] for column in cursor.fetchall()]
        newDf = df.filter(l1)
        return newDf
    
    # insert data into database
    def insertData(self, insertQuery, tuple_of_tuples):
        for i in tuple_of_tuples:
            ti = list(tuple(x) for x in i)
            self.cursor.executemany(insertQuery, ti)
            self.connection.commit()
